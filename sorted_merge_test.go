package streamhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// mergeWithWatchdog merges with bufBytes read buffers, failing the test if
// the merge does not return.
func mergeWithWatchdog(t *testing.T, ctx context.Context, output string, inputs []string, cfg SortedFileConfig, bufBytes int) error {
	t.Helper()
	return runBuildWithWatchdog(t, 2*time.Minute, func() error {
		return mergeSortedFiles(ctx, output, inputs, cfg, bufBytes)
	})
}

// writeMergeInputs writes a sorted file of random records for each size, and
// returns their paths and all their records. The inputs' metadata lengths
// vary, so their records start at different offsets.
func writeMergeInputs(t *testing.T, dir string, payloadSize int, sizes []int) ([]string, []sortedRecord) {
	t.Helper()
	rng := newTestRNG(t)
	var paths []string
	var all []sortedRecord
	for i, n := range sizes {
		recs := randomSortedRecords(rng, n, payloadSize)
		path := filepath.Join(dir, fmt.Sprintf("in%04d.sorted", i))
		meta := bytes.Repeat([]byte{byte(i)}, i*1500%10000)
		writeSortedFile(t, path, SortedFileConfig{PayloadSize: payloadSize, Metadata: meta}, recs)
		paths = append(paths, path)
		all = append(all, recs...)
	}
	return paths, all
}

func TestMergeSortedFiles(t *testing.T) {
	rng := newTestRNG(t)
	randomSizes := func(inputs, maxSize int) []int {
		sizes := make([]int, inputs)
		for i := range sizes {
			sizes[i] = rng.IntN(maxSize + 1)
		}
		return sizes
	}
	type mergeCase struct {
		name        string
		payloadSize int
		sizes       []int
		bufBytes    int
	}
	cases := []mergeCase{
		{"one input", 3, []int{20000}, mergeReadBuffer},
		{"inputs smaller than a buffer", 3, []int{1, 2, 100, 3000}, mergeReadBuffer},
		{"empty inputs among others", 4, []int{0, 500, 0, 0, 2000, 0}, mergeReadBuffer},
		{"many inputs", 3, randomSizes(300, 400), mergeReadBuffer},
		{"refills across record boundaries", 3, []int{30000, 25000, 9000}, 2 * directAlign},
	}
	for ps := 0; ps <= maxPayloadSize; ps++ {
		cases = append(cases, mergeCase{fmt.Sprintf("random sizes, payload=%d", ps), ps, randomSizes(1+rng.IntN(12), 15000), []int{2 * directAlign, mergeReadBuffer}[rng.IntN(2)]})
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			inputs, want := writeMergeInputs(t, dir, tc.payloadSize, tc.sizes)
			slices.SortFunc(want, compareSortedRecords)
			output := filepath.Join(dir, "merged.sorted")
			cfg := SortedFileConfig{PayloadSize: tc.payloadSize, Metadata: []byte("merged"), BytesPerSync: 64 << 10}
			if err := mergeWithWatchdog(t, context.Background(), output, inputs, cfg, tc.bufBytes); err != nil {
				t.Fatal(err)
			}
			h, err := ReadSortedFileHeader(output)
			if err != nil {
				t.Fatal(err)
			}
			if h.Count != uint64(len(want)) || h.PayloadSize != tc.payloadSize || string(h.Metadata) != "merged" {
				t.Fatalf("header %+v, want %d records with %d-byte payloads and the merge's metadata", h, len(want), tc.payloadSize)
			}
			// readSortedFile fails on a record out of order. Equal keys may come
			// from their inputs in any order, so compare as sorted by payload too.
			got := readSortedFile(t, output)
			slices.SortFunc(got, compareSortedRecords)
			if !slices.Equal(got, want) {
				t.Fatal("the merged records are not the inputs' records")
			}
		})
	}
}

func TestMergeSortedFilesNoRecords(t *testing.T) {
	dir := t.TempDir()
	empty, _ := writeMergeInputs(t, dir, 2, []int{0, 0})
	for _, inputs := range [][]string{nil, empty} {
		output := filepath.Join(dir, "merged.sorted")
		if err := mergeWithWatchdog(t, context.Background(), output, inputs, SortedFileConfig{PayloadSize: 2, Metadata: []byte("m")}, mergeReadBuffer); err != nil {
			t.Fatal(err)
		}
		if h, err := ReadSortedFileHeader(output); err != nil || h.Count != 0 || string(h.Metadata) != "m" {
			t.Fatalf("merging %d empty inputs: header %+v, %v", len(inputs), h, err)
		}
	}
}

func TestMergeSortedFilesRejectsPayloadSizes(t *testing.T) {
	dir := t.TempDir()
	three, _ := writeMergeInputs(t, dir, 3, []int{10})
	four := filepath.Join(dir, "four.sorted")
	writeSortedFile(t, four, SortedFileConfig{PayloadSize: 4}, randomSortedRecords(newTestRNG(t), 10, 4))
	output := filepath.Join(dir, "merged.sorted")
	for _, tc := range []struct {
		inputs      []string
		payloadSize int
	}{
		{[]string{three[0], four}, 3},
		{[]string{four, three[0]}, 4},
		{three, 4},
	} {
		if err := mergeWithWatchdog(t, context.Background(), output, tc.inputs, SortedFileConfig{PayloadSize: tc.payloadSize}, mergeReadBuffer); err == nil {
			t.Fatalf("merging %v into %d-byte payloads: want an error", tc.inputs, tc.payloadSize)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatalf("a rejected merge created the output: %v", err)
		}
	}
}

// TestMergeSortedFilesRejectsOutputAmongInputs checks that a merge into one
// of its inputs fails before creating the output, which would truncate the
// input.
func TestMergeSortedFilesRejectsOutputAmongInputs(t *testing.T) {
	dir := t.TempDir()
	inputs, _ := writeMergeInputs(t, dir, 3, []int{1000, 2000})
	before, err := os.ReadFile(inputs[1])
	if err != nil {
		t.Fatal(err)
	}
	// inputs[1], spelled differently.
	output := dir + string(filepath.Separator) + "." + string(filepath.Separator) + filepath.Base(inputs[1])
	if err := mergeWithWatchdog(t, context.Background(), output, inputs, SortedFileConfig{PayloadSize: 3}, mergeReadBuffer); err == nil {
		t.Fatal("a merge into one of its inputs returned no error")
	}
	after, err := os.ReadFile(inputs[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) {
		t.Fatal("the merge changed its input")
	}
}

// TestMergeSortedFilesRejectsUnsortedInput checks that an input whose records
// are out of order fails the merge, wherever the disorder is.
func TestMergeSortedFilesRejectsUnsortedInput(t *testing.T) {
	dir := t.TempDir()
	inputs, _ := writeMergeInputs(t, dir, 3, []int{5000, 6000})
	recs := uniqueSortedRecords(newTestRNG(t), 5000, 3)
	clean := filepath.Join(dir, "clean.sorted")
	writeSortedFile(t, clean, SortedFileConfig{PayloadSize: 3}, recs)
	data, err := os.ReadFile(clean)
	if err != nil {
		t.Fatal(err)
	}
	// The second swap straddles a refill of a two-block buffer.
	for _, i := range []int{0, 431, len(recs) - 2} {
		bad := slices.Clone(data)
		swapRecords(bad, MinKeySize+3, i)
		corrupt := filepath.Join(dir, "corrupt.sorted")
		if err := os.WriteFile(corrupt, bad, 0o600); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(dir, "merged.sorted")
		err := mergeWithWatchdog(t, context.Background(), output, append(slices.Clone(inputs), corrupt), SortedFileConfig{PayloadSize: 3}, 2*directAlign)
		if !errors.Is(err, ErrUnsortedInput) {
			t.Fatalf("records %d and %d swapped: the merge returned %v, want ErrUnsortedInput", i, i+1, err)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatalf("a failed merge left its output: %v", err)
		}
	}
}

// TestMergeSortedFilesInputShorterThanItsCount covers an input that shrinks
// after its header was checked.
func TestMergeSortedFilesInputShorterThanItsCount(t *testing.T) {
	dir := t.TempDir()
	inputs, _ := writeMergeInputs(t, dir, 3, []int{1000})
	w, err := NewSortedFileWriter(filepath.Join(dir, "merged.sorted"), SortedFileConfig{PayloadSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, count := range []uint64{1001, 5000} {
		runs, _, _, err := readSortedRuns(inputs)
		if err != nil {
			t.Fatal(err)
		}
		runs[0].end = runs[0].start + int64(count)*(MinKeySize+3)
		err = runBuildWithWatchdog(t, time.Minute, func() error {
			return mergeRuns(context.Background(), runs, w, 2*directAlign)
		})
		if !errors.Is(err, ErrTruncatedFile) {
			t.Fatalf("an input counted %d records but holding 1000: the merge returned %v", count, err)
		}
	}
}

func TestMergeSortedFilesCanceled(t *testing.T) {
	dir := t.TempDir()
	inputs, _ := writeMergeInputs(t, dir, 3, []int{20000, 20000, 20000, 20000})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	output := filepath.Join(dir, "merged.sorted")
	if err := mergeWithWatchdog(t, ctx, output, inputs, SortedFileConfig{PayloadSize: 3}, mergeReadBuffer); !errors.Is(err, context.Canceled) {
		t.Fatalf("a merge with a canceled context returned %v", err)
	}
	if _, err := os.Stat(output); !os.IsNotExist(err) {
		t.Fatalf("a canceled merge left its output: %v", err)
	}

	// Cancel once the merge goroutine has handed over its first batch.
	runs, _, _, err := readSortedRuns(inputs)
	if err != nil {
		t.Fatal(err)
	}
	const recordSize = MinKeySize + 3
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	out, pool := make(chan *[]byte, mergeChanDepth), newBatchPool(recordSize)
	merged := make(chan error, 1)
	go func() {
		defer close(out)
		merged <- mergeFiles(ctx, runs, recordSize, mergeReadBuffer, out, pool)
	}()
	err = runBuildWithWatchdog(t, time.Minute, func() error {
		pool <- <-out
		cancel()
		batches := 1
		for b := range out {
			batches++
			pool <- b
		}
		if total := (4*20000 + mergeBatchRecords - 1) / mergeBatchRecords; batches >= total {
			return fmt.Errorf("the merge sent all %d batches after it was canceled", batches)
		}
		return <-merged
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("a merge canceled midway returned %v", err)
	}
}

// BenchmarkMergeSortedFiles merges 90 inputs of 1M records with 3-byte payloads.
func BenchmarkMergeSortedFiles(b *testing.B) {
	const inputs, perInput = 90, 1_000_000
	dir := b.TempDir()
	rng := newTestRNG(b)
	paths := make([]string, inputs)
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("in%02d.sorted", i))
		writeSortedFile(b, paths[i], SortedFileConfig{PayloadSize: 3}, stratifiedSortedRecords(rng, perInput, 3))
	}
	output := filepath.Join(dir, "merged.sorted")
	for b.Loop() {
		if err := MergeSortedFiles(context.Background(), output, paths, SortedFileConfig{PayloadSize: 3}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(inputs*perInput)*float64(b.N)/b.Elapsed().Seconds(), "records/s")
}

// stratifiedSortedRecords returns n records in key order without sorting
// them: record i's first eight bytes are random within the i-th of n equal
// ranges, so inputs interleave as uniform random keys would.
func stratifiedSortedRecords(rng *rand.Rand, n, payloadSize int) []sortedRecord {
	recs := make([]sortedRecord, n)
	step := ^uint64(0) / uint64(n)
	for i := range recs {
		binary.BigEndian.PutUint64(recs[i].key[0:8], uint64(i)*step+rng.Uint64N(step))
		binary.BigEndian.PutUint64(recs[i].key[8:16], rng.Uint64())
		recs[i].payload = rng.Uint64() & payloadMaskFor(payloadSize)
	}
	return recs
}
