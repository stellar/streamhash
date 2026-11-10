package streamhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	intbits "github.com/stellar/streamhash/internal/bits"
)

// buildFilesWithWatchdog builds from inputs with tune, failing the test if
// the build does not return.
func buildFilesWithWatchdog(t *testing.T, ctx context.Context, output string, inputs []string, tune sortedBuildTuning, opts ...BuildOption) error {
	t.Helper()
	return runBuildWithWatchdog(t, 2*time.Minute, func() error {
		return buildFromSortedFiles(ctx, output, inputs, tune, opts...)
	})
}

func readFileBytes(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// referenceIndex builds recs with a SortedBuilder and returns the index
// file's bytes.
func referenceIndex(t testing.TB, path string, recs []sortedRecord, opts ...BuildOption) []byte {
	t.Helper()
	entries := make([]entry, len(recs))
	for i, r := range recs {
		entries[i] = entry{Key: recs[i].key[:], Payload: r.payload}
	}
	if err := buildFromEntries(context.Background(), path, entries, opts...); err != nil {
		t.Fatal(err)
	}
	return readFileBytes(t, path)
}

// The layouts in sortedFileLayouts.
const (
	layoutSpread = iota
	layoutOneFile
	layoutEmptyFiles
	layoutRanges
)

// sortedFileLayouts are ways to spread records over files: file returns
// the file of record i of n, out of files.
var sortedFileLayouts = []struct {
	name  string
	files int
	file  func(rng *rand.Rand, i, n, files int) int
}{
	{"spread", 24, func(rng *rand.Rand, _, _, files int) int { return rng.IntN(files) }},
	{"one file", 1, func(*rand.Rand, int, int, int) int { return 0 }},
	// The odd-numbered files stay empty.
	{"empty files among full", 12, func(rng *rand.Rand, _, _, files int) int { return 2 * rng.IntN(files/2) }},
	// Each file holds a key range, so most stripes have records in one file.
	{"ranges", 8, func(_ *rand.Rand, i, n, files int) int { return i * files / n }},
}

// writeLayout writes recs, which are in key order, to files under dir as
// layout spreads them, and returns their paths.
func writeLayout(t *testing.T, dir string, rng *rand.Rand, recs []sortedRecord, payloadSize, layout int) []string {
	t.Helper()
	l := sortedFileLayouts[layout]
	parts := make([][]sortedRecord, l.files)
	for i, r := range recs {
		f := l.file(rng, i, len(recs), l.files)
		parts[f] = append(parts[f], r)
	}
	paths := make([]string, l.files)
	for f, part := range parts {
		paths[f] = filepath.Join(dir, fmt.Sprintf("layout%d-%02d.sorted", layout, f))
		writeSortedFile(t, paths[f], SortedFileConfig{PayloadSize: payloadSize}, part)
	}
	return paths
}

// TestBuildFromSortedFilesEquivalence checks that the build's index is
// byte-identical to a SortedBuilder's, for both algorithms, payload and
// fingerprint sizes, worker counts, stripe sizes and file layouts.
func TestBuildFromSortedFilesEquivalence(t *testing.T) {
	const numKeys = 30000
	rng := newTestRNG(t)
	// Stripe sizes: the default, which is one stripe at this size; one block
	// per stripe; and a few blocks per stripe, with a partial last stripe.
	stripeKeys := []int{0, 1, 9000}
	workers := []int{1, 2, 4, 16}
	for _, c := range []struct{ payloadSize, fpSize int }{{0, 0}, {3, 1}, {5, 3}, {8, 4}} {
		recs := uniqueSortedRecords(rng, numKeys, c.payloadSize)
		dir := t.TempDir()
		files := make([][]string, len(sortedFileLayouts))
		for l := range sortedFileLayouts {
			files[l] = writeLayout(t, dir, rng, recs, c.payloadSize, l)
		}
		for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
			opts := []BuildOption{WithAlgorithm(algo), WithFingerprint(c.fpSize)}
			want := referenceIndex(t, filepath.Join(dir, "reference.idx"), recs, append(opts, WithPayload(c.payloadSize))...)
			for l, layout := range sortedFileLayouts {
				for _, w := range workers {
					for _, s := range stripeKeys {
						name := fmt.Sprintf("%s/payload=%d/fp=%d/%s/workers=%d/stripe=%d", algo, c.payloadSize, c.fpSize, layout.name, w, s)
						t.Run(name, func(t *testing.T) {
							output := filepath.Join(dir, "built.idx")
							err := buildFilesWithWatchdog(t, context.Background(), output, files[l], sortedBuildTuning{stripeKeys: s}, append(opts, WithWorkers(w))...)
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(readFileBytes(t, output), want) {
								t.Fatal("the index differs from the SortedBuilder's")
							}
						})
					}
				}
			}
		}
	}
}

// TestBuildFromSortedFilesAcrossBufferSeams checks the equivalence with the
// smallest read buffers and many bounded stripes, so that the gatherer's cut
// crosses read-buffer seams and gallops past the records it scans in order.
func TestBuildFromSortedFilesAcrossBufferSeams(t *testing.T) {
	rng := newTestRNG(t)
	for _, c := range []struct{ payloadSize, fpSize, files, numKeys int }{{0, 0, 1, 60000}, {3, 1, 4, 60000}, {8, 4, 2, 40000}} {
		recs := uniqueSortedRecords(rng, c.numKeys, c.payloadSize)
		dir := t.TempDir()
		parts := make([][]sortedRecord, c.files)
		for _, r := range recs {
			f := rng.IntN(c.files)
			parts[f] = append(parts[f], r)
		}
		files := make([]string, c.files)
		for f, part := range parts {
			files[f] = filepath.Join(dir, fmt.Sprintf("in%02d.sorted", f))
			writeSortedFile(t, files[f], SortedFileConfig{PayloadSize: c.payloadSize}, part)
		}
		for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
			opts := []BuildOption{WithAlgorithm(algo), WithFingerprint(c.fpSize)}
			want := referenceIndex(t, filepath.Join(dir, "reference.idx"), recs, append(opts, WithPayload(c.payloadSize))...)
			for _, w := range []int{1, 3} {
				for _, s := range []int{3000, 9000} {
					// The smallest buffers, then three of a 19-byte record's units.
					for _, buf := range []int{1, 3 * 77824} {
						name := fmt.Sprintf("%s/payload=%d/fp=%d/files=%d/workers=%d/stripe=%d/buf=%d", algo, c.payloadSize, c.fpSize, c.files, w, s, buf)
						t.Run(name, func(t *testing.T) {
							output := filepath.Join(dir, "built.idx")
							err := buildFilesWithWatchdog(t, context.Background(), output, files, sortedBuildTuning{stripeKeys: s, bufBytes: buf}, append(opts, WithWorkers(w))...)
							if err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(readFileBytes(t, output), want) {
								t.Fatal("the index differs from the SortedBuilder's")
							}
						})
					}
				}
			}
		}
	}
}

// TestBuildFromSortedFilesSmall covers no keys at all, which is two empty
// blocks, and totals so small that blocks are empty, with and without empty
// files.
func TestBuildFromSortedFilesSmall(t *testing.T) {
	rng := newTestRNG(t)
	for _, n := range []int{0, 1, 2, 3, 7} {
		recs := uniqueSortedRecords(rng, n, 2)
		dir := t.TempDir()
		opts := []BuildOption{WithPayload(2), WithFingerprint(1)}
		want := referenceIndex(t, filepath.Join(dir, "reference.idx"), recs, opts...)
		inputs := map[string][]string{
			"one file":    writeLayout(t, dir, rng, recs, 2, layoutOneFile),
			"empty files": writeLayout(t, dir, rng, recs, 2, layoutEmptyFiles),
		}
		keys := make([][]byte, len(recs))
		for i := range recs {
			keys[i] = recs[i].key[:]
		}
		for name, files := range inputs {
			for _, w := range []int{1, 4} {
				for _, s := range []int{0, 1} {
					t.Run(fmt.Sprintf("keys=%d/%s/workers=%d/stripe=%d", n, name, w, s), func(t *testing.T) {
						output := filepath.Join(dir, "built.idx")
						err := buildFilesWithWatchdog(t, context.Background(), output, files, sortedBuildTuning{stripeKeys: s}, WithFingerprint(1), WithWorkers(w))
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(readFileBytes(t, output), want) {
							t.Fatal("the index differs from the SortedBuilder's")
						}
						idx, err := Open(output)
						if err != nil {
							t.Fatal(err)
						}
						defer idx.Close()
						if stats := idx.Stats(); stats.NumKeys != uint64(len(recs)) || stats.NumBlocks != 2 {
							t.Fatalf("the index has %d keys in %d blocks, want %d in 2", stats.NumKeys, stats.NumBlocks, len(recs))
						}
						verifyMPHF(t, idx, keys)
					})
				}
			}
		}
	}
}

// TestBuildFromSortedFilesBytesPerSync checks that writeback changes nothing
// in the index, even when it starts on nearly every write.
func TestBuildFromSortedFilesBytesPerSync(t *testing.T) {
	rng := newTestRNG(t)
	recs := uniqueSortedRecords(rng, 50000, 3)
	dir := t.TempDir()
	files := writeLayout(t, dir, rng, recs, 3, layoutSpread)
	for _, w := range []int{1, 4} {
		var indexes [][]byte
		for _, bytesPerSync := range []int{0, 1} {
			output := filepath.Join(dir, fmt.Sprintf("sync%d-%d.idx", bytesPerSync, w))
			err := buildFilesWithWatchdog(t, context.Background(), output, files, sortedBuildTuning{stripeKeys: 9000}, WithFingerprint(1), WithWorkers(w), WithBytesPerSync(bytesPerSync))
			if err != nil {
				t.Fatal(err)
			}
			indexes = append(indexes, readFileBytes(t, output))
		}
		if !bytes.Equal(indexes[0], indexes[1]) {
			t.Fatalf("workers=%d: the index built with WithBytesPerSync differs from the one built without it", w)
		}
	}
}

// TestBuildFromSortedFilesPayloadOption checks that WithPayload is accepted
// when it matches the files and rejected when it does not.
func TestBuildFromSortedFilesPayloadOption(t *testing.T) {
	rng := newTestRNG(t)
	recs := uniqueSortedRecords(rng, 3000, 3)
	dir := t.TempDir()
	files := writeLayout(t, dir, rng, recs, 3, layoutSpread)
	want := referenceIndex(t, filepath.Join(dir, "reference.idx"), recs, WithPayload(3))
	output := filepath.Join(dir, "built.idx")
	if err := buildFilesWithWatchdog(t, context.Background(), output, files, sortedBuildTuning{}, WithPayload(3)); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(readFileBytes(t, output), want) {
		t.Fatal("the index built with a matching WithPayload differs from the SortedBuilder's")
	}
	for _, ps := range []int{0, 4} {
		output := filepath.Join(dir, "rejected.idx")
		if err := buildFilesWithWatchdog(t, context.Background(), output, files, sortedBuildTuning{}, WithPayload(ps)); err == nil {
			t.Fatalf("WithPayload(%d) on files with 3-byte payloads: want an error", ps)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatalf("a rejected build left its output: %v", err)
		}
	}
}

// TestBuildFromSortedFilesRejectsOutputAmongInputs checks that a build into
// one of its inputs fails before creating the output, which would truncate
// the input.
func TestBuildFromSortedFilesRejectsOutputAmongInputs(t *testing.T) {
	rng := newTestRNG(t)
	dir := t.TempDir()
	files := writeLayout(t, dir, rng, uniqueSortedRecords(rng, 3000, 3), 3, layoutSpread)
	before := readFileBytes(t, files[3])
	// files[3], spelled differently.
	output := dir + string(filepath.Separator) + "." + string(filepath.Separator) + filepath.Base(files[3])
	if err := buildFilesWithWatchdog(t, context.Background(), output, files, sortedBuildTuning{}); err == nil {
		t.Fatal("a build into one of its inputs returned no error")
	}
	if !bytes.Equal(readFileBytes(t, files[3]), before) {
		t.Fatal("the build changed its input")
	}
}

// TestBuildFromSortedFilesRejects covers inputs the build must fail on, at
// one worker and several, and that a failed build removes its output.
func TestBuildFromSortedFilesRejects(t *testing.T) {
	const numKeys = 30000
	rng := newTestRNG(t)
	dir := t.TempDir()
	write := func(name string, payloadSize int, recs []sortedRecord) string {
		path := filepath.Join(dir, name+".sorted")
		writeSortedFile(t, path, SortedFileConfig{PayloadSize: payloadSize}, recs)
		return path
	}
	// swapped returns a copy of path with records i and i+1 swapped.
	swapped := func(path string, recordSize, i int) string {
		data := readFileBytes(t, path)
		swapRecords(data, recordSize, i)
		out := filepath.Join(dir, fmt.Sprintf("swapped%d-%s", i, filepath.Base(path)))
		if err := os.WriteFile(out, data, 0o600); err != nil {
			t.Fatal(err)
		}
		return out
	}
	recs3 := uniqueSortedRecords(rng, numKeys, 3)
	clean3 := write("clean3", 3, recs3)
	other3 := write("other3", 3, uniqueSortedRecords(rng, 5000, 3))
	recs0 := uniqueSortedRecords(rng, numKeys, 0)
	clean0 := write("clean0", 0, recs0)

	truncated := filepath.Join(dir, "truncated.sorted")
	if err := os.WriteFile(truncated, readFileBytes(t, clean3)[:sortedFileAlign+100*19+7], 0o600); err != nil {
		t.Fatal(err)
	}

	// A stripe boundary: with 9000-key stripes the geometry has a few blocks
	// per stripe, and the first record of stripe 1 is the first whose prefix
	// is at or past the stripe's bound.
	numBlocks, err := numBlocksForAlgo(AlgoBijection, uint64(len(recs3)), 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	geo, err := newStripeGeometry(numBlocks, uint64(len(recs3)), 9000)
	if err != nil {
		t.Fatal(err)
	}
	if geo.numStripes < 3 {
		t.Fatalf("%d stripes, want several", geo.numStripes)
	}
	bound, _ := geo.bound(0)
	boundary := slices.IndexFunc(recs3, func(r sortedRecord) bool { return binary.BigEndian.Uint64(r.key[:8]) >= bound })
	if boundary < 1 {
		t.Fatalf("no record at or past the first stripe bound %#x", bound)
	}

	// The seam between the smallest read buffers of 16-byte records.
	seam := sortedBuildBufferSize(1, MinKeySize, 1) / MinKeySize

	withDuplicate := slices.Insert(slices.Clone(recs3), 1000, recs3[1000])
	duplicateIn := write("duplicate", 3, withDuplicate)
	duplicateAcross := write("duplicate-across", 3, recs3[1000:1001])

	for _, tc := range []struct {
		name   string
		inputs []string
		tune   sortedBuildTuning
		want   error
	}{
		{"no inputs", nil, sortedBuildTuning{}, nil},
		{"mismatched payload sizes", []string{clean3, clean0}, sortedBuildTuning{}, nil},
		{"a truncated file", []string{clean3, truncated}, sortedBuildTuning{}, ErrTruncatedFile},
		{"a record out of order inside a run", []string{other3, swapped(clean3, 19, 777)}, sortedBuildTuning{stripeKeys: 9000}, ErrUnsortedInput},
		{"a record out of order across a read-buffer seam", []string{swapped(clean0, 16, seam-1)}, sortedBuildTuning{bufBytes: 1}, ErrUnsortedInput},
		{"a record outside its stripe", []string{other3, swapped(clean3, 19, boundary-1)}, sortedBuildTuning{stripeKeys: 9000}, ErrUnsortedInput},
		{"a duplicate key within one file", []string{duplicateIn}, sortedBuildTuning{}, ErrDuplicateKey},
		{"a duplicate key across two files", []string{clean3, duplicateAcross}, sortedBuildTuning{}, ErrDuplicateKey},
	} {
		for _, w := range []int{1, 4} {
			for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
				t.Run(fmt.Sprintf("%s/workers=%d/%s", tc.name, w, algo), func(t *testing.T) {
					output := filepath.Join(dir, "rejected.idx")
					err := buildFilesWithWatchdog(t, context.Background(), output, tc.inputs, tc.tune, WithWorkers(w), WithAlgorithm(algo))
					if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
						t.Fatalf("the build returned %v, want %v", err, tc.want)
					}
					if _, err := os.Stat(output); !os.IsNotExist(err) {
						t.Fatalf("a failed build left its output: %v", err)
					}
				})
			}
		}
	}
}

// TestBuildFromSortedFilesCanceled checks that a build canceled before it
// starts, or between stripes, returns the cancellation and removes its
// output.
func TestBuildFromSortedFilesCanceled(t *testing.T) {
	rng := newTestRNG(t)
	recs := uniqueSortedRecords(rng, 30000, 3)
	dir := t.TempDir()
	files := writeLayout(t, dir, rng, recs, 3, layoutSpread)
	for _, w := range []int{1, 4} {
		for _, at := range []int{-1, 0, 3} {
			t.Run(fmt.Sprintf("workers=%d/stripe=%d", w, at), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tune := sortedBuildTuning{stripeKeys: 1}
				if at < 0 {
					cancel()
				} else {
					tune.afterGather = func(s int) {
						if s == at {
							cancel()
						}
					}
				}
				output := filepath.Join(dir, "canceled.idx")
				err := buildFilesWithWatchdog(t, ctx, output, files, tune, WithFingerprint(1), WithWorkers(w))
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("the canceled build returned %v", err)
				}
				if _, err := os.Stat(output); !os.IsNotExist(err) {
					t.Fatalf("the canceled build left its output: %v", err)
				}
			})
		}
	}
}

// TestStripeGeometry checks the stripe bounds against FastRange32 and the
// split of blocks into stripes.
func TestStripeGeometry(t *testing.T) {
	rng := newTestRNG(t)
	for i := range 20000 {
		n := uint32(rng.Uint64N(math.MaxUint32-1)) + 2
		b := uint32(rng.Uint64N(uint64(n)-1)) + 1
		if i%4 == 0 {
			n = uint32(rng.IntN(1000)) + 2
			b = uint32(rng.IntN(int(n)-1)) + 1
		}
		p := lowPrefix(b, n)
		if intbits.FastRange32(p, n) != b || intbits.FastRange32(p-1, n) != b-1 {
			t.Fatalf("lowPrefix(%d, %d) = %#x maps to %d, and one below to %d", b, n, p, intbits.FastRange32(p, n), intbits.FastRange32(p-1, n))
		}
	}

	for _, tc := range []struct {
		numBlocks        uint32
		totalKeys        uint64
		stripeKeys       int
		blocksPerStripe  uint32
		numStripes       int
		lastStripeBlocks uint32
	}{
		{2, 0, 100000, 2, 1, 2},
		{2, 5, 100000, 2, 1, 2},
		{10, 30000, 9000, 3, 4, 1},
		{10, 30000, 1, 1, 10, 1},
		{10, 30000, 1 << 20, 10, 1, 10},
		{1000, 3_000_000, 32 << 10, 10, 100, 10},
	} {
		geo, err := newStripeGeometry(tc.numBlocks, tc.totalKeys, tc.stripeKeys)
		if err != nil {
			t.Fatal(err)
		}
		_, last := geo.blocks(geo.numStripes - 1)
		_, bounded := geo.bound(geo.numStripes - 1)
		if geo.blocksPerStripe != tc.blocksPerStripe || geo.numStripes != tc.numStripes || last != tc.lastStripeBlocks || bounded {
			t.Fatalf("%d blocks, %d keys, %d-key stripes: %+v, last stripe of %d blocks, bounded %v", tc.numBlocks, tc.totalKeys, tc.stripeKeys, geo, last, bounded)
		}
		for s := 0; s+1 < geo.numStripes; s++ {
			first, count := geo.blocks(s)
			next, _ := geo.blocks(s + 1)
			bound, bounded := geo.bound(s)
			if !bounded || first+count != next || intbits.FastRange32(bound, geo.numBlocks) != next || intbits.FastRange32(bound-1, geo.numBlocks) != next-1 {
				t.Fatalf("stripe %d of %+v: blocks %d+%d, next %d, bound %#x", s, geo, first, count, next, bound)
			}
		}
	}
}

// TestSortedBuildSizing checks the read buffer and stripe sizes against the
// budgets.
func TestSortedBuildSizing(t *testing.T) {
	for recordSize := MinKeySize; recordSize <= MinKeySize+maxPayloadSize; recordSize++ {
		// The smallest buffer that holds whole records and whole 4096-byte blocks.
		unit := directAlign * (recordSize / (recordSize & -recordSize))
		for _, files := range []int{1, 10, 11, 16, 100, 1000, 1024, 1025, 5000} {
			size := sortedBuildBufferSize(files, recordSize, 0)
			if size%unit != 0 {
				t.Fatalf("%d files of %d-byte records: a %d-byte buffer is not a multiple of %d", files, recordSize, size, unit)
			}
			want := min(max(sortedBuildReadBudget/2/files, sortedBuildMinBuffer), sortedBuildMaxBuffer)
			if size-want >= unit || want-size >= unit {
				t.Fatalf("%d files of %d-byte records: a %d-byte buffer, want within %d of %d", files, recordSize, size, unit, want)
			}
		}
		if size := sortedBuildBufferSize(1000, recordSize, 1); size != unit {
			t.Fatalf("%d-byte records: the smallest buffer is %d bytes, want %d", recordSize, size, unit)
		}
	}
	for _, tc := range []struct{ inFlight, recordSize, want int }{
		{3, 19, sortedBuildMaxStripeKeys},
		{18, 19, sortedBuildStripeBudget / (18 * (19 + 24))},
		{66, 24, sortedBuildStripeBudget / (66 * (24 + 24))},
		{130, 16, sortedBuildMinStripeKeys},
	} {
		if got := sortedBuildStripeKeys(tc.inFlight, tc.recordSize, 0); got != tc.want {
			t.Fatalf("%d stripes in flight, %d-byte records: %d-key stripes, want %d", tc.inFlight, tc.recordSize, got, tc.want)
		}
	}
	if got := sortedBuildStripeKeys(4, 19, 777); got != 777 {
		t.Fatalf("a forced stripe size came out as %d", got)
	}
}

// BenchmarkBuildFromSortedFiles builds 16M keys with 3-byte payloads and
// 1-byte fingerprints from 16 sorted files, against a SortedBuilder fed the
// same keys in block order from memory. 16M keys make 64 stripes, so that
// at 8 workers the last stripes do not dominate.
func BenchmarkBuildFromSortedFiles(b *testing.B) {
	const files, perFile = 16, 1_000_000
	dir := b.TempDir()
	rng := newTestRNG(b)
	paths := make([]string, files)
	var all []sortedRecord
	for i := range paths {
		paths[i] = filepath.Join(dir, fmt.Sprintf("in%02d.sorted", i))
		recs := stratifiedSortedRecords(rng, perFile, 3)
		writeSortedFile(b, paths[i], SortedFileConfig{PayloadSize: 3}, recs)
		all = append(all, recs...)
	}
	slices.SortFunc(all, compareSortedRecords)
	opts := []BuildOption{WithPayload(3), WithFingerprint(1)}
	output := filepath.Join(dir, "bench.idx")
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("files/workers=%d", workers), func(b *testing.B) {
			for b.Loop() {
				if err := BuildFromSortedFiles(context.Background(), output, paths, append(opts, WithWorkers(workers))...); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(all))*float64(b.N)/b.Elapsed().Seconds(), "keys/s")
		})
		b.Run(fmt.Sprintf("builder/workers=%d", workers), func(b *testing.B) {
			for b.Loop() {
				sb, err := NewSortedBuilder(context.Background(), output, uint64(len(all)), append(opts, WithWorkers(workers))...)
				if err != nil {
					b.Fatal(err)
				}
				for _, r := range all {
					if err := sb.AddKey(r.key[:], r.payload); err != nil {
						b.Fatal(err)
					}
				}
				if err := sb.Finish(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(all))*float64(b.N)/b.Elapsed().Seconds(), "keys/s")
		})
	}
}
