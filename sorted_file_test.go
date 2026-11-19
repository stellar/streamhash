package streamhash

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

type sortedRecord struct {
	key     [MinKeySize]byte
	payload uint64
}

func compareSortedRecords(a, b sortedRecord) int {
	if c := bytes.Compare(a.key[:], b.key[:]); c != 0 {
		return c
	}
	return cmp.Compare(a.payload, b.payload)
}

// randomSortedRecords returns n records in key order, about one in sixteen
// repeating the key before it, with random payloads of payloadSize bytes.
func randomSortedRecords(rng *rand.Rand, n, payloadSize int) []sortedRecord {
	recs := make([]sortedRecord, n)
	for i := range recs {
		if i > 0 && rng.IntN(16) == 0 {
			recs[i].key = recs[i-1].key
		} else {
			fillFromRNG(rng, recs[i].key[:])
		}
		recs[i].payload = rng.Uint64() & payloadMaskFor(payloadSize)
	}
	slices.SortFunc(recs, compareSortedRecords)
	return recs
}

// uniqueSortedRecords is randomSortedRecords without the repeated keys.
func uniqueSortedRecords(rng *rand.Rand, n, payloadSize int) []sortedRecord {
	return slices.CompactFunc(randomSortedRecords(rng, n, payloadSize), func(a, b sortedRecord) bool { return a.key == b.key })
}

// swapRecords swaps records i and i+1 in the bytes of a sorted file whose
// header is 4096 bytes.
func swapRecords(data []byte, recordSize, i int) {
	pair := data[sortedFileAlign+i*recordSize:][:2*recordSize]
	copy(pair, slices.Concat(pair[recordSize:], pair[:recordSize]))
}

func writeSortedFile(t testing.TB, path string, cfg SortedFileConfig, recs []sortedRecord) {
	t.Helper()
	w, err := NewSortedFileWriter(path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, r := range recs {
		if err := w.Add(r.key[:], r.payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
}

func readSortedFile(t testing.TB, path string) []sortedRecord {
	t.Helper()
	r, err := OpenSortedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var recs []sortedRecord
	for {
		key, payload, err := r.Next()
		if err == io.EOF {
			return recs
		}
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, sortedRecord{key, payload})
	}
}

func TestSortedFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rng := newTestRNG(t)
	for ps := 0; ps <= maxPayloadSize; ps++ {
		t.Run(fmt.Sprintf("payload=%d", ps), func(t *testing.T) {
			recs := randomSortedRecords(rng, 5000, ps)
			// The smallest and largest keys, the second with the largest payload.
			recs[0] = sortedRecord{}
			recs = append(recs, sortedRecord{payload: payloadMaskFor(ps)})
			recs[len(recs)-1].key = [MinKeySize]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
			meta := fmt.Appendf(nil, "metadata for %d-byte payloads", ps)
			path := filepath.Join(dir, fmt.Sprintf("payload%d.sorted", ps))
			writeSortedFile(t, path, SortedFileConfig{PayloadSize: ps, Metadata: meta}, recs)

			h, err := ReadSortedFileHeader(path)
			if err != nil {
				t.Fatal(err)
			}
			if h.PayloadSize != ps || h.Count != uint64(len(recs)) || !bytes.Equal(h.Metadata, meta) {
				t.Fatalf("header %+v, want payload size %d, count %d, metadata %q", h, ps, len(recs), meta)
			}
			if got := readSortedFile(t, path); !slices.Equal(got, recs) {
				t.Fatal("records read back differ from the records written")
			}
		})
	}
}

// TestSortedFileLayout pins the bytes on disk to streamhash-spec.md §3.9.
func TestSortedFileLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "layout.sorted")
	recs := []sortedRecord{
		{key: [MinKeySize]byte{0x01, 15: 0xaa}, payload: 0x030201},
		{key: [MinKeySize]byte{0x02, 15: 0xbb}, payload: 0x060504},
	}
	writeSortedFile(t, path, SortedFileConfig{PayloadSize: 3, Metadata: []byte("meta")}, recs)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := make([]byte, 4096)
	copy(want, []byte{0x46, 0x53, 0x54, 0x53, 0x01, 0x00, 0x03, 0x00})
	binary.LittleEndian.PutUint64(want[8:], 2)
	binary.LittleEndian.PutUint32(want[16:], 4)
	copy(want[20:], "meta")
	want = append(want, recs[0].key[:]...)
	want = append(want, 0x01, 0x02, 0x03)
	want = append(want, recs[1].key[:]...)
	want = append(want, 0x04, 0x05, 0x06)
	if !bytes.Equal(data, want) {
		t.Fatalf("file bytes differ from the layout\ngot:  %x\nwant: %x", data, want)
	}
}

// TestSortedFileMetadataSizes checks that the header pads to the next
// multiple of 4096 when the metadata does not fit in the first one, up to
// the maximum, with and without records.
func TestSortedFileMetadataSizes(t *testing.T) {
	dir := t.TempDir()
	three := randomSortedRecords(newTestRNG(t), 3, 2)
	for _, tc := range []struct{ metadataLen, recordsOffset int }{
		{0, 4096},
		{4096 - sortedFileFixedSize, 4096},
		{4096 - sortedFileFixedSize + 1, 8192},
		{maxSortedFileMetadata, 69632},
	} {
		for _, recs := range [][]sortedRecord{nil, three} {
			path := filepath.Join(dir, fmt.Sprintf("meta%d-records%d.sorted", tc.metadataLen, len(recs)))
			meta := bytes.Repeat([]byte{0x5a}, tc.metadataLen)
			writeSortedFile(t, path, SortedFileConfig{PayloadSize: 2, Metadata: meta}, recs)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if want := int64(tc.recordsOffset + len(recs)*(MinKeySize+2)); info.Size() != want {
				t.Fatalf("%d bytes of metadata, %d records: file is %d bytes, want %d", tc.metadataLen, len(recs), info.Size(), want)
			}
			h, err := ReadSortedFileHeader(path)
			if err != nil {
				t.Fatal(err)
			}
			if h.Count != uint64(len(recs)) || h.PayloadSize != 2 || !bytes.Equal(h.Metadata, meta) {
				t.Fatalf("%d bytes of metadata, %d records: header %+v", tc.metadataLen, len(recs), h)
			}
			if got := readSortedFile(t, path); !slices.Equal(got, recs) {
				t.Fatalf("%d bytes of metadata, %d records: records read back differ", tc.metadataLen, len(recs))
			}
		}
	}
}

func TestNewSortedFileWriterRejectsConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.sorted")
	for _, cfg := range []SortedFileConfig{
		{PayloadSize: -1},
		{PayloadSize: maxPayloadSize + 1},
		{Metadata: make([]byte, maxSortedFileMetadata+1)},
	} {
		if _, err := NewSortedFileWriter(path, cfg); err == nil {
			t.Fatalf("payload size %d and %d bytes of metadata: want an error", cfg.PayloadSize, len(cfg.Metadata))
		} else if cfg.PayloadSize != 0 && !errors.Is(err, ErrPayloadTooLarge) {
			t.Fatalf("payload size %d: got %v, want ErrPayloadTooLarge", cfg.PayloadSize, err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("a rejected config created the file: %v", err)
		}
	}
}

func TestSortedFileAddRejects(t *testing.T) {
	dir := t.TempDir()
	key := func(first, last byte) []byte {
		k := make([]byte, MinKeySize)
		k[0], k[MinKeySize-1] = first, last
		return k
	}
	path := filepath.Join(dir, "rejects.sorted")
	w, err := NewSortedFileWriter(path, SortedFileConfig{PayloadSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	steps := []struct {
		key     []byte
		payload uint64
		want    error
	}{
		{key(5, 5)[:MinKeySize-1], 0, ErrKeyTooShort},
		{key(5, 5), 1 << 24, ErrPayloadOverflow},
		{key(5, 5), 1, nil},
		{key(5, 5), 2, nil}, // a key equal to the one before it
		{append(key(5, 5), 0x00), 3, nil},
		{key(4, 9), 0, ErrUnsortedInput},
		{key(5, 4), 0, ErrUnsortedInput},
		{key(6, 0), 1<<24 - 1, nil},
	}
	for i, s := range steps {
		if err := w.Add(s.key, s.payload); !errors.Is(err, s.want) {
			t.Fatalf("step %d: Add returned %v, want %v", i, err, s.want)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	var want []sortedRecord
	for _, s := range steps {
		if s.want == nil {
			want = append(want, sortedRecord{key: [MinKeySize]byte(s.key), payload: s.payload})
		}
	}
	if got := readSortedFile(t, path); !slices.Equal(got, want) {
		t.Fatalf("read back %v, want only the records Add accepted: %v", got, want)
	}

	for _, ps := range []int{0, 1, 7, 8} {
		w, err := NewSortedFileWriter(filepath.Join(dir, "payload.sorted"), SortedFileConfig{PayloadSize: ps})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Add(key(1, 1), payloadMaskFor(ps)); err != nil {
			t.Fatalf("payload size %d: Add of the largest payload returned %v", ps, err)
		}
		if ps < 8 {
			if err := w.Add(key(1, 1), payloadMaskFor(ps)+1); !errors.Is(err, ErrPayloadOverflow) {
				t.Fatalf("payload size %d: Add of a payload one too large returned %v", ps, err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSortedFileWriterLifecycle checks that Close without Finish removes the
// file, Close after Finish keeps it, and a closed writer refuses more calls.
func TestSortedFileWriterLifecycle(t *testing.T) {
	k := make([]byte, MinKeySize)
	for _, tc := range []struct {
		name string
		end  func(*SortedFileWriter) error
		kept bool
	}{
		{"Close", (*SortedFileWriter).Close, false},
		{"Finish", (*SortedFileWriter).Finish, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "w.sorted")
			w, err := NewSortedFileWriter(path, SortedFileConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Add(k, 0); err != nil {
				t.Fatal(err)
			}
			if err := tc.end(w); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatalf("Close after %s returned %v", tc.name, err)
			}
			h, err := ReadSortedFileHeader(path)
			if tc.kept && (err != nil || h.Count != 1) {
				t.Fatalf("header %+v, %v, want the finished file", h, err)
			}
			if !tc.kept && !os.IsNotExist(err) {
				t.Fatalf("Close without Finish left the file: %v", err)
			}
			if err := w.Add(k, 0); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("Add after %s returned %v", tc.name, err)
			}
			if err := w.Finish(); !errors.Is(err, os.ErrClosed) {
				t.Fatalf("Finish after %s returned %v", tc.name, err)
			}
		})
	}
}

// TestSortedFileBytesPerSync checks that writeback changes nothing in the
// file, even when it starts on every flush.
func TestSortedFileBytesPerSync(t *testing.T) {
	dir := t.TempDir()
	recs := randomSortedRecords(newTestRNG(t), 200000, 3)
	var files [][]byte
	for _, bytesPerSync := range []int{0, 1, 2 << 20} {
		path := filepath.Join(dir, fmt.Sprintf("sync%d.sorted", bytesPerSync))
		writeSortedFile(t, path, SortedFileConfig{PayloadSize: 3, BytesPerSync: bytesPerSync}, recs)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, data)
	}
	for i := 1; i < len(files); i++ {
		if !bytes.Equal(files[0], files[i]) {
			t.Fatal("the file written with BytesPerSync differs from the one written without it")
		}
	}
}

func TestReadSortedFileHeaderRejects(t *testing.T) {
	dir := t.TempDir()
	rng := newTestRNG(t)
	valid := func(payloadSize int) []byte {
		path := filepath.Join(dir, fmt.Sprintf("valid%d.sorted", payloadSize))
		writeSortedFile(t, path, SortedFileConfig{PayloadSize: payloadSize, Metadata: []byte("meta")}, randomSortedRecords(rng, 10, payloadSize))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	data3, data0 := valid(3), valid(0)
	indexPath, _ := buildTestIndex(t, 1000, 16)
	index, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	mutated := func(data []byte, mutate func([]byte)) []byte {
		b := slices.Clone(data)
		mutate(b)
		return b
	}

	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{"shorter than the fixed header", data3[:sortedFileFixedSize-1], ErrTruncatedFile},
		{"cut in the padding", data3[:100], ErrTruncatedFile},
		{"metadata longer than the file", mutated(data3, func(b []byte) { binary.LittleEndian.PutUint32(b[16:], 5000) }), ErrTruncatedFile},
		{"cut mid-record", data3[:len(data3)-1], ErrTruncatedFile},
		{"cut a whole record", data3[:len(data3)-19], ErrTruncatedFile},
		{"count one too large", mutated(data3, func(b []byte) { binary.LittleEndian.PutUint64(b[8:], 11) }), ErrTruncatedFile},
		// 16 × (10 + 2^60) wraps to 16 × 10, so only a division catches it.
		{"count that overflows", mutated(data0, func(b []byte) { binary.LittleEndian.PutUint64(b[8:], 10+1<<60) }), ErrTruncatedFile},
		{"an extra record", append(slices.Clone(data3), make([]byte, 19)...), ErrKeyCountMismatch},
		{"an unfinished writer's count of 0", mutated(data3, func(b []byte) { binary.LittleEndian.PutUint64(b[8:], 0) }), ErrKeyCountMismatch},
		{"wrong magic", mutated(data3, func(b []byte) { b[0] ^= 1 }), ErrInvalidMagic},
		{"an index file", index, ErrInvalidMagic},
		{"wrong version", mutated(data3, func(b []byte) { b[4] = 2 }), ErrInvalidVersion},
		{"payload size over 8", mutated(data3, func(b []byte) { b[6] = maxPayloadSize + 1 }), ErrCorruptedIndex},
		{"metadata length over the maximum", mutated(data3, func(b []byte) { binary.LittleEndian.PutUint32(b[16:], maxSortedFileMetadata+1) }), ErrCorruptedIndex},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, "bad.sorted")
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadSortedFileHeader(path); !errors.Is(err, tc.want) {
				t.Fatalf("ReadSortedFileHeader returned %v, want %v", err, tc.want)
			}
			if r, err := OpenSortedFile(path); !errors.Is(err, tc.want) {
				if r != nil {
					r.Close()
				}
				t.Fatalf("OpenSortedFile returned %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSortedFileReaderRejectsUnsorted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unsorted.sorted")
	writeSortedFile(t, path, SortedFileConfig{PayloadSize: 2}, uniqueSortedRecords(newTestRNG(t), 20, 2))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	swapRecords(data, MinKeySize+2, 5)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	r, err := OpenSortedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := range 6 {
		if _, _, err := r.Next(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if _, _, err := r.Next(); !errors.Is(err, ErrUnsortedInput) {
		t.Fatalf("the record out of order returned %v, want ErrUnsortedInput", err)
	}
}

// TestSortedFileReaderFileShrinks covers a file cut after its header was checked.
func TestSortedFileReaderFileShrinks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shrinks.sorted")
	writeSortedFile(t, path, SortedFileConfig{PayloadSize: 3}, randomSortedRecords(newTestRNG(t), 10, 3))
	r, err := OpenSortedFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := os.Truncate(path, sortedFileAlign+9*(MinKeySize+3)+1); err != nil {
		t.Fatal(err)
	}
	for i := range 9 {
		if _, _, err := r.Next(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	if _, _, err := r.Next(); !errors.Is(err, ErrTruncatedFile) {
		t.Fatalf("the cut record returned %v, want ErrTruncatedFile", err)
	}
}

// TestSortedFileFeedsSortedBuilder checks that a sorted file's key order is
// one SortedBuilder accepts: bytewise order of the first 16 bytes is
// non-decreasing block order.
func TestSortedFileFeedsSortedBuilder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "keys.sorted")
	writeSortedFile(t, path, SortedFileConfig{PayloadSize: 3}, uniqueSortedRecords(newTestRNG(t), 20000, 3))
	var keys [][]byte
	var payloads []uint64
	for _, r := range readSortedFile(t, path) {
		keys = append(keys, r.key[:])
		payloads = append(payloads, r.payload)
	}
	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		idx := buildAndOpen(t, keys, payloads, WithAlgorithm(algo), WithPayload(3), WithFingerprint(1))
		verifyPayloads(t, idx, keys, payloads, 3)
		idx.Close()
	}
}

// BenchmarkSortedFileWriter adds 1M records with 3-byte payloads. Finish,
// which syncs, is not timed.
func BenchmarkSortedFileWriter(b *testing.B) {
	const n = 1_000_000
	recs := randomSortedRecords(newTestRNG(b), n, 3)
	path := filepath.Join(b.TempDir(), "bench.sorted")
	for b.Loop() {
		w, err := NewSortedFileWriter(path, SortedFileConfig{PayloadSize: 3})
		if err != nil {
			b.Fatal(err)
		}
		for _, r := range recs {
			if err := w.Add(r.key[:], r.payload); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		if err := w.Finish(); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "records/s")
}
