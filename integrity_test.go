package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/cespare/xxhash/v2"
)

// rehashIndex recomputes the HeaderHash and the RAM index entries' checks of
// the index in data, so that a test's patch reaches the code behind them.
func rehashIndex(data []byte) {
	off := uint64(headerSize)
	for range 2 {
		if off+4 > uint64(len(data)) {
			return
		}
		off += 4 + uint64(binary.LittleEndian.Uint32(data[off:]))
	}
	if off > uint64(len(data)) {
		return
	}
	binary.LittleEndian.PutUint64(data[headerHashOffset:], headerHash(data[:off]))
	entries := uint64(binary.LittleEndian.Uint32(data[14:])) + 1
	for i := uint64(0); i < entries && off+ramIndexEntrySize <= uint64(len(data)); i++ {
		entry := data[off : off+ramIndexEntrySize]
		binary.LittleEndian.PutUint32(entry[ramIndexEntryDataSize:], ramIndexEntryCheck(uint32(i), entry))
		off += ramIndexEntrySize
	}
}

// rehashSortedFile recomputes the RecordsHash and HeaderHash of the sorted
// file in data, so that a test's patch reaches the code behind them.
func rehashSortedFile(data []byte) {
	if len(data) < sortedFileFixedSize {
		return
	}
	metadataLen := int(binary.LittleEndian.Uint32(data[sortedFileMetadataLenOffset:]))
	if metadataLen > len(data)-sortedFileFixedSize {
		return
	}
	if off := sortedFileRecordsOffset(metadataLen); off <= int64(len(data)) {
		binary.LittleEndian.PutUint64(data[sortedFileRecordsHashOffset:], xxhash.Sum64(data[off:]))
	}
	hash := sortedFileHeaderHash(data[:sortedFileFixedSize], data[sortedFileFixedSize:sortedFileFixedSize+metadataLen])
	binary.LittleEndian.PutUint64(data[sortedFileHeaderHashOffset:], hash)
}

// integrityIndex builds an index of several blocks, with payloads,
// fingerprints and user metadata, and returns its bytes and records.
func integrityIndex(t *testing.T) ([]byte, []sortedRecord) {
	t.Helper()
	recs := uniqueSortedRecords(newTestRNG(t), 20000, 3)
	path := filepath.Join(t.TempDir(), "index.idx")
	return referenceIndex(t, path, recs, WithPayload(3), WithFingerprint(1), WithMetadata([]byte("user metadata"))), recs
}

// TestOpenChecksHeaderHash flips a bit in every byte Open reads: the header,
// the user metadata and the algorithm config.
func TestOpenChecksHeaderHash(t *testing.T) {
	data, _ := integrityIndex(t)
	idx, err := OpenBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	for off := range idx.ramIndexOffset {
		bad := slices.Clone(data)
		bad[off] ^= 0x10
		_, err := OpenBytes(bad)
		if err == nil {
			t.Fatalf("Open accepted a flipped bit in byte %d", off)
		}
		// No other check covers TotalKeys' low bytes, the seed or the user
		// metadata.
		if (off >= 6 && off < 10 || off >= 23 && off < 31 || off >= 68 && off < 81) && !errors.Is(err, ErrChecksumFailed) {
			t.Fatalf("a flipped bit in byte %d returned %v, want ErrChecksumFailed", off, err)
		}
	}
}

// TestLookupChecksRAMIndexEntry flips a bit in each field of a block's RAM
// index entry, and moves an intact entry to another block's position.
func TestLookupChecksRAMIndexEntry(t *testing.T) {
	data, recs := integrityIndex(t)
	clean, err := OpenPayloadBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	numBlocks := clean.NumBlocks()
	if numBlocks < 4 {
		t.Fatalf("the index has %d blocks, the test needs 4", numBlocks)
	}
	const block = 2
	entry := func(data []byte, b uint32) []byte {
		return data[clean.ramIndexOffset+uint64(b)*ramIndexEntrySize:][:ramIndexEntrySize]
	}
	for _, tc := range []struct {
		name   string
		mutate func(data []byte)
	}{
		{"KeysBefore", func(data []byte) { entry(data, block)[0] ^= 1 }},
		{"MetadataOffset", func(data []byte) { entry(data, block)[5] ^= 1 }},
		{"another block's entry", func(data []byte) { copy(entry(data, block), entry(data, block+1)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := slices.Clone(data)
			tc.mutate(bad)
			idx, err := OpenPayloadBytes(bad)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range recs {
				// A lookup reads its block's entry and the next.
				b := blockIndexFromPrefix(extractPrefix(r.key[:]), numBlocks)
				reads := b == block || b == block-1
				_, payload, err := idx.QueryPayload(r.key[:])
				if reads && !errors.Is(err, ErrChecksumFailed) {
					t.Fatalf("a lookup in block %d returned %v, want ErrChecksumFailed", b, err)
				}
				if !reads && (err != nil || payload != r.payload) {
					t.Fatalf("a lookup in block %d returned %d, %v", b, payload, err)
				}
			}
			if err := idx.Verify(); !errors.Is(err, ErrChecksumFailed) {
				t.Fatalf("Verify returned %v, want ErrChecksumFailed", err)
			}
		})
	}
}

// TestVerifyCoversWholeFile flips a bit in bytes past the ones Open checks:
// every byte of the RAM index and the footer, its reserved bytes included,
// and a sample of the regions between them.
func TestVerifyCoversWholeFile(t *testing.T) {
	data, _ := integrityIndex(t)
	idx, err := OpenBytes(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Verify(); err != nil {
		t.Fatal(err)
	}
	flipped := func(off int) {
		t.Helper()
		data[off] ^= 0x04
		if err := idx.Verify(); err == nil {
			t.Fatalf("Verify accepted a flipped bit in byte %d of %d", off, len(data))
		}
		data[off] ^= 0x04
	}
	for off := int(idx.ramIndexOffset); off < int(idx.payloadRegionOffset); off++ {
		flipped(off)
	}
	for off := int(idx.payloadRegionOffset); off < len(data)-footerSize; off += 97 {
		flipped(off)
	}
	for off := len(data) - footerSize; off < len(data); off++ {
		flipped(off)
	}
	data[headerHashOffset-1] ^= 0x04
	if err := idx.Verify(); !errors.Is(err, ErrChecksumFailed) {
		t.Fatalf("Verify returned %v for a header changed since Open, want ErrChecksumFailed", err)
	}
}

// TestSortedFileRecordsHash flips a bit in a record's payload, which leaves
// the records in order, and checks that everything that reads the records
// rejects the file.
func TestSortedFileRecordsHash(t *testing.T) {
	dir := t.TempDir()
	rng := newTestRNG(t)
	recs := uniqueSortedRecords(rng, 30000, 3)
	clean, other := filepath.Join(dir, "clean.sorted"), filepath.Join(dir, "other.sorted")
	writeSortedFile(t, clean, SortedFileConfig{PayloadSize: 3, Metadata: []byte("meta")}, recs)
	writeSortedFile(t, other, SortedFileConfig{PayloadSize: 3}, uniqueSortedRecords(rng, 5000, 3))

	// The first and last records, and one past a refill of the merge's buffer.
	for _, i := range []int{0, 431, len(recs) - 1} {
		data := readFileBytes(t, clean)
		data[sortedFileAlign+i*19+MinKeySize] ^= 0x01
		corrupt := filepath.Join(dir, "corrupt.sorted")
		if err := os.WriteFile(corrupt, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadSortedFileHeader(corrupt); err != nil {
			t.Fatalf("record %d: the header: %v", i, err)
		}

		for _, w := range []int{1, 4} {
			output := filepath.Join(dir, "corrupt.idx")
			err := buildFilesWithWatchdog(t, context.Background(), output, []string{other, corrupt}, sortedBuildTuning{}, WithWorkers(w))
			if !errors.Is(err, ErrChecksumFailed) {
				t.Fatalf("record %d: the build with %d workers returned %v, want ErrChecksumFailed", i, w, err)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("a failed build left its output: %v", err)
			}
		}

		output := filepath.Join(dir, "merged.sorted")
		err := mergeWithWatchdog(t, context.Background(), output, []string{other, corrupt}, SortedFileConfig{PayloadSize: 3}, 2*directAlign)
		if !errors.Is(err, ErrChecksumFailed) {
			t.Fatalf("record %d: the merge returned %v, want ErrChecksumFailed", i, err)
		}
		if _, err := os.Stat(output); !os.IsNotExist(err) {
			t.Fatalf("a failed merge left its output: %v", err)
		}

		r, err := OpenSortedFile(corrupt)
		if err != nil {
			t.Fatal(err)
		}
		for range len(recs) - 1 {
			if _, _, err := r.Next(); err != nil {
				t.Fatalf("record %d: Next returned %v before the last record", i, err)
			}
		}
		if _, _, err := r.Next(); !errors.Is(err, ErrChecksumFailed) {
			t.Fatalf("record %d: Next returned %v for the last record, want ErrChecksumFailed", i, err)
		}
		r.Close()
	}
}

// TestSortedFileHeaderHash flips a bit in every byte of the fixed header and
// the metadata.
func TestSortedFileHeaderHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "header.sorted")
	writeSortedFile(t, path, SortedFileConfig{PayloadSize: 3, Metadata: []byte("meta")}, randomSortedRecords(newTestRNG(t), 10, 3))
	data := readFileBytes(t, path)
	for off := range sortedFileFixedSize + len("meta") {
		data[off] ^= 0x20
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := ReadSortedFileHeader(path)
		if err == nil {
			t.Fatalf("a flipped bit in byte %d was accepted", off)
		}
		// Past the magic, version, payload size and metadata length, only
		// the hash covers the header. A flipped count is caught by it first.
		if off >= 7 && off < sortedFileMetadataLenOffset || off >= sortedFileFixedSize {
			if !errors.Is(err, ErrChecksumFailed) {
				t.Fatalf("a flipped bit in byte %d returned %v, want ErrChecksumFailed", off, err)
			}
		}
		data[off] ^= 0x20
	}
}

// TestSortedFileUnfinishedWriter checks that a file whose writer never reached
// Finish does not open, with or without records on disk.
func TestSortedFileUnfinishedWriter(t *testing.T) {
	for _, records := range []int{0, 10} {
		path := filepath.Join(t.TempDir(), "unfinished.sorted")
		w, err := NewSortedFileWriter(path, SortedFileConfig{PayloadSize: 3})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range randomSortedRecords(newTestRNG(t), records, 3) {
			if err := w.Add(r.key[:], r.payload); err != nil {
				t.Fatal(err)
			}
		}
		// As a crash would leave it: written, never finished.
		if err := w.bw.Flush(); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadSortedFileHeader(path); !errors.Is(err, ErrChecksumFailed) {
			t.Fatalf("%d records: ReadSortedFileHeader returned %v, want ErrChecksumFailed", records, err)
		}
		if r, err := OpenSortedFile(path); err == nil {
			r.Close()
			t.Fatalf("%d records: OpenSortedFile accepted the file", records)
		}
		if err := w.f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}
