package streamhash

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// buildTestIndex builds a test MPHF index and returns the file path and sorted keys.
func buildTestIndex(t *testing.T, numKeys, keySize int, opts ...BuildOption) (idxPath string, keys [][]byte) {
	t.Helper()
	rng := newTestRNG(t)
	keys = generateRandomKeys(rng, numKeys, keySize)
	slices.SortFunc(keys, bytes.Compare)
	idxPath = filepath.Join(t.TempDir(), "test.idx")
	if err := quickBuild(t.Context(), idxPath, keys, opts...); err != nil {
		t.Fatalf("quickBuild: %v", err)
	}
	return idxPath, keys
}

// TestOpenFile verifies that OpenFile produces an index that agrees with Open
// on all queries and Verify.
func TestOpenFile(t *testing.T) {
	idxPath, keys := buildTestIndex(t, 500, 24)

	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("os.Open: %v", err)
	}
	defer f.Close()

	idxFile, err := OpenFile(f)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer idxFile.Close()

	// Verify all keys produce valid ranks in [0, N).
	verifyMPHF(t, idxFile, keys)

	// Verify integrity.
	if err := idxFile.Verify(); err != nil {
		t.Errorf("Verify after OpenFile: %v", err)
	}
}

// TestOpenBytes verifies that OpenBytes produces an index that agrees with Open
// on all queries and Verify.
func TestOpenBytes(t *testing.T) {
	idxPath, keys := buildTestIndex(t, 500, 24)

	// Read the file into memory.
	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	idxBytes, err := OpenBytes(data)
	if err != nil {
		t.Fatalf("OpenBytes: %v", err)
	}
	defer idxBytes.Close()

	// Verify all keys produce valid ranks in [0, N).
	verifyMPHF(t, idxBytes, keys)

	// Verify integrity.
	if err := idxBytes.Verify(); err != nil {
		t.Errorf("Verify after OpenBytes: %v", err)
	}
}

// TestOpenBytesMatchesOpen verifies that OpenBytes and Open return the same
// ranks for every key.
func TestOpenBytesMatchesOpen(t *testing.T) {
	idxPath, keys := buildTestIndex(t, 200, 16)

	idxMmap, err := Open(idxPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer idxMmap.Close()

	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	idxBytes, err := OpenBytes(data)
	if err != nil {
		t.Fatalf("OpenBytes: %v", err)
	}
	defer idxBytes.Close()

	for i, key := range keys {
		r1, err1 := idxMmap.QueryRank(key)
		r2, err2 := idxBytes.QueryRank(key)
		if err1 != nil || err2 != nil {
			t.Errorf("key %d: Open err=%v, OpenBytes err=%v", i, err1, err2)
			continue
		}
		if r1 != r2 {
			t.Errorf("key %d: Open rank=%d, OpenBytes rank=%d", i, r1, r2)
		}
	}
}

// TestOpenBytesTruncated verifies that OpenBytes rejects truncated data
// with ErrTruncatedFile.
func TestOpenBytesTruncated(t *testing.T) {
	_, err := OpenBytes(make([]byte, 100))
	if !errors.Is(err, sherr.ErrTruncatedFile) {
		t.Fatalf("expected ErrTruncatedFile, got %v", err)
	}
}

// TestOpenBytesCloseIsNoop verifies that Close on an OpenBytes index is safe
// to call multiple times and that queries fail after close.
func TestOpenBytesCloseIsNoop(t *testing.T) {
	idxPath, keys := buildTestIndex(t, 50, 16)

	data, err := os.ReadFile(idxPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	idx, err := OpenBytes(data)
	if err != nil {
		t.Fatalf("OpenBytes: %v", err)
	}

	// Close multiple times — should not panic or error.
	if err := idx.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := idx.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	// Queries should fail after close.
	_, err = idx.QueryRank(keys[0])
	if !errors.Is(err, sherr.ErrIndexClosed) {
		t.Errorf("Query after Close: expected ErrIndexClosed, got %v", err)
	}
}

// TestOpenFileDoesNotCloseFile verifies that after OpenFile returns,
// the original file descriptor is still usable by the caller.
func TestOpenFileDoesNotCloseFile(t *testing.T) {
	idxPath, keys := buildTestIndex(t, 50, 16)

	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("os.Open: %v", err)
	}
	defer f.Close()

	idx, err := OpenFile(f)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer idx.Close()

	// Verify file descriptor was NOT closed by OpenFile — reading from f should succeed.
	buf := make([]byte, 1)
	_, err = f.ReadAt(buf, 0)
	if err != nil {
		t.Errorf("ReadAt after OpenFile failed: %v (fd should still be open)", err)
	}

	// Index should still work.
	verifyMPHF(t, idx, keys)
}

// TestMaxBlockKeys pins the format-level block ceiling: version 1 stores
// per-block cumulative key counts as uint16, so an opened index must report
// exactly that ceiling, and it must bound any real block's occupancy (for a
// single-block index, the whole key count).
func TestMaxBlockKeys(t *testing.T) {
	idxPath, _ := buildTestIndex(t, 500, 24)
	idx, err := Open(idxPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer idx.Close()
	if got := idx.MaxBlockKeys(); got != math.MaxUint16 {
		t.Fatalf("MaxBlockKeys = %d, want %d (uint16 format ceiling)", got, math.MaxUint16)
	}
	// Every real block's occupancy must respect the advertised ceiling —
	// checked against adjacent cumulative counts in the RAM index (the
	// sentinel entry at NumBlocks makes the last block's count defined).
	for i := uint32(0); i < idx.NumBlocks(); i++ {
		occ := idx.ramEntry(i+1).KeysBefore - idx.ramEntry(i).KeysBefore
		if occ > uint64(idx.MaxBlockKeys()) {
			t.Fatalf("block %d holds %d keys, above the reported ceiling %d", i, occ, idx.MaxBlockKeys())
		}
	}
}

// prefetch must ask for every RAM index and metadata byte the keys' lookups
// read, and little more. The test models a cold cache: the RAM index and
// metadata are garbage until prefetch asks for them, and asking copies the real
// bytes in. The lookups must then answer as they do on the real index, which
// also shows prefetch read no entry before asking for it.
func TestPrefetchAsksForWhatLookupsRead(t *testing.T) {
	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		path, keys := buildTestIndex(t, 500_000, MinKeySize, WithAlgorithm(algo), WithPayload(4), WithFingerprint(2))
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		real, err := OpenBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		ram := byteRange{real.ramIndexOffset, real.ramIndexOffset + uint64(len(real.ramIndexBytes))}
		meta := byteRange{real.metadataRegionOffset, uint64(len(data)) - footerSize}

		cache := slices.Clone(data)
		for _, r := range []byteRange{ram, meta} {
			for i := r.start; i < r.end; i++ {
				cache[i] ^= 0xFF
			}
		}
		idx, err := OpenBytes(cache)
		if err != nil {
			t.Fatal(err)
		}
		batch := make([][]byte, 5)
		for i := range batch {
			batch[i] = keys[i*len(keys)/len(batch)] // keys are sorted: spread them over the blocks
		}
		var asked uint64
		idx.prefetch(batch, func(start, end uint64) {
			copy(cache[start:end], data[start:end])
			asked += end - start
		})

		if all := ram.end - ram.start + meta.end - meta.start; asked*2 > all {
			t.Fatalf("%v: asked for %d of %d bytes, not much less than everything", algo, asked, all)
		}
		for _, key := range batch {
			want, _ := real.QueryRank(key)
			if got, err := idx.QueryRank(key); err != nil || got != want {
				t.Fatalf("%v: a lookup read bytes prefetch did not ask for: rank %d, %v; want %d", algo, got, err, want)
			}
		}
	}
}

// QueryBatch must give each key the answer the single-key lookups give it, in
// its own slot, whether the lookups run in turn or in goroutines.
func TestQueryBatch(t *testing.T) {
	keys := generateRandomKeys(newTestRNG(t), 10_000, MinKeySize)
	payloads := make([]uint64, len(keys))
	for i := range payloads {
		payloads[i] = uint64(i)
	}
	batch := append(keys[:100:100], make([]byte, MinKeySize), []byte("short"))
	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		for _, sizes := range []struct{ payload, fingerprint int }{{0, 0}, {0, 2}, {3, 0}, {4, 1}} {
			idx := buildAndOpenUnsorted(t, keys, payloads, t.TempDir(),
				WithAlgorithm(algo), WithPayload(sizes.payload), WithFingerprint(sizes.fingerprint))
			pi, _ := idx.WithPayload()
			for _, concurrency := range []int{1, 4} {
				for i, got := range idx.QueryBatch(batch, concurrency) {
					var want Result
					if sizes.payload == 0 {
						want.Rank, want.Err = idx.QueryRank(batch[i])
					} else {
						want.Rank, want.Payload, want.Err = pi.QueryPayload(batch[i])
					}
					if got != want {
						t.Fatalf("%v, %+v, concurrency %d, key %d: got %+v, want %+v",
							algo, sizes, concurrency, i, got, want)
					}
				}
			}
			idx.Close()
		}
	}
}
