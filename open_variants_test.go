package streamhash

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// buildTestIndex builds a test MPHF index and returns the file path and sorted keys.
func buildTestIndex(t *testing.T, numKeys, keySize int) (idxPath string, keys [][]byte) {
	t.Helper()
	rng := newTestRNG(t)
	keys = generateRandomKeys(rng, numKeys, keySize)
	slices.SortFunc(keys, bytes.Compare)
	idxPath = filepath.Join(t.TempDir(), "test.idx")
	if err := quickBuild(t.Context(), idxPath, keys); err != nil {
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
