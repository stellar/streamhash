package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// buildEmptyIndex builds a zero-key index via the requested builder type and
// options, returning the file path.
func buildEmptyIndex(t *testing.T, unsorted bool, opts ...BuildOption) string {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.idx")

	if unsorted {
		b, err := NewUnsortedBuilder(ctx, path, 0, dir, opts...)
		if err != nil {
			t.Fatalf("NewUnsortedBuilder(0): %v", err)
		}
		if err := b.Finish(); err != nil {
			t.Fatalf("unsorted Finish: %v", err)
		}
		return path
	}

	b, err := NewSortedBuilder(ctx, path, 0, opts...)
	if err != nil {
		t.Fatalf("NewSortedBuilder(0): %v", err)
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("sorted Finish: %v", err)
	}
	return path
}

// TestEmptyIndexWithWorkers checks that a zero-key build requested with
// WithWorkers still succeeds: newBuilder forces single-threaded for an empty
// build (nothing to parallelize), so it must not reach the parallel finish
// paths, which assume real block work.
func TestEmptyIndexWithWorkers(t *testing.T) {
	for _, unsorted := range []bool{false, true} {
		name := "sorted"
		if unsorted {
			name = "unsorted"
		}
		t.Run(name, func(t *testing.T) {
			path := buildEmptyIndex(t, unsorted, WithWorkers(4))
			idx, err := Open(path)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			if got := idx.NumKeys(); got != 0 {
				t.Errorf("NumKeys() = %d, want 0", got)
			}
			key := make([]byte, MinKeySize)
			if _, err := idx.QueryRank(key); !errors.Is(err, sherr.ErrNotFound) {
				t.Errorf("QueryRank = %v, want ErrNotFound", err)
			}
		})
	}
}

// TestEmptyIndex pins the zero-key contract across both algorithms, both
// builder types, and with/without payload+fingerprint: the build succeeds, the
// index opens and passes integrity Verify, reports zero keys, and every
// QueryRank resolves to ErrNotFound (never a spurious hit or a decode error).
func TestEmptyIndex(t *testing.T) {
	algos := []struct {
		name string
		opt  BuildOption
	}{
		{"bijection", WithAlgorithm(AlgoBijection)},
		{"ptrhash", WithAlgorithm(AlgoPTRHash)},
	}
	builders := []struct {
		name     string
		unsorted bool
	}{
		{"sorted", false},
		{"unsorted", true},
	}
	// mphf-only vs the txhash-style payload+fingerprint layout, which gives the
	// header a non-zero PayloadSize/FingerprintSize even though the payload
	// region is empty.
	optionSets := []struct {
		name       string
		opts       []BuildOption
		hasPayload bool
	}{
		{"mphf-only", nil, false},
		{"payload+fingerprint", []BuildOption{WithPayload(8), WithFingerprint(4)}, true},
	}

	for _, a := range algos {
		for _, bt := range builders {
			for _, optSet := range optionSets {
				t.Run(a.name+"/"+bt.name+"/"+optSet.name, func(t *testing.T) {
					opts := append([]BuildOption{a.opt}, optSet.opts...)
					path := buildEmptyIndex(t, bt.unsorted, opts...)

					idx, err := Open(path)
					if err != nil {
						t.Fatalf("Open empty index: %v", err)
					}
					defer idx.Close()

					if got := idx.NumKeys(); got != 0 {
						t.Errorf("NumKeys() = %d, want 0", got)
					}
					if err := idx.Verify(); err != nil {
						t.Errorf("Verify() on empty index: %v", err)
					}

					// Every key must miss cleanly.
					for i := range 200 {
						var key [16]byte
						binary.LittleEndian.PutUint64(key[0:], uint64(i)*0x9e3779b97f4a7c15)
						binary.LittleEndian.PutUint64(key[8:], uint64(i)^0xdeadbeef)
						if _, qerr := idx.QueryRank(key[:]); !errors.Is(qerr, sherr.ErrNotFound) {
							t.Fatalf("QueryRank(key %d) = %v, want ErrNotFound", i, qerr)
						}
					}

					// The payload read path (used by the txhash cold store) must
					// also miss cleanly rather than read the zero-size region.
					if optSet.hasPayload {
						pi, perr := OpenPayload(path)
						if perr != nil {
							t.Fatalf("OpenPayload empty index: %v", perr)
						}
						var key [16]byte
						binary.LittleEndian.PutUint64(key[0:], 0x1234)
						if _, _, qerr := pi.QueryPayload(key[:]); !errors.Is(qerr, sherr.ErrNotFound) {
							t.Errorf("QueryPayload on empty index = %v, want ErrNotFound", qerr)
						}
						_ = pi.Close()
					}

					// The in-memory open path must accept it too.
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					bIdx, err := OpenBytes(data)
					if err != nil {
						t.Fatalf("OpenBytes empty index: %v", err)
					}
					if got := bIdx.NumKeys(); got != 0 {
						t.Errorf("OpenBytes NumKeys() = %d, want 0", got)
					}
					if err := bIdx.Close(); err != nil {
						t.Errorf("Close: %v", err)
					}
				})
			}
		}
	}
}
