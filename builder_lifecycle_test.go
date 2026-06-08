package streamhash

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stellar/streamhash/internal/sherr"
)

// ============================================================================
// Build Mode Equivalence
// ============================================================================

func TestBuildModeEquivalence(t *testing.T) {
	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	configs := []struct {
		name    string
		payload int
		fp      int
		keySize int
	}{
		{"p4_fp0", 4, 0, 16},
		{"p0_fp0", 0, 0, 16},
		{"p4_fp2", 4, 2, 20},
		{"p4_fp1", 4, 1, 20}, // fp=1 is the default, previously untested
		{"p4_fp3", 4, 3, 20}, // fp=3 is an untested size
		{"p1_fp0", 1, 0, 16}, // minimal payload
		{"p8_fp4", 8, 4, 24}, // maximal payload + fingerprint
		{"p0_fp2", 0, 2, 20}, // fingerprint-only, no payload
	}

	numKeys := 5000

	for _, algo := range algorithms {
		for _, cfg := range configs {
			t.Run(fmt.Sprintf("%s/%s", algo.name, cfg.name), func(t *testing.T) {
				tmpDir := t.TempDir()
				ctx := context.Background()

				// Generate deterministic keys
				rng := newTestRNG(t)
				keys := generateRandomKeys(rng, numKeys, cfg.keySize)

				// Build options
				var opts []BuildOption
				opts = append(opts, WithAlgorithm(algo.algo))
				if cfg.payload > 0 {
					opts = append(opts, WithPayload(cfg.payload))
				}
				if cfg.fp > 0 {
					opts = append(opts, WithFingerprint(cfg.fp))
				}

				// Get numBlocks for sorting
				numBlocks, err := numBlocksForAlgo(algo.algo, uint64(numKeys), cfg.payload, cfg.fp)
				if err != nil {
					t.Fatal(err)
				}

				// Sort keys by block index
				sortedKeys := make([][]byte, len(keys))
				copy(sortedKeys, keys)
				slices.SortFunc(sortedKeys, func(a, b []byte) int {
					return cmp.Compare(blockIndexFromPrefix(extractPrefix(a), numBlocks),
						blockIndexFromPrefix(extractPrefix(b), numBlocks))
				})

				// Compute payload mask to ensure values fit in configured size.
				var payloadMask uint64
				if cfg.payload >= 8 {
					payloadMask = ^uint64(0)
				} else if cfg.payload > 0 {
					payloadMask = (1 << (cfg.payload * 8)) - 1
				}

				keyToPayload := make(map[string]uint64)
				for i, key := range keys {
					keyToPayload[string(key)] = uint64(i) & payloadMask
				}

				// Build sorted
				sortedPath := filepath.Join(tmpDir, "sorted.idx")
				sortedOpts := append([]BuildOption{WithWorkers(1)}, opts...)
				builderS, err := NewSortedBuilder(ctx, sortedPath, uint64(numKeys), sortedOpts...)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range sortedKeys {
					if err := builderS.AddKey(key, keyToPayload[string(key)]); err != nil {
						builderS.Close()
						t.Fatal(err)
					}
				}
				if err := builderS.Finish(); err != nil {
					t.Fatal(err)
				}

				// Build parallel
				parallelPath := filepath.Join(tmpDir, "parallel.idx")
				parallelOpts := append([]BuildOption{WithWorkers(4)}, opts...)
				builderP, err := NewSortedBuilder(ctx, parallelPath, uint64(numKeys), parallelOpts...)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range sortedKeys {
					if err := builderP.AddKey(key, keyToPayload[string(key)]); err != nil {
						builderP.Close()
						t.Fatal(err)
					}
				}
				if err := builderP.Finish(); err != nil {
					t.Fatal(err)
				}

				// Build unsorted (single-threaded)
				unsortedPath := filepath.Join(tmpDir, "unsorted.idx")
				unsortedOpts := append([]BuildOption{WithWorkers(1)}, opts...)
				builderU, err := NewUnsortedBuilder(ctx, unsortedPath, uint64(numKeys), tmpDir, unsortedOpts...)
				if err != nil {
					t.Fatal(err)
				}
				for i, key := range keys {
					if err := builderU.AddKey(key, uint64(i)&payloadMask); err != nil {
						builderU.Close()
						t.Fatal(err)
					}
				}
				if err := builderU.Finish(); err != nil {
					t.Fatal(err)
				}

				// Build unsorted (parallel)
				unsortedParPath := filepath.Join(tmpDir, "unsorted_par.idx")
				unsortedParOpts := append([]BuildOption{WithWorkers(4)}, opts...)
				builderUP, err := NewUnsortedBuilder(ctx, unsortedParPath, uint64(numKeys), tmpDir, unsortedParOpts...)
				if err != nil {
					t.Fatal(err)
				}
				for i, key := range keys {
					if err := builderUP.AddKey(key, uint64(i)&payloadMask); err != nil {
						builderUP.Close()
						t.Fatal(err)
					}
				}
				if err := builderUP.Finish(); err != nil {
					t.Fatal(err)
				}

				// Compare files
				sortedData, err := os.ReadFile(sortedPath)
				if err != nil {
					t.Fatalf("ReadFile sorted: %v", err)
				}
				parallelData, err := os.ReadFile(parallelPath)
				if err != nil {
					t.Fatalf("ReadFile parallel: %v", err)
				}
				unsortedData, err := os.ReadFile(unsortedPath)
				if err != nil {
					t.Fatalf("ReadFile unsorted: %v", err)
				}
				unsortedParData, err := os.ReadFile(unsortedParPath)
				if err != nil {
					t.Fatalf("ReadFile unsorted_par: %v", err)
				}

				// Both algorithms are deterministic, so every build mode must
				// produce byte-identical output. (PTRHash's Phase-2 eviction RNG
				// is now seeded from the global seed rather than process-global
				// state, so its Cuckoo solver is reproducible across modes too.)
				if !bytes.Equal(sortedData, parallelData) {
					t.Errorf("%s: sorted and parallel outputs differ", algo.name)
				}
				if !bytes.Equal(sortedData, unsortedData) {
					t.Errorf("%s: sorted and unsorted outputs differ", algo.name)
				}
				if !bytes.Equal(sortedData, unsortedParData) {
					t.Errorf("%s: sorted and unsorted_parallel outputs differ", algo.name)
				}

				// Independently confirm every mode is a valid MPHF with correct payloads.
				for _, tc := range []struct {
					name string
					path string
				}{
					{"sorted", sortedPath},
					{"parallel", parallelPath},
					{"unsorted", unsortedPath},
					{"unsorted_parallel", unsortedParPath},
				} {
					idx, err := Open(tc.path)
					if err != nil {
						t.Fatalf("Open %s: %v", tc.name, err)
					}
					ranks := make(map[uint64]bool)
					for _, key := range keys {
						rank, err := idx.QueryRank(key)
						if err != nil {
							t.Fatalf("Query %s: %v", tc.name, err)
						}
						ranks[rank] = true
					}
					if len(ranks) != numKeys {
						t.Errorf("%s: expected %d unique ranks, got %d", tc.name, numKeys, len(ranks))
					}
					// Verify payloads round-trip correctly
					if cfg.payload > 0 {
						pi, piErr := idx.WithPayload()
						if piErr != nil {
							t.Fatalf("WithPayload %s: %v", tc.name, piErr)
						}
						for ki, key := range keys {
							_, got, err := pi.QueryPayload(key)
							if err != nil {
								t.Fatalf("QueryPayload %s key %d: %v", tc.name, ki, err)
							}
							want := keyToPayload[string(key)]
							if got != want {
								t.Fatalf("%s key %d: payload mismatch: got %d, want %d", tc.name, ki, got, want)
							}
						}
					}
					idx.Close()
				}
			})
		}
	}
}

// ============================================================================
// Builder Options and Lifecycle
// ============================================================================

func TestGlobalSeedAffectsIndex(t *testing.T) {
	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	for _, algoCase := range algorithms {
		for _, unsorted := range []bool{false, true} {
			modeName := "sorted"
			if unsorted {
				modeName = "unsorted"
			}
			t.Run(fmt.Sprintf("%s/%s", algoCase.name, modeName), func(t *testing.T) {
				tmpDir := t.TempDir()
				numKeys := 1000
				rng := newTestRNG(t)
				keys := generateRandomKeys(rng, numKeys, 24)

				var opts1, opts2 []BuildOption
				opts1 = append(opts1, WithAlgorithm(algoCase.algo), WithGlobalSeed(0x1111111111111111))
				opts2 = append(opts2, WithAlgorithm(algoCase.algo), WithGlobalSeed(0x2222222222222222))

				ctx := context.Background()
				path1 := filepath.Join(tmpDir, "seed1.idx")
				path2 := filepath.Join(tmpDir, "seed2.idx")

				if err := buildFromSlice(ctx, path1, entriesToSlice(keys), opts1...); err != nil {
					t.Fatalf("Build 1: %v", err)
				}
				if err := buildFromSlice(ctx, path2, entriesToSlice(keys), opts2...); err != nil {
					t.Fatalf("Build 2: %v", err)
				}

				idx1, err := Open(path1)
				if err != nil {
					t.Fatal(err)
				}
				defer idx1.Close()

				idx2, err := Open(path2)
				if err != nil {
					t.Fatal(err)
				}
				defer idx2.Close()

				differentCount := 0
				queryErrors := 0
				for _, key := range keys {
					r1, e1 := idx1.QueryRank(key)
					r2, e2 := idx2.QueryRank(key)
					if e1 != nil || e2 != nil {
						queryErrors++
						continue
					}
					if r1 != r2 {
						differentCount++
					}
				}
				if queryErrors > len(keys)/10 {
					t.Fatalf("%d/%d queries returned errors", queryErrors, len(keys))
				}
				// At least some keys should map differently with different seeds
				if differentCount == 0 {
					t.Error("Expected different seeds to produce different rank mappings")
				}
			})
		}
	}
}

func TestUserMetadataRoundtrip(t *testing.T) {
	metadata := []byte("testdata-with-variable-length-content")

	numKeys := 100
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = make([]byte, 20)
		binary.BigEndian.PutUint64(keys[i][0:8], uint64(i)*0xABCDEF0123456789)
		binary.BigEndian.PutUint64(keys[i][8:16], uint64(i))
		binary.BigEndian.PutUint32(keys[i][16:20], uint32(i))
	}
	slices.SortFunc(keys, bytes.Compare)

	idx := buildAndOpen(t, keys, nil, WithMetadata(metadata))
	defer idx.Close()

	storedMetadata := idx.UserMetadata()
	if !bytes.Equal(storedMetadata, metadata) {
		t.Errorf("Metadata mismatch: got %v, want %v", storedMetadata, metadata)
	}
}

// ============================================================================
// Parallel Builder Lifecycle
// ============================================================================

func TestParallelBuilderContextTimeoutPrecision(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "timeout.idx")

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	numKeys := 100000
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 16)
	sortKeysByBlock(keys, uint64(numKeys), nil)

	builder, err := NewSortedBuilder(ctx, indexPath, uint64(numKeys), WithWorkers(4))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}

	var addErr error
	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			addErr = err
			break
		}
	}

	if addErr == nil {
		addErr = builder.Finish()
	}
	builder.Close()

	// The timeout should have fired — either AddKey or Finish should have failed
	if addErr == nil {
		t.Fatal("expected context.DeadlineExceeded but build completed before timeout")
	}
	if !errors.Is(addErr, context.DeadlineExceeded) {
		t.Errorf("expected context.DeadlineExceeded, got: %v", addErr)
	}
}

func TestParallelBuilderCloseUnderLoad(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "close_load.idx")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	numKeys := 5000
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 16)
	sortKeysByBlock(keys, uint64(numKeys), nil)

	builder, err := NewSortedBuilder(ctx, indexPath, uint64(numKeys), WithWorkers(4))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}

	// Add partial keys
	for i := 0; i < numKeys/2; i++ {
		if err := builder.AddKey(keys[i], 0); err != nil {
			break
		}
	}

	cancel()
	if err := builder.Close(); err != nil {
		t.Errorf("Close after cancel: %v", err)
	}

	// Incomplete index should not be openable
	if _, err := Open(indexPath); err == nil {
		t.Error("Expected Open to fail on incomplete index after cancel+close")
	}
}

func TestParallelBuilderClose(t *testing.T) {
	tmpDir := t.TempDir()
	output := filepath.Join(tmpDir, "parallel_close.idx")

	numKeys := 1000
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 16)
	sortKeysByBlock(keys, uint64(numKeys), nil)

	builder, err := NewSortedBuilder(context.Background(), output, uint64(numKeys), WithWorkers(4))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}

	for i := range 500 {
		if err := builder.AddKey(keys[i], 0); err != nil {
			builder.Close()
			t.Fatalf("AddKey failed: %v", err)
		}
	}

	// Close without Finish — should not error or panic
	if err := builder.Close(); err != nil {
		t.Errorf("Close without Finish: %v", err)
	}
	// Output file should not be a valid index
	if _, err := Open(output); err == nil {
		t.Error("Expected Open to fail on incomplete index")
	}
}

func TestParallelBuilderStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	for _, alg := range algorithms {
		t.Run(alg.name, func(t *testing.T) {
			rng := newTestRNG(t)
			for cycle := range 10 {
				tmpDir := t.TempDir()
				indexPath := filepath.Join(tmpDir, "stress.idx")

				numKeys := 5000
				keys := generateRandomKeys(rng, numKeys, 16)
				sortKeysByBlock(keys, uint64(numKeys), []BuildOption{WithAlgorithm(alg.algo)})

				builder, err := NewSortedBuilder(context.Background(), indexPath, uint64(numKeys),
					WithWorkers(4), WithAlgorithm(alg.algo))
				if err != nil {
					t.Fatalf("Cycle %d: newBuilder failed: %v", cycle, err)
				}
				for _, key := range keys {
					if err := builder.AddKey(key, 0); err != nil {
						builder.Close()
						t.Fatalf("Cycle %d: AddKey failed: %v", cycle, err)
					}
				}
				if err := builder.Finish(); err != nil {
					t.Fatalf("Cycle %d: Finish failed: %v", cycle, err)
				}

				idx, err := Open(indexPath)
				if err != nil {
					t.Fatalf("Cycle %d: Open failed: %v", cycle, err)
				}
				verifyMPHF(t, idx, keys)
				if err := idx.Verify(); err != nil {
					idx.Close()
					t.Fatalf("Cycle %d: Verify failed: %v", cycle, err)
				}
				idx.Close()
			}
		})
	}
}

func TestParallelBuilderConcurrentBuilds(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrent builds test in short mode")
	}

	numBuilders := 4
	rng := newTestRNG(t)
	var wg sync.WaitGroup
	errCh := make(chan error, numBuilders)

	for b := range numBuilders {
		// Generate distinct keys per goroutine from the shared RNG (sequential, before goroutine launch).
		numKeys := 2000
		keys := generateRandomKeys(rng, numKeys, 16)
		wg.Add(1)
		go func(builderID int, keys [][]byte) {
			defer wg.Done()
			tmpDir := t.TempDir()
			indexPath := filepath.Join(tmpDir, fmt.Sprintf("concurrent_%d.idx", builderID))

			numKeys := len(keys)
			sortKeysByBlock(keys, uint64(numKeys), nil)

			builder, err := NewSortedBuilder(context.Background(), indexPath, uint64(numKeys), WithWorkers(2))
			if err != nil {
				errCh <- fmt.Errorf("builder %d: newBuilder: %w", builderID, err)
				return
			}
			for _, key := range keys {
				if err := builder.AddKey(key, 0); err != nil {
					builder.Close()
					errCh <- fmt.Errorf("builder %d: AddKey: %w", builderID, err)
					return
				}
			}
			if err := builder.Finish(); err != nil {
				errCh <- fmt.Errorf("builder %d: Finish: %w", builderID, err)
				return
			}

			idx, err := Open(indexPath)
			if err != nil {
				errCh <- fmt.Errorf("builder %d: Open: %w", builderID, err)
				return
			}
			if err := idx.Verify(); err != nil {
				idx.Close()
				errCh <- fmt.Errorf("builder %d: Verify: %w", builderID, err)
				return
			}
			idx.Close()
		}(b, keys)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// ============================================================================
// Builder Close and Error Handling
// ============================================================================

func TestBuilderCloseIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "idempotent.idx")

	numKeys := 100
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 16)
	sortKeysByBlock(keys, uint64(numKeys), nil)

	builder, err := NewSortedBuilder(context.Background(), indexPath, uint64(numKeys))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			builder.Close()
			t.Fatal(err)
		}
	}
	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}

	// Close 3 times, all should succeed
	for i := range 3 {
		if err := builder.Close(); err != nil {
			t.Errorf("Close() #%d failed: %v", i+1, err)
		}
	}

	// Verify the index is still valid after multiple closes
	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open after 3x Close: %v", err)
	}
	defer idx.Close()
	verifyMPHF(t, idx, keys)
}

func TestUnsortedReplayCloseAfterFailedFinish(t *testing.T) {
	tmpDir := t.TempDir()
	output := filepath.Join(tmpDir, "fail.idx")

	numKeys := 50000
	ctx, cancel := context.WithCancel(context.Background())

	builder, err := NewUnsortedBuilder(ctx, output, uint64(numKeys),
		tmpDir, WithPayload(4))
	if err != nil {
		t.Fatal(err)
	}

	rng := newTestRNG(t)
	for i := range numKeys {
		key := make([]byte, 16)
		fillFromRNG(rng, key)
		if err := builder.AddKey(key, uint64(i)); err != nil {
			builder.Close()
			t.Fatalf("AddKey %d: %v", i, err)
		}
	}

	cancel()
	err = builder.Finish()
	if err == nil {
		t.Fatal("expected Finish to fail after cancel")
	}
	// Context.Canceled propagation is verified by TestContextCancellation/UnsortedReplay;
	// this test focuses on Close + Open behavior after a failed Finish.

	// Close should succeed after failed Finish
	if err := builder.Close(); err != nil {
		t.Errorf("Close after failed Finish: %v", err)
	}

	// Output should not be a valid index
	if _, err := Open(output); err == nil {
		t.Error("Expected Open to fail on index from cancelled build")
	}
}

func TestBuilderRejectsOutOfOrderKeys(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "out_of_order.idx")

	numKeys := 500
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 24)

	// Sort correctly first
	numBlocks, err := numBlocksForAlgo(AlgoBijection, uint64(numKeys), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(keys, func(a, b []byte) int {
		return cmp.Compare(blockIndexFromPrefix(extractPrefix(a), numBlocks),
			blockIndexFromPrefix(extractPrefix(b), numBlocks))
	})

	// Swap two keys from different blocks to create out-of-order
	// Find two keys in different blocks
	swapped := false
	for i := 0; i < len(keys)-1; i++ {
		pi := extractPrefix(keys[i])
		pj := extractPrefix(keys[i+1])
		bi := blockIndexFromPrefix(pi, numBlocks)
		bj := blockIndexFromPrefix(pj, numBlocks)
		if bi < bj {
			// keys[i] is in an earlier block than keys[i+1] - swap them
			keys[i], keys[i+1] = keys[i+1], keys[i]
			swapped = true
			break
		}
	}
	if !swapped {
		t.Fatal("test setup bug: all keys in same block despite numBlocks >= 2")
	}

	builder, err := NewSortedBuilder(context.Background(), indexPath, uint64(numKeys))
	if err != nil {
		t.Fatal(err)
	}

	var addErr error
	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			addErr = err
			break
		}
	}
	builder.Close()

	if addErr == nil {
		t.Error("expected error for out-of-order keys")
	} else if !errors.Is(addErr, sherr.ErrUnsortedInput) {
		t.Errorf("expected ErrUnsortedInput, got: %v", addErr)
	}
}
