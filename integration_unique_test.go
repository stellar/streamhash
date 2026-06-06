// integration_unique_test.go tests end-to-end behaviors that don't fit the
// parameterized integration matrix: small-N edge cases (MPHF and payload+fp
// variants), concurrency safety, key-length boundaries, fingerprint
// false-positive rates, algorithm-specific correctness (degenerate splits,
// indistinguishable hashes), and statistics.
package streamhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	randv2 "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// ============================================================================
// Small Index Edge Cases
// ============================================================================

func TestSmallIndexEdgeCases(t *testing.T) {
	testCases := []int{1, 2, 3, 7, 8, 9}

	makeKeys := func(rng *randv2.Rand, n int) [][]byte {
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = make([]byte, 24)
			fillFromRNG(rng, keys[i])
		}
		return keys
	}

	t.Run("MPHF", func(t *testing.T) {
		for _, numKeys := range testCases {
			t.Run(fmt.Sprintf("N=%d", numKeys), func(t *testing.T) {
				rng := newTestRNG(t)
				keys := makeKeys(rng, numKeys)
				slices.SortFunc(keys, bytes.Compare)

				idx := buildAndOpen(t, keys, nil)
				defer idx.Close()
				verifyMPHF(t, idx, keys)
			})
		}
	})

	t.Run("PayloadAndFingerprint", func(t *testing.T) {
		for _, numKeys := range testCases {
			t.Run(fmt.Sprintf("N=%d", numKeys), func(t *testing.T) {
				rng := newTestRNG(t)
				keys := makeKeys(rng, numKeys)
				payloads := make([]uint64, numKeys)
				for i := range payloads {
					payloads[i] = uint64(i)
				}
				sortKeysAndPayloads(keys, payloads)

				idx := buildAndOpen(t, keys, payloads,
					WithPayload(4), WithFingerprint(2))
				defer idx.Close()

				verifyMPHF(t, idx, keys)
				verifyPayloads(t, idx, keys, payloads, 4)
				verifyNonMemberRejection(t, rng, idx, 100)
			})
		}
	})
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestConcurrentQueries(t *testing.T) {
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, 1000, 24)
	slices.SortFunc(keys, bytes.Compare)
	idx := buildAndOpen(t, keys, nil)
	defer idx.Close()

	numWorkers := 10
	queriesPerWorker := 100
	ranks := make([]uint64, len(keys))
	var wg sync.WaitGroup
	errCh := make(chan error, numWorkers*queriesPerWorker)

	for w := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := range queriesPerWorker {
				keyIdx := (workerID*queriesPerWorker + i) % len(keys)
				rank, err := idx.QueryRank(keys[keyIdx])
				if err != nil {
					errCh <- err
				} else {
					ranks[keyIdx] = rank // non-overlapping indices per worker
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)
	queryErrors := 0
	for err := range errCh {
		queryErrors++
		t.Errorf("Concurrent query error: %v", err)
	}

	// Verify MPHF bijectivity: all ranks unique and in [0, N).
	// Skip when queries failed — failed entries leave ranks at zero-value,
	// which is indistinguishable from legitimate rank 0.
	if queryErrors == 0 {
		seen := make(map[uint64]bool, len(keys))
		for i, r := range ranks {
			if r >= uint64(len(keys)) {
				t.Errorf("key %d: rank %d out of range [0, %d)", i, r, len(keys))
			}
			if seen[r] {
				t.Errorf("key %d: duplicate rank %d", i, r)
			}
			seen[r] = true
		}
	}
}

func TestConcurrentQueryAfterParallelBuild(t *testing.T) {
	numKeys := 2000
	keys := make([][]byte, numKeys)
	for i := range keys {
		src := make([]byte, 20)
		binary.BigEndian.PutUint64(src[0:8], uint64(i))
		binary.BigEndian.PutUint64(src[8:16], uint64(i*7919))
		for j := 16; j < 20; j++ {
			src[j] = byte(i + j)
		}
		keys[i] = PreHash(src)
	}
	sortKeysByBlock(keys, uint64(numKeys), nil)

	idx := buildAndOpen(t, keys, nil, WithWorkers(8))
	defer idx.Close()

	ranks := make([]uint64, numKeys)
	var wg sync.WaitGroup
	errCh := make(chan error, numKeys)

	for i, key := range keys {
		wg.Add(1)
		go func(ki int, k []byte) {
			defer wg.Done()
			rank, err := idx.QueryRank(k)
			if err != nil {
				errCh <- err
			} else {
				ranks[ki] = rank
			}
		}(i, key)
	}

	wg.Wait()
	close(errCh)
	errCount := 0
	for err := range errCh {
		errCount++
		if errCount <= 5 {
			t.Errorf("Concurrent query error: %v", err)
		}
	}
	if errCount > 5 {
		t.Errorf("Total concurrent query errors: %d", errCount)
	}

	// Verify MPHF bijectivity: all ranks unique and in [0, N).
	// Skip when queries failed — failed entries leave ranks at zero-value,
	// which is indistinguishable from legitimate rank 0.
	if errCount == 0 {
		seen := make(map[uint64]bool, numKeys)
		for i, r := range ranks {
			if r >= uint64(numKeys) {
				t.Errorf("key %d: rank %d out of range [0, %d)", i, r, numKeys)
			}
			if seen[r] {
				t.Errorf("key %d: duplicate rank %d", i, r)
			}
			seen[r] = true
		}
	}
}

// ============================================================================
// Correctness Tests
// ============================================================================

func TestNonDuplicate16ByteKeys(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "collision.idx")

	// Two keys that share the same first 15 bytes but differ in byte 15.
	// These are NOT duplicates — they should build successfully.
	keys := make([][]byte, 2)
	keys[0] = make([]byte, 16)
	keys[1] = make([]byte, 16)
	for i := range 15 {
		keys[0][i] = byte(i + 1)
		keys[1][i] = byte(i + 1)
	}
	keys[0][15] = 0x00
	keys[1][15] = 0x01
	slices.SortFunc(keys, bytes.Compare)
	ctx := context.Background()
	keyIter := func(yield func([]byte, []byte) bool) {
		for _, k := range keys {
			if !yield(k, nil) {
				return
			}
		}
	}
	err := buildSorted(ctx, indexPath, uint64(len(keys)), keyIter)
	if err != nil {
		t.Fatalf("Build failed for non-duplicate 16-byte keys: %v", err)
	}

	// Verify the MPHF works
	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()
	verifyMPHF(t, idx, keys)
}

// ============================================================================
// Statistics and Bounds Tests
// ============================================================================

func TestStats(t *testing.T) {
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, 1000, 24)
	slices.SortFunc(keys, bytes.Compare)

	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "stats.idx")
	if err := quickBuildNoPreHash(context.Background(), indexPath, keys); err != nil {
		t.Fatal(err)
	}

	stats, err := GetStats(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if stats.NumKeys != 1000 {
		t.Errorf("NumKeys: expected 1000, got %d", stats.NumKeys)
	}
	if stats.BitsPerKey < 1 || stats.BitsPerKey > 100 {
		t.Errorf("BitsPerKey out of range: %f", stats.BitsPerKey)
	}
}

func TestBitsPerKeyBounds(t *testing.T) {
	t.Run("ScaleProgression", func(t *testing.T) {
		sizes := []int{1000, 10000, 100000}
		for _, n := range sizes {
			t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
				rng := newTestRNG(t)
				tmpDir := t.TempDir()
				indexPath := filepath.Join(tmpDir, "test.idx")

				keys := generateRandomKeys(rng, n, 24)
				entries := make([]entry, len(keys))
				for i, k := range keys {
					entries[i] = entry{Key: k}
				}
				if err := buildFromEntries(context.Background(), indexPath, entries); err != nil {
					t.Fatal(err)
				}

				stat, err := os.Stat(indexPath)
				if err != nil {
					t.Fatal(err)
				}
				bitsPerKey := float64(stat.Size()*8) / float64(n)
				t.Logf("N=%d: bits/key=%.2f", n, bitsPerKey)
				if bitsPerKey > 10.0 {
					t.Errorf("bits/key %.2f exceeds threshold", bitsPerKey)
				}
			})
		}
	})

	t.Run("PayloadFingerprintCombos", func(t *testing.T) {
		configs := []struct {
			name        string
			payloadSize int
			fpSize      int
			expectedMin float64
			expectedMax float64
		}{
			{"MPHF_only", 0, 0, 2.0, 6.0},
			{"fp1", 0, 1, 9.0, 13.0},
			{"fp2", 0, 2, 17.0, 21.0},
			{"fp4", 0, 4, 33.0, 37.0},
			{"p3_fp2", 3, 2, 41.0, 45.0},
			{"p8", 8, 0, 65.0, 69.0},
			{"p8_fp2", 8, 2, 81.0, 85.0},
			{"p8_fp4", 8, 4, 97.0, 101.0},
		}

		n := 100000

		for _, cfg := range configs {
			t.Run(cfg.name, func(t *testing.T) {
				rng := newTestRNG(t)
				tmpDir := t.TempDir()
				indexPath := filepath.Join(tmpDir, "test.idx")

				keys := generateRandomKeys(rng, n, 24)
				entries := make([]entry, len(keys))
				for i, k := range keys {
					entries[i] = entry{Key: k}
					if cfg.payloadSize > 0 {
						entries[i].Payload = uint64(i)
					}
				}

				var opts []BuildOption
				if cfg.payloadSize > 0 {
					opts = append(opts, WithPayload(cfg.payloadSize))
				}
				if cfg.fpSize > 0 {
					opts = append(opts, WithFingerprint(cfg.fpSize))
				}

				if err := buildFromEntries(context.Background(), indexPath, entries, opts...); err != nil {
					t.Fatal(err)
				}

				stat, err := os.Stat(indexPath)
				if err != nil {
					t.Fatal(err)
				}
				bitsPerKey := float64(stat.Size()*8) / float64(n)
				t.Logf("bits/key=%.2f (expected: %.1f-%.1f)", bitsPerKey, cfg.expectedMin, cfg.expectedMax)

				if bitsPerKey < cfg.expectedMin {
					t.Errorf("bits/key %.2f below minimum %.1f", bitsPerKey, cfg.expectedMin)
				}
				if bitsPerKey > cfg.expectedMax {
					t.Errorf("bits/key %.2f above maximum %.1f", bitsPerKey, cfg.expectedMax)
				}
			})
		}
	})
}

// ============================================================================
// Fingerprint Tests
// ============================================================================

func TestFingerprintFalsePositiveRate(t *testing.T) {
	fpConfigs := []struct {
		name           string
		fpSize         int
		expectedFPR    float64
		tolerance      float64
		nonMemberTests int
	}{
		{"fp1", 1, 1.0 / 256, 3.0, 100000},
		{"fp2", 2, 1.0 / 65536, 3.0, 1000000},
		{"fp3", 3, 1.0 / (1 << 24), 3.0, 1000000},
		{"fp4", 4, 1.0 / (1 << 32), 10.0, 100000},
	}

	algos := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	keySizes := []int{16, 17, 24}

	n := 10000

	for _, algo := range algos {
		for _, keySize := range keySizes {
			for _, cfg := range fpConfigs {
				name := fmt.Sprintf("%s/%dB/%s", algo.name, keySize, cfg.name)
				t.Run(name, func(t *testing.T) {
					rng := newTestRNG(t)
					keys := generateRandomKeys(rng, n, keySize)
					entries := make([]entry, len(keys))
					for i, k := range keys {
						entries[i] = entry{Key: k}
					}

					tmpDir := t.TempDir()
					indexPath := filepath.Join(tmpDir, "test.idx")
					if err := buildFromEntries(context.Background(), indexPath, entries,
						WithFingerprint(cfg.fpSize), WithAlgorithm(algo.algo)); err != nil {
						t.Fatal(err)
					}

					idx, err := Open(indexPath)
					if err != nil {
						t.Fatal(err)
					}
					defer idx.Close()

					// Verify members
					memberErrors := 0
					for i := range 100 {
						keyIdx := i * (n / 100)
						if _, err := idx.QueryRank(keys[keyIdx]); err != nil {
							memberErrors++
						}
					}
					if memberErrors > 0 {
						t.Errorf("%d member keys returned errors", memberErrors)
					}

					// Test non-members with same key size
					falsePositives := 0
					for i := range cfg.nonMemberTests {
						spread1 := uint64(i+n+1000000) * 0x8E3779B97F4A7C15
						spread2 := uint64(i+n+2000000) * 0x9E3779B97F4A7C17
						key := make([]byte, keySize)
						binary.BigEndian.PutUint64(key[0:8], spread1)
						binary.BigEndian.PutUint64(key[8:16], spread2)
						if keySize > 16 {
							spread3 := uint64(i+n+3000000) * 0xAE3779B97F4A7C19
							for j := 16; j < keySize && j < 24; j++ {
								key[j] = byte(spread3 >> ((j - 16) * 8))
							}
						}

						if _, err := idx.QueryRank(key); err == nil {
							falsePositives++
						}
					}

					observedFPR := float64(falsePositives) / float64(cfg.nonMemberTests)
					expectedFPs := cfg.expectedFPR * float64(cfg.nonMemberTests)
					t.Logf("FPR=%.6f%% (expected ~%.6f%%), FPs=%d", observedFPR*100, cfg.expectedFPR*100, falsePositives)

					if cfg.fpSize <= 2 {
						maxExpectedFPs := int(expectedFPs * cfg.tolerance)
						if falsePositives > maxExpectedFPs {
							t.Errorf("Too many FPs: %d (max %d)", falsePositives, maxExpectedFPs)
						}
					}
					if cfg.fpSize >= 3 && falsePositives > 2 {
						t.Errorf("Expected <=2 FPs for %d-byte fp, got %d", cfg.fpSize, falsePositives)
					}
				})
			}
		}
	}
}

// ============================================================================
// Key Length and Boundary Tests
// ============================================================================

func TestKeyLengthBoundary(t *testing.T) {
	boundaries := []int{65534, 65535}

	makeKeys := func(t testing.TB, keyLen, numKeys int) [][]byte {
		rng := newTestRNG(t)
		keys := make([][]byte, numKeys)
		for i := range keys {
			keys[i] = make([]byte, keyLen)
			fillFromRNG(rng, keys[i])
		}
		return keys
	}

	for _, keyLen := range boundaries {
		t.Run(fmt.Sprintf("len=%d/MPHF", keyLen), func(t *testing.T) {
			keys := makeKeys(t, keyLen, 10)
			slices.SortFunc(keys, bytes.Compare)
			idx := buildAndOpen(t, keys, nil)
			defer idx.Close()
			verifyMPHF(t, idx, keys)
		})

		t.Run(fmt.Sprintf("len=%d/PayloadAndFP", keyLen), func(t *testing.T) {
			keys := makeKeys(t, keyLen, 10)
			payloads := make([]uint64, len(keys))
			for i := range payloads {
				payloads[i] = uint64(i * 1000)
			}
			opts := []BuildOption{WithPayload(4), WithFingerprint(2)}
			sortKeysAndPayloads(keys, payloads)
			idx := buildAndOpen(t, keys, payloads, opts...)
			defer idx.Close()
			verifyMPHF(t, idx, keys)
			verifyPayloads(t, idx, keys, payloads, 4)
		})
	}
}

func TestRapidOpenClose(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping rapid open/close in short mode")
	}

	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, 100, 24)
	slices.SortFunc(keys, bytes.Compare)

	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "rapid.idx")
	if err := quickBuildNoPreHash(context.Background(), indexPath, keys); err != nil {
		t.Fatal(err)
	}

	for i := range 100 {
		idx, err := Open(indexPath)
		if err != nil {
			t.Fatalf("Iteration %d: Open failed: %v", i, err)
		}
		if _, err := idx.QueryRank(keys[0]); err != nil {
			idx.Close()
			t.Fatalf("Iteration %d: Query failed: %v", i, err)
		}
		idx.Close()
	}
}

func TestFingerprintWithVeryShortKeys(t *testing.T) {
	// Test 16-byte keys (minimum size) with fingerprint
	// For 16-byte keys (minimum size), fingerprint is extracted by the unified mixer.
	fpSizes := []int{1, 2, 4}
	for _, fpSize := range fpSizes {
		t.Run(fmt.Sprintf("fp=%d", fpSize), func(t *testing.T) {
			rng := newTestRNG(t)
			numKeys := 100
			keys := generateRandomKeys(rng, numKeys, 16) // minimum key size
			slices.SortFunc(keys, bytes.Compare)

			idx := buildAndOpen(t, keys, nil, WithFingerprint(fpSize))
			defer idx.Close()

			verifyMPHF(t, idx, keys)
			verifyNonMemberRejection(t, rng, idx, 1000)
		})
	}
}

// ============================================================================
// Algorithm-specific integration tests
// ============================================================================

// TestMixedKeyLengths verifies that keys of different lengths within a single
// build produce correct results.
func TestMixedKeyLengths(t *testing.T) {
	keyLens := []int{16, 20, 24, 32}
	keysPerLen := 125
	numKeys := keysPerLen * len(keyLens)

	rng := newTestRNG(t)
	keys := make([][]byte, 0, numKeys)
	for _, kl := range keyLens {
		for range keysPerLen {
			key := make([]byte, kl)
			fillFromRNG(rng, key)
			keys = append(keys, key)
		}
	}

	payloads := make([]uint64, numKeys)
	for i := range payloads {
		payloads[i] = uint64(i)
	}

	sortKeysAndPayloads(keys, payloads)

	idx := buildAndOpen(t, keys, payloads, WithPayload(4), WithFingerprint(2))
	defer idx.Close()

	verifyMPHF(t, idx, keys)
	verifyPayloads(t, idx, keys, payloads, 4)
}

// TestSameK1DifferentK0 verifies that keys with identical k1 but different k0
// produce correct lookups. This is the core property that k0^k1 slot computation fixes.
func TestSameK1DifferentK0(t *testing.T) {
	const numKeys = 10
	keys := make([][]byte, numKeys)
	sharedK1 := uint64(0xDEADBEEFCAFEBABE)

	for i := range numKeys {
		key := make([]byte, 16)
		binary.LittleEndian.PutUint64(key[0:8], uint64(i*12345+67890))
		binary.LittleEndian.PutUint64(key[8:16], sharedK1)
		keys[i] = key
	}

	payloads := make([]uint64, numKeys)
	for i := range payloads {
		payloads[i] = uint64(i)
	}

	idx := buildAndOpenUnsorted(t, keys, payloads, t.TempDir(),
		WithFingerprint(4), WithPayload(4),
		WithAlgorithm(AlgoPTRHash))
	defer idx.Close()

	pi, err := idx.WithPayload()
	if err != nil {
		t.Fatalf("WithPayload: %v", err)
	}
	for i, key := range keys {
		_, payload, err := pi.QueryPayload(key)
		if err != nil {
			t.Errorf("QueryPayload failed for key %d: %v", i, err)
			continue
		}
		if payload != uint64(i) {
			t.Errorf("Wrong payload for key %d: got %d, want %d", i, payload, i)
		}
	}
}

// TestFingerprintKeyLengths verifies fingerprint extraction works correctly
// across different key lengths and fingerprint sizes. All key lengths use the
// unified mixer (extractFingerprint(k0, k1, fpSize)).
func TestFingerprintKeyLengths(t *testing.T) {
	type testCase struct {
		keyLen int
		name   string
	}

	fpConfigs := []struct {
		fpSize int
		cases  []testCase
	}{
		{1, []testCase{
			{16, "16B_min"},
			{17, "17B"},
			{24, "24B"},
		}},
		{2, []testCase{
			{16, "16B_min"},
			{17, "17B"},
			{18, "18B"},
			{24, "24B"},
		}},
		{4, []testCase{
			{16, "16B_min"},
			{17, "17B"},
			{18, "18B"},
			{19, "19B"},
			{20, "20B"},
			{24, "24B"},
		}},
	}

	algos := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	for _, algo := range algos {
		for _, fpCfg := range fpConfigs {
			for _, tc := range fpCfg.cases {
				name := fmt.Sprintf("%s/fp%d/%s", algo.name, fpCfg.fpSize, tc.name)
				t.Run(name, func(t *testing.T) {
					const numKeys = 100
					keyRNG := newTestRNG(t)
					keys := make([][]byte, numKeys)
					for i := range numKeys {
						key := make([]byte, tc.keyLen)
						fillFromRNG(keyRNG, key)
						keys[i] = key
					}

					payloads := make([]uint64, numKeys)
					for i := range payloads {
						payloads[i] = uint64(i)
					}

					idx := buildAndOpenUnsorted(t, keys, payloads, t.TempDir(),
						WithFingerprint(fpCfg.fpSize), WithPayload(4),
						WithAlgorithm(algo.algo))
					defer idx.Close()

					pi, err := idx.WithPayload()
					if err != nil {
						t.Fatalf("WithPayload: %v", err)
					}
					for i, key := range keys {
						_, payload, err := pi.QueryPayload(key)
						if err != nil {
							t.Errorf("QueryPayload failed for key %d: %v", i, err)
							continue
						}
						if payload != uint64(i) {
							t.Errorf("Wrong payload for key %d: got %d, want %d", i, payload, i)
						}
					}
				})
			}
		}
	}
}

// TestPTRHashErrIndistinguishableHashes verifies that when keys have
// indistinguishable hash values (identical first 16 bytes), the solver
// returns an appropriate error.
func TestPTRHashErrIndistinguishableHashes(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "indistinguishable.idx")

	// Create keys with identical first 16 bytes (K0 and K1 will be same)
	numKeys := 10
	keys := make([][32]byte, numKeys)

	suffixRNG := newTestRNG(t)
	for i := range keys {
		binary.BigEndian.PutUint64(keys[i][0:8], 0xDEADBEEFCAFEBABE)
		binary.BigEndian.PutUint64(keys[i][8:16], 0x1234567890ABCDEF)
		fillFromRNG(suffixRNG, keys[i][16:])
	}

	slices.SortFunc(keys, func(a, b [32]byte) int {
		return bytes.Compare(a[:], b[:])
	})

	entries := make([]entry, numKeys)
	for i, k := range keys {
		keyCopy := make([]byte, 32)
		copy(keyCopy, k[:])
		entries[i] = entry{Key: keyCopy, Payload: 0}
	}

	ctx := context.Background()
	err := buildFromSlice(ctx, indexPath, entries,
		WithAlgorithm(AlgoPTRHash),
		WithGlobalSeed(0),
	)

	// Keys with identical first 16 bytes produce identical k0/k1 and thus
	// identical slot inputs — no pilot can map them to distinct slots.
	if err == nil {
		t.Fatal("expected error for keys with identical first 16 bytes, got success")
	}
	if !errors.Is(err, sherr.ErrIndistinguishableHashes) &&
		!errors.Is(err, sherr.ErrDuplicateKey) {
		t.Fatalf("expected ErrIndistinguishableHashes or ErrDuplicateKey, got: %v", err)
	}
}

// TestDegenerateSplitHandling verifies that degenerate splits (8+0 or 0+8)
// are handled correctly by the bijection algorithm.
func TestDegenerateSplitHandling(t *testing.T) {
	// Create 8 keys with unique 16-byte prefixes
	keys := make([][]byte, 8)
	for i := range keys {
		keys[i] = make([]byte, 32)
		binary.BigEndian.PutUint64(keys[i][0:8], uint64(i)*0x123456789ABCDEF0)
		binary.BigEndian.PutUint64(keys[i][8:16], uint64(i)*0x0FEDCBA987654321+uint64(i+1000))
		binary.BigEndian.PutUint64(keys[i][16:24], uint64(i)*0x1111111111111111)
		binary.BigEndian.PutUint64(keys[i][24:32], uint64(i)*0x2222222222222222)
	}

	sortKeysByBlock(keys, 8, nil)

	idx := buildAndOpen(t, keys, nil)
	defer idx.Close()

	verifyMPHF(t, idx, keys)
}
