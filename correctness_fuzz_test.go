package streamhash

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

// This file fuzzes the core correctness contract across the full parameter
// space -- the project's stated #1 priority. For fuzzed (numKeys, payloadSize,
// fingerprintSize, algorithm), it builds the SAME key set via all four build
// paths (sorted-serial, sorted-parallel, unsorted-serial, unsorted-parallel)
// and asserts:
//
//   - each index is a valid minimal perfect hash (every key -> distinct rank in
//     [0,N)), Verify() passes, and payloads round-trip exactly;
//   - for AlgoBijection (order-independent), all four outputs are BYTE-IDENTICAL
//     -- the specialized/parallel paths must match the generic one.
//
// This targets the "hardcoded assumption about a configurable size" class (e.g.
// `<< 8` assuming a 1-byte field) that is silent for the default config and only
// breaks for other payload/fingerprint sizes. builder_lifecycle_test.go checks
// fixed configs; this sweeps the space.

func payloadMaskFor(payloadSize int) uint64 {
	switch {
	case payloadSize <= 0:
		return 0
	case payloadSize >= 8:
		return ^uint64(0)
	default:
		return (uint64(1) << (payloadSize * 8)) - 1
	}
}

// buildVariant builds keys into path using the given mode/workers/options,
// assigning each key the payload recorded in keyToPayload.
func buildVariant(t *testing.T, ctx context.Context, path, tmpDir string, keys [][]byte, keyToPayload map[string]uint64, unsorted bool, workers int, opts []BuildOption) {
	t.Helper()
	allOpts := append([]BuildOption{WithWorkers(workers)}, opts...)

	if unsorted {
		b, err := NewUnsortedBuilder(ctx, path, uint64(len(keys)), tmpDir, allOpts...)
		if err != nil {
			t.Fatalf("NewUnsortedBuilder: %v", err)
		}
		for _, k := range keys {
			if err := b.AddKey(k, keyToPayload[string(k)]); err != nil {
				b.Close()
				t.Fatalf("unsorted AddKey: %v", err)
			}
		}
		if err := b.Finish(); err != nil {
			t.Fatalf("unsorted Finish: %v", err)
		}
		return
	}

	sortedKeys := make([][]byte, len(keys))
	copy(sortedKeys, keys)
	sortKeysByBlock(sortedKeys, uint64(len(keys)), allOpts)
	b, err := NewSortedBuilder(ctx, path, uint64(len(keys)), allOpts...)
	if err != nil {
		t.Fatalf("NewSortedBuilder: %v", err)
	}
	for _, k := range sortedKeys {
		if err := b.AddKey(k, keyToPayload[string(k)]); err != nil {
			b.Close()
			t.Fatalf("sorted AddKey: %v", err)
		}
	}
	if err := b.Finish(); err != nil {
		t.Fatalf("sorted Finish: %v", err)
	}
}

// verifyVariant asserts the index at path is a valid MPHF with correct payloads.
func verifyVariant(t *testing.T, label, path string, keys [][]byte, keyToPayload map[string]uint64, payloadSize int) {
	t.Helper()
	idx, err := Open(path)
	if err != nil {
		t.Fatalf("%s: Open: %v", label, err)
	}
	defer idx.Close()
	if err := idx.Verify(); err != nil {
		t.Fatalf("%s: Verify: %v", label, err)
	}
	verifyMPHF(t, idx, keys) // shared helper: every key -> distinct rank in [0,N)

	if payloadSize > 0 {
		pidx, err := OpenPayload(path)
		if err != nil {
			t.Fatalf("%s: OpenPayload: %v", label, err)
		}
		defer pidx.Close()
		for _, k := range keys {
			_, got, err := pidx.QueryPayload(k)
			if err != nil {
				t.Fatalf("%s: QueryPayload: %v", label, err)
			}
			if want := keyToPayload[string(k)]; got != want {
				t.Fatalf("%s: payload round-trip: got %d want %d", label, got, want)
			}
		}
	}
}

// runCorrectnessConfig builds and validates one (algo, payload, fp, numKeys)
// configuration across all four build paths.
func runCorrectnessConfig(t *testing.T, algo Algorithm, payloadSize, fpSize, numKeys int, seed uint64) {
	t.Helper()

	rng := rand.New(rand.NewPCG(seed, seed^0xa5a5a5a5))
	keys := generateRandomKeys(rng, numKeys, 16)

	mask := payloadMaskFor(payloadSize)
	keyToPayload := make(map[string]uint64, numKeys)
	for i, k := range keys {
		keyToPayload[string(k)] = uint64(i) & mask
	}

	opts := []BuildOption{WithAlgorithm(algo)}
	if payloadSize > 0 {
		opts = append(opts, WithPayload(payloadSize))
	}
	if fpSize > 0 {
		opts = append(opts, WithFingerprint(fpSize))
	}

	ctx := context.Background()
	tmpDir := t.TempDir()
	variants := []struct {
		name     string
		unsorted bool
		workers  int
	}{
		{"sorted", false, 1},
		{"sorted-parallel", false, 4},
		{"unsorted", true, 1},
		{"unsorted-parallel", true, 4},
	}

	paths := make([]string, len(variants))
	for i, v := range variants {
		paths[i] = filepath.Join(tmpDir, v.name+".idx")
		buildVariant(t, ctx, paths[i], tmpDir, keys, keyToPayload, v.unsorted, v.workers, opts)
		verifyVariant(t, v.name, paths[i], keys, keyToPayload, payloadSize)
	}

	// Every build path must yield byte-identical output. Bijection is inherently
	// order-independent; PTRHash is now deterministic too (its eviction RNG is
	// seeded from the global seed, not process-global state), so cross-path
	// byte-identity holds for both algorithms. This guards both the
	// order-independence of the layout and the determinism of the solvers.
	ref, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatalf("read %s: %v", variants[0].name, err)
	}
	for i := 1; i < len(paths); i++ {
		other, err := os.ReadFile(paths[i])
		if err != nil {
			t.Fatalf("read %s: %v", variants[i].name, err)
		}
		if !bytes.Equal(ref, other) {
			t.Fatalf("%s output differs between %s and %s (p%d/fp%d/n%d): len %d vs %d",
				algo, variants[0].name, variants[i].name, payloadSize, fpSize, numKeys, len(ref), len(other))
		}
	}
}

// TestLargeScaleMPHFValidity builds 1M keys for both algorithms and brute-force
// verifies a valid minimal perfect hash (every key -> distinct rank in [0,N)),
// plus determinism at scale (parallel and serial builds are byte-identical). The
// rest of the suite caps at ~100k for full build+verify; this guards the
// many-blocks / Phase-2-heavy regime. Skipped under -short.
func TestLargeScaleMPHFValidity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 1M-key validity test under -short")
	}
	const n = 1_000_000
	keys := generateRandomKeys(newTestRNG(t), n, 16)

	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		t.Run(algo.String(), func(t *testing.T) {
			par := filepath.Join(t.TempDir(), "par.idx")
			ser := filepath.Join(t.TempDir(), "ser.idx")
			if err := quickBuild(context.Background(), par, keys, WithAlgorithm(algo), WithWorkers(4)); err != nil {
				t.Fatalf("parallel build: %v", err)
			}
			if err := quickBuild(context.Background(), ser, keys, WithAlgorithm(algo), WithWorkers(1)); err != nil {
				t.Fatalf("serial build: %v", err)
			}
			idx, err := Open(par)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer idx.Close()
			verifyMPHF(t, idx, keys) // brute force: every key -> distinct rank in [0,N)

			pb, err := os.ReadFile(par)
			if err != nil {
				t.Fatalf("read parallel: %v", err)
			}
			sb, err := os.ReadFile(ser)
			if err != nil {
				t.Fatalf("read serial: %v", err)
			}
			if !bytes.Equal(pb, sb) {
				t.Fatalf("%s: parallel(4) and serial(1) builds not byte-identical (%d vs %d bytes)", algo, len(pb), len(sb))
			}
		})
	}
}

// TestCorrectnessParameterSweep deterministically covers a representative slice
// of the parameter space across all build paths (the in-CI counterpart to the
// fuzz target).
func TestCorrectnessParameterSweep(t *testing.T) {
	payloadSizes := []int{0, 1, 4, 8}
	fpSizes := []int{0, 1, 4}
	sizes := []int{500}
	if !testing.Short() {
		payloadSizes = []int{0, 1, 2, 3, 4, 5, 6, 7, 8}
		fpSizes = []int{0, 1, 2, 3, 4}
		sizes = []int{200, 2000}
	}

	var seed uint64
	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		for _, ps := range payloadSizes {
			for _, fp := range fpSizes {
				for _, n := range sizes {
					seed++
					name := fmt.Sprintf("%s_p%d_fp%d_n%d", algo, ps, fp, n)
					t.Run(name, func(t *testing.T) {
						runCorrectnessConfig(t, algo, ps, fp, n, seed)
					})
				}
			}
		}
	}
}

// FuzzCorrectnessParameterSpace explores arbitrary (algo, payload, fp, numKeys)
// configurations, asserting the MPHF contract and cross-path byte-identity for
// bijection. A failure means a configuration produces a wrong or inconsistent
// index -- the silent-correctness class that point tests miss.
func FuzzCorrectnessParameterSpace(f *testing.F) {
	f.Add(uint8(0), uint8(4), uint8(1), uint16(2000)) // bijection, p4, fp1
	f.Add(uint8(1), uint8(4), uint8(1), uint16(2000)) // ptrhash,   p4, fp1
	f.Add(uint8(0), uint8(0), uint8(0), uint16(1000)) // bijection, MPHF-only
	f.Add(uint8(0), uint8(8), uint8(4), uint16(1500)) // bijection, wide payload + fp
	f.Add(uint8(1), uint8(2), uint8(3), uint16(800))  // ptrhash,   odd sizes
	f.Add(uint8(0), uint8(3), uint8(2), uint16(3000)) // bijection, odd sizes

	f.Fuzz(func(t *testing.T, algoB, payloadB, fpB uint8, numKeysB uint16) {
		algo := Algorithm(algoB % 2)
		payloadSize := int(payloadB % 9)   // 0..8
		fpSize := int(fpB % 5)             // 0..4
		numKeys := int(numKeysB%3000) + 50 // 50..3049
		runCorrectnessConfig(t, algo, payloadSize, fpSize, numKeys, uint64(numKeysB)+1)
	})
}
