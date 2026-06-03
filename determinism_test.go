package streamhash

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
)

// TestBuildDeterminismAndGlobalSeed verifies that builds are reproducible and
// that WithGlobalSeed actually controls the output for BOTH algorithms. PTRHash
// was previously non-deterministic — its Phase-2 eviction RNG drew from the
// process-global generator and ignored WithGlobalSeed — so the same inputs could
// produce different indexes run to run. This guards that fix:
//   - same seed  -> byte-identical output (reproducible)
//   - other seed -> different output (the seed is actually used)
func TestBuildDeterminismAndGlobalSeed(t *testing.T) {
	rng := rand.New(rand.NewPCG(99, 1234))
	keys := generateRandomKeys(rng, 5000, 16)

	buildWithSeed := func(t *testing.T, algo Algorithm, seed uint64) []byte {
		t.Helper()
		// quickBuild (test_helpers_test.go) copies + block-sorts the keys and
		// builds a sorted index; we just pass the algorithm/seed/workers options
		// and read the resulting bytes.
		p := filepath.Join(t.TempDir(), "idx")
		if err := quickBuild(context.Background(), p, keys, WithAlgorithm(algo), WithGlobalSeed(seed), WithWorkers(4)); err != nil {
			t.Fatalf("quickBuild: %v", err)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		return data
	}

	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		t.Run(algo.String(), func(t *testing.T) {
			a1 := buildWithSeed(t, algo, 0xA5A5A5A5)
			a2 := buildWithSeed(t, algo, 0xA5A5A5A5)
			if !bytes.Equal(a1, a2) {
				t.Fatalf("%s: identical inputs + same WithGlobalSeed produced different output (not reproducible)", algo)
			}
			other := buildWithSeed(t, algo, 0x12345678)
			if bytes.Equal(a1, other) {
				t.Fatalf("%s: a different WithGlobalSeed produced identical output (seed is ignored)", algo)
			}
		})
	}
}
