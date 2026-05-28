package bijection

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"testing"

	"github.com/tamirms/streamhash/internal/sherr"
)

// Named seeds for deterministic reproduction.
const (
	testSeed1 = 0x1234567890ABCDEF
	testSeed2 = 0xFEDCBA9876543210
)

func newTestRNG(t testing.TB) *rand.Rand {
	t.Helper()
	h := fnv.New128a()
	h.Write([]byte(t.Name()))
	sum := h.Sum(nil)
	s1 := binary.LittleEndian.Uint64(sum[:8])
	s2 := binary.LittleEndian.Uint64(sum[8:])
	return rand.New(rand.NewPCG(testSeed1^s1, testSeed2^s2))
}

type keyPair struct {
	k0, k1 uint64
}

// buildBlockAndDecode builds a block using the Builder, then returns the metadata,
// keys, and a Decoder for verification.
func buildBlockAndDecode(t *testing.T, rng *rand.Rand, numKeys int, globalSeed uint64) ([]byte, []keyPair, *Decoder) {
	t.Helper()

	bb := NewBuilder(uint64(numKeys*2), globalSeed, 0, 0) // MPHF-only
	bb.maxKeysPerBlock = numKeys + 1000                   // allow large blocks for testing

	keys := make([]keyPair, numKeys)
	for i := range keys {
		k0 := rng.Uint64()
		k1 := rng.Uint64()
		keys[i] = keyPair{k0, k1}
		bb.AddKey(k0, k1, 0, 0)
	}

	metaDst := make([]byte, bb.MaxMetadataSizeForCurrentBlock())
	payDst := make([]byte, 0)

	metaLen, _, _, err := bb.BuildSeparatedInto(metaDst, payDst)
	if err != nil {
		t.Fatalf("BuildSeparatedInto failed: %v", err)
	}

	dec, err := NewDecoder(nil, globalSeed)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}

	return metaDst[:metaLen], keys, dec
}

// assertBijection verifies that slots are unique and in [0, size).
func assertBijection(t *testing.T, slots []int, size int) {
	t.Helper()
	seen := make(map[int]bool, len(slots))
	for i, s := range slots {
		if s < 0 || s >= size {
			t.Errorf("slot[%d]=%d out of range [0, %d)", i, s, size)
		}
		if seen[s] {
			t.Errorf("slot[%d]=%d is a duplicate", i, s)
		}
		seen[s] = true
	}
}

// verifyBijection checks that the seed produces a valid bijection.
func verifyBijection(t *testing.T, bucket []bucketEntry, seed uint32, size uint32) {
	t.Helper()
	occupied := make(map[uint32]bool)
	for i, entry := range bucket {
		slot := mixFromParts(entry.mixParts, seed, size)
		if slot >= size {
			t.Errorf("slot %d >= size %d for entry %d", slot, size, i)
		}
		if occupied[slot] {
			t.Errorf("collision at slot %d (seed=%d, size=%d)", slot, seed, size)
		}
		occupied[slot] = true
	}
}

// TestWymixPinnedValues pins the wymix function (128-bit multiply + XOR fold)
// to known outputs. This guards against accidental changes to the mixing
// primitive that would silently break compatibility with persisted indexes:
// round-trip tests cannot catch such changes because both build and query
// use the same function in the same process.
func TestWymixPinnedValues(t *testing.T) {
	cases := []struct {
		a, b uint64
		want uint64
	}{
		{0x0000000000000000, 0x0000000000000000, 0x0000000000000000},
		{0x0000000000000001, 0x0000000000000001, 0x0000000000000001},
		{0x0123456789ABCDEF, 0xFEDCBA9876543210, 0x2317228F48165BB2},
		{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
		{0xDEADBEEFCAFEBABE, 0x0123456789ABCDEF, 0x7E4BD22A04F6DD7F},
	}
	for _, tc := range cases {
		got := wymix(tc.a, tc.b)
		if got != tc.want {
			t.Errorf("wymix(0x%016X, 0x%016X) = 0x%016X, want 0x%016X",
				tc.a, tc.b, got, tc.want)
		}
	}
}

// TestSolverWithSpecificFailingKeys is a regression test for a specific
// historical failure case where particular key byte patterns caused solver
// failures. Retained for reproducibility.
func TestSolverWithSpecificFailingKeys(t *testing.T) {
	globalSeed := uint64(testSeed1)

	keyAHex := "26f0586901fdf9c2da9034aa24af3d12e68f5ba74735eb27"
	keyBHex := "36f38c4bcfaef65c44ce9cb6e9270f126ad890950e13dd46"

	keyA, err := hex.DecodeString(keyAHex)
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := hex.DecodeString(keyBHex)
	if err != nil {
		t.Fatal(err)
	}

	k0A := binary.LittleEndian.Uint64(keyA[0:8])
	k1A := binary.LittleEndian.Uint64(keyA[8:16])
	k0B := binary.LittleEndian.Uint64(keyB[0:8])
	k1B := binary.LittleEndian.Uint64(keyB[8:16])

	mixPartsA := mixParts{
		xoredK0: k0A ^ globalSeed,
		xoredK1: k1A ^ globalSeed,
	}
	mixPartsB := mixParts{
		xoredK0: k0B ^ globalSeed,
		xoredK1: k1B ^ globalSeed,
	}

	if mixPartsA == mixPartsB {
		t.Fatal("mixParts are identical — guaranteed collision")
	}

	bucket := []bucketEntry{
		{mixParts: mixPartsA},
		{mixParts: mixPartsB},
	}

	seed, needsFallback := solveBucket2(bucket, maxEncodableSeed(2))
	if needsFallback {
		// Verify extended search succeeds for these keys
		maxSeed := maxExtendedSeedForB(blockBits)
		for s := range maxSeed {
			sA := mixFromParts(mixPartsA, s, 2)
			sB := mixFromParts(mixPartsB, s, 2)
			if sA != sB {
				return // extended search would find a working seed
			}
		}
		t.Fatalf("no working seed in %d seeds for these keys", maxSeed)
	} else {
		verifyBijection(t, bucket, seed, 2)
	}
}

// TestSolveBucketSpecializedVsBitmask compares specialized solvers
// (solveBucket2-8) against solveBucketBitmask for consistency.
func TestSolveBucketSpecializedVsBitmask(t *testing.T) {
	rng := newTestRNG(t)

	solvers := map[int]func([]bucketEntry, uint32) (uint32, bool){
		2: solveBucket2,
		3: solveBucket3,
		4: solveBucket4,
		5: solveBucket5,
		6: solveBucket6,
		7: solveBucket7,
		8: solveBucket8,
	}

	for size := 2; size <= 8; size++ {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			specializedSuccesses := 0
			bitmaskSuccesses := 0

			for trial := range 100 {
				// Generate random mixParts
				entries := make([]bucketEntry, size)
				for j := range entries {
					entries[j].mixParts = mixParts{
						xoredK0: rng.Uint64(),
						xoredK1: rng.Uint64(),
					}
				}

				maxSeed := maxEncodableSeed(size)
				seedSpec, fallbackSpec := solvers[size](entries, maxSeed)
				seedBitmask, fallbackBitmask := solveBucketBitmask(entries, size, maxSeed)

				if !fallbackSpec {
					specializedSuccesses++
					verifyBijection(t, entries, seedSpec, uint32(size))
				}
				if !fallbackBitmask {
					bitmaskSuccesses++
					verifyBijection(t, entries, seedBitmask, uint32(size))
				}

				// Both should find the same first valid seed
				if !fallbackSpec && !fallbackBitmask {
					if seedSpec != seedBitmask {
						t.Errorf("size=%d trial=%d: specialized seed=%d, bitmask seed=%d (both valid but differ)",
							size, trial, seedSpec, seedBitmask)
					}
				}
			}

			t.Logf("size=%d: specialized=%d/100 bitmask=%d/100", size, specializedSuccesses, bitmaskSuccesses)

			// With deterministic seeds and 100 trials, sizes 2-8 should succeed
			// nearly 100% of the time (observed: all 100/100). Use 90% threshold
			// to allow minor statistical variation if seeds change.
			if specializedSuccesses < 90 || bitmaskSuccesses < 90 {
				t.Errorf("size=%d: unexpectedly low success rate: spec=%d bitmask=%d",
					size, specializedSuccesses, bitmaskSuccesses)
			}
		})
	}
}

// TestSolveBucketBitmaskLargerSizes tests solveBucketBitmask for sizes 9-16.
func TestSolveBucketBitmaskLargerSizes(t *testing.T) {
	rng := newTestRNG(t)

	for _, size := range []int{9, 10, 12, 16} {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			// Production range: solveDirectBucket passes maxEncodableSeed(size),
			// which is 4096 for sizes >= 8 (golombParameter=8, 16<<8).
			prodMax := maxEncodableSeed(size)
			prodSuccesses := 0

			// Extended range: solveExtended passes maxExtendedSeedForB(blockBits).
			// Used as fallback when the production range fails.
			extMax := maxExtendedSeedForB(blockBits)
			extSuccesses := 0

			for range 100 {
				entries := make([]bucketEntry, size)
				for j := range entries {
					entries[j].mixParts = mixParts{
						xoredK0: rng.Uint64(),
						xoredK1: rng.Uint64(),
					}
				}

				seed, needsFallback := solveBucketBitmask(entries, size, prodMax)
				if !needsFallback {
					prodSuccesses++
					verifyBijection(t, entries, seed, uint32(size))
				}

				seed, needsFallback = solveBucketBitmask(entries, size, extMax)
				if !needsFallback {
					extSuccesses++
					verifyBijection(t, entries, seed, uint32(size))
				}
			}

			// Extended range should succeed for the vast majority of trials.
			// Observed rates: size=9: 100, size=10: 100, size=12: 100, size=16: 89.
			// Use 80% threshold to allow variation.
			if extSuccesses < 80 {
				t.Errorf("size=%d extended: only %d/100 trials succeeded, expected at least 80",
					size, extSuccesses)
			}
			// Production success rate drops with larger buckets (e.g. ~95% for
			// size 9, ~0% for size 16). Just verify the solver is functionally
			// correct — any successes must produce valid bijections.
			t.Logf("size=%d: production=%d/100 extended=%d/100", size, prodSuccesses, extSuccesses)
		})
	}
}

// TestSolveBucketDuplicates verifies that duplicate entries cause
// needsFallback=true. Historical regression — size=2 duplicate caused incorrect pilot.
func TestSolveBucketDuplicates(t *testing.T) {
	rng := newTestRNG(t)

	// Sizes 2-8: specialized solvers (the production code path)
	solvers := map[int]func([]bucketEntry, uint32) (uint32, bool){
		2: solveBucket2,
		3: solveBucket3,
		4: solveBucket4,
		5: solveBucket5,
		6: solveBucket6,
		7: solveBucket7,
		8: solveBucket8,
	}
	for size := 2; size <= 8; size++ {
		mp := mixParts{xoredK0: rng.Uint64(), xoredK1: rng.Uint64()}
		entries := make([]bucketEntry, size)
		for j := range entries {
			entries[j].mixParts = mp // all identical
		}

		maxSeed := maxEncodableSeed(size)
		_, needsFallback := solvers[size](entries, maxSeed)
		if !needsFallback {
			t.Errorf("size=%d: duplicate entries should require fallback", size)
		}
	}

	// Sizes 9-16: bitmask solver
	for _, size := range []int{9, 12, 16} {
		mp := mixParts{xoredK0: rng.Uint64(), xoredK1: rng.Uint64()}
		entries := make([]bucketEntry, size)
		for j := range entries {
			entries[j].mixParts = mp
		}

		_, needsFallback := solveBucketBitmask(entries, size, maxEncodableSeed(8))
		if !needsFallback {
			t.Errorf("size=%d: duplicate entries should require fallback", size)
		}
	}
}

// TestMixFromPartsEdgeCases verifies mixFromParts returns 0 for
// bucketSize 0 or 1, regardless of seed.
func TestMixFromPartsEdgeCases(t *testing.T) {
	mp := mixParts{xoredK0: 0xDEADBEEF, xoredK1: 0xCAFEBABE}
	for _, seed := range []uint32{0, 1, 42, 0xFFFFFFFF} {
		if got := mixFromParts(mp, seed, 0); got != 0 {
			t.Errorf("mixFromParts(_, %d, 0) = %d, want 0", seed, got)
		}
		if got := mixFromParts(mp, seed, 1); got != 0 {
			t.Errorf("mixFromParts(_, %d, 1) = %d, want 0", seed, got)
		}
	}
}

// TestSolverPostCondition builds blocks with random keys in MPHF-only
// mode and verifies solver postconditions via the production Decoder.QuerySlot API.
func TestSolverPostCondition(t *testing.T) {
	keyCounts := []int{10, 100, 500, 1000, 2000, 2800}

	for _, keyCount := range keyCounts {
		t.Run(fmt.Sprintf("keys=%d", keyCount), func(t *testing.T) {
			rng := newTestRNG(t)
			for trial := range 10 {
				globalSeed := testSeed1 ^ uint64(keyCount) ^ uint64(trial)

				meta, keys, dec := buildBlockAndDecode(t, rng, keyCount, globalSeed)

				slots := make([]int, len(keys))
				for i, kp := range keys {
					slot, err := dec.QuerySlot(kp.k0, kp.k1, meta, keyCount)
					if err != nil {
						t.Fatalf("keyCount=%d trial=%d key=%d: QuerySlot error: %v",
							keyCount, trial, i, err)
					}
					slots[i] = slot
				}

				assertBijection(t, slots, keyCount)
			}
		})
	}
}

// TestSolverPostConditionLargeScale tests the solver at scale (up to 10k keys)
// by overriding the block size limit and verifying through the production
// Decoder.QuerySlot path.
func TestSolverPostConditionLargeScale(t *testing.T) {
	keyCounts := []int{3000, 5000, 8000, 10000}

	for _, keyCount := range keyCounts {
		t.Run(fmt.Sprintf("keys=%d", keyCount), func(t *testing.T) {
			rng := newTestRNG(t)
			globalSeed := testSeed1 ^ uint64(keyCount)

			meta, keys, dec := buildBlockAndDecode(t, rng, keyCount, globalSeed)

			slots := make([]int, len(keys))
			for i, kp := range keys {
				slot, err := dec.QuerySlot(kp.k0, kp.k1, meta, keyCount)
				if err != nil {
					t.Fatalf("key=%d: QuerySlot error: %v", i, err)
				}
				slots[i] = slot
			}

			assertBijection(t, slots, keyCount)
		})
	}
}

// TestSolveSplitBucketDuplicateKey verifies that solveSplitBucket reports
// ErrDuplicateKey when entries have identical mixParts. Identical mixParts
// imply identical keys (the XOR with globalSeed is bijective): mixFromParts
// returns the same hash for every seed, so no split can separate them. This is
// a duplicate key, not a seed-search exhaustion, so the precise ErrDuplicateKey
// is returned rather than ErrSplitBucketSeedSearchFailed.
func TestSolveSplitBucketDuplicateKey(t *testing.T) {
	bb := NewBuilder(10000, testSeed1, 0, 0)

	identicalParts := mixParts{xoredK0: 0xAAAAAAAAAAAAAAAA, xoredK1: 0xBBBBBBBBBBBBBBBB}
	bucket := make([]bucketEntry, splitThreshold)
	for i := range bucket {
		bucket[i] = bucketEntry{mixParts: identicalParts}
	}

	_, _, err := bb.solveSplitBucket(0, bucket)
	if err == nil {
		t.Fatal("expected ErrDuplicateKey, got nil")
	}
	if !errors.Is(err, sherr.ErrDuplicateKey) {
		t.Fatalf("expected ErrDuplicateKey, got: %v", err)
	}
}
