package bijection

import "testing"

// (verifyBijection lives in solver_test.go.)

// randomBucket builds a bucket of distinct random mixParts. With 128 random bits
// per entry, collisions are effectively impossible, so the bucket is duplicate-free.
func randomBucket(rng interface{ Uint64() uint64 }, size int) []bucketEntry {
	bucket := make([]bucketEntry, size)
	for i := range bucket {
		bucket[i] = bucketEntry{mixParts: mixParts{xoredK0: rng.Uint64(), xoredK1: rng.Uint64()}}
	}
	return bucket
}

// TestBucketSolverParity covers the generic array solver (solveDirectBucketArray,
// 0% in normal runs because >64-entry buckets don't occur with random keys) by
// exercising it on solvable sizes and asserting it agrees with the dispatched
// reference (unrolled for 2-8, bitmask for 9-64) and the bitmask solver: same
// (seed, found), and any found seed is a true bijection. All three search seeds
// 0..maxSeed-1 with the same collision rule, so they must return the identical
// first collision-free seed.
func TestBucketSolverParity(t *testing.T) {
	rng := newTestRNG(t)
	bb := NewBuilder(100000, testSeed1, 0, 0)

	for trial := 0; trial < 500; trial++ {
		size := 2 + rng.IntN(63) // [2, 64]
		bucket := randomBucket(rng, size)
		maxSeed := maxEncodableSeed(size)
		if maxSeed == 0 {
			continue
		}

		refSeed, refFail := bb.solveDirectBucket(bucket, size)
		arrSeed, arrFail := bb.solveDirectBucketArray(bucket, size, maxSeed)
		bmSeed, bmFail := solveBucketBitmask(bucket, size, maxSeed)

		if arrFail != refFail || (!arrFail && arrSeed != refSeed) {
			t.Fatalf("size %d: array=(%d,%v) != reference=(%d,%v)", size, arrSeed, arrFail, refSeed, refFail)
		}
		if bmFail != refFail || (!bmFail && bmSeed != refSeed) {
			t.Fatalf("size %d: bitmask=(%d,%v) != reference=(%d,%v)", size, bmSeed, bmFail, refSeed, refFail)
		}
		if !refFail {
			verifyBijection(t, bucket, refSeed, uint32(size))
		}
	}
}

// TestExtendedSolverParity covers solveExtendedArray (0% in normal runs) by
// asserting it agrees with solveExtendedBitmask on small, reliably-solvable
// buckets, and that the found seed is a true bijection.
func TestExtendedSolverParity(t *testing.T) {
	rng := newTestRNG(t)
	bb := NewBuilder(100000, testSeed1, 0, 0)

	for trial := 0; trial < 200; trial++ {
		size := 2 + rng.IntN(7) // [2, 8]: a bijection is found well within the seed budget
		bucket := randomBucket(rng, size)

		arrSeed, arrErr := bb.solveExtendedArray(bucket, size)
		bmSeed, bmErr := bb.solveExtendedBitmask(bucket, size)

		if (arrErr == nil) != (bmErr == nil) {
			t.Fatalf("size %d: array err=%v but bitmask err=%v", size, arrErr, bmErr)
		}
		if arrErr == nil && arrSeed != bmSeed {
			t.Fatalf("size %d: array seed=%d != bitmask seed=%d", size, arrSeed, bmSeed)
		}
		if arrErr == nil {
			verifyBijection(t, bucket, arrSeed, uint32(size))
		}
	}
}

// TestLargeBucketSolverNoCrash exercises the >64-entry array paths directly.
// Buckets this large never arise from well-distributed keys (lambda≈3), so these
// paths are otherwise unexercised; the point is that the slot indexing stays in
// bounds (no panic/OOB) and any seed reported as solving is a real bijection.
// A bijection on >64 entries is not expected within the seed budget, so the
// common outcome is "not found" — which still exercises the full search.
func TestLargeBucketSolverNoCrash(t *testing.T) {
	rng := newTestRNG(t)
	bb := NewBuilder(100000, testSeed1, 0, 0)

	for _, size := range []int{65, 130, 256} {
		bucket := randomBucket(rng, size)
		seed, fail := bb.solveDirectBucketArray(bucket, size, maxEncodableSeed(size))
		if !fail {
			verifyBijection(t, bucket, seed, uint32(size))
		}
	}

	// One pass through the extended array path (larger 2^21 seed budget).
	const size = 65
	bucket := randomBucket(rng, size)
	if seed, err := bb.solveExtendedArray(bucket, size); err == nil {
		verifyBijection(t, bucket, seed, uint32(size))
	}
}
