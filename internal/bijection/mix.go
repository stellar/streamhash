package bijection

import (
	"math/bits"

	intbits "github.com/stellar/streamhash/internal/bits"
)

// mixParts holds pre-computed parts of the Mix function that don't depend on the bucket seed.
// This enables fast bijection solving by avoiding key byte reads in the inner loop.
type mixParts struct {
	xoredK0 uint64 // k0 ^ globalSeed (pre-XORed with the global seed, not the bucket seed)
	xoredK1 uint64 // k1 ^ globalSeed (pre-XORed with the global seed, not the bucket seed)
}

// wymix performs a 128-bit multiply and XOR fold.
// This is the core mixing primitive from WyHash v4.
func wymix(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}

// mixFromParts computes a slot index using pre-computed mixParts.
func mixFromParts(parts mixParts, seed uint32, bucketSize uint32) uint32 {
	if bucketSize <= 1 {
		return 0
	}
	mixed := wymix(parts.xoredK0^uint64(seed), parts.xoredK1)
	return intbits.FastRange32(mixed, bucketSize)
}
