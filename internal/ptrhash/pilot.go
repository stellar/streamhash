package ptrhash

import (
	"encoding/binary"
	"math/bits"

	intbits "github.com/tamirms/streamhash/internal/bits"
)

// pilotHashC is the mixing constant used by PTRHash for pilot hashing.
// This is a well-chosen odd constant that provides good mixing properties.
// Origin: PtrHash paper (https://arxiv.org/abs/2502.15539).
const pilotHashC = 0x517cc1b727220a95

const (
	remapTableHeaderSize = 2 // uint16 count
	remapTableEntrySize  = 2 // uint16 slot index
)

// =============================================================================
// Pilot Hash Functions (for slot computation)
// =============================================================================

// pilotHash computes the pilot hash value for a given pilot and seed.
// This can be pre-computed once per pilot and reused for all keys in a bucket.
// The solver precomputes all 256 values in reset() to avoid SplitMix64
// overhead in the hot inner loops (see solver.pilotHPs).
//
// =============================================================================
// DESIGN: SplitMix64 finalizer + "| 1"
// =============================================================================
//
// The function applies a SplitMix64 finalizer to the raw multiplication
// C*(pilot^globalSeed). This achieves two goals:
//
// 1. PILOT INDEPENDENCE (≈256 independent trials)
//
//	Without the finalizer, pilot hash values are correlated: the raw
//	multiplication C*x produces outputs where nearby inputs share high-bit
//	patterns. Empirically measured (10,000 trials):
//
//	  Without SplitMix64:  ~180-225 independent trials (pilots are correlated)
//	  With SplitMix64:     ~253-259 independent trials (pilots are independent)
//
//	At scale (1T keys, ~31.6M blocks), the per-block failure probability is
//	exponentially sensitive to pilot independence. Without the finalizer (~1 in
//	30 builds at 1T keys would fail); with it, builds are reliable (see config.go).
//
//	For comparison, the Rust PTRHash uses XOR slot mixing (h ^ hp) which
//	yields only ~3-5 independent trials, requiring much larger parts (~millions
//	of keys) to compensate. Our MUL slot mixing ((h*hp)^(h>>32)) is inherently
//	better but still needs SplitMix64 for full independence.
//
// 2. "| 1" ENSURES BIJECTIVE MULTIPLICATION
//
//	The "| 1" forces hp to be odd. Multiplying by an odd number is a bijection
//	mod 2^64 (every input maps to a unique output). Even hp values lose the LSB,
//	causing non-bijective multiplication and more slot collisions.
//
//	Additionally, "| 1" prevents hp=0 (which would collapse slot computation
//	to depend only on h>>32, losing half the entropy).
//
// =============================================================================
func pilotHash(pilot uint8, globalSeed uint64) uint64 {
	x := pilotHashC * (uint64(pilot) ^ globalSeed)
	// SplitMix64 finalizer (Stafford variant, from splitmix64.c by Sebastiano Vigna)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x | 1
}

// foldSlotInput precomputes h ^ (h >> 32) from h = k0 ^ k1.
// This is hoisted out of the inner pilot loop so the per-pilot cost is
// just MUL + UMULH (pilotSlotFolded), saving an XOR and a shift per
// pilot per entry on the critical path.
func foldSlotInput(k0, k1 uint64) uint64 {
	h := k0 ^ k1
	return h ^ (h >> 32)
}

// pilotSlotFolded computes the slot from a pre-folded hash value.
// hFolded must be computed by foldSlotInput (or equivalent h ^ (h >> 32)).
// This is the inner-loop primitive: just a 64-bit multiply + FastRange.
//
// The high 64 bits of the 128-bit product (hFolded*hp * numSlots) give a
// uniform mapping to [0, numSlots) — the same FastRange technique used in
// bits.FastRange32, but applied to the full 128-bit intermediate result.
func pilotSlotFolded(hFolded uint64, hp uint64, numSlots uint32) uint32 {
	hi, _ := bits.Mul64(hFolded*hp, uint64(numSlots))
	return uint32(hi)
}

// initPilotHPs precomputes pilotHash values for all 256 pilots into dst.
// Used by both the Decoder (query path) and solver (build path).
func initPilotHPs(dst *[numPilotValues]uint64, globalSeed uint64) {
	for i := range dst {
		dst[i] = pilotHash(uint8(i), globalSeed)
	}
}

// cubicEpsBucket computes LOCAL bucket index using CubicEps distribution.
// This creates skewed bucket sizes for faster Cuckoo solving:
//   - Many small buckets (1-2 keys) trivially placed in gaps
//   - Few large buckets (8+ keys) placed first when pool is empty
//
// Formula: x² × (1+x)/2 × 255/256 + x/256
//
// NOTE: Only used for local bucket assignment WITHIN a block.
func cubicEpsBucket(x uint64, numBuckets uint32) uint32 {
	if numBuckets <= 1 {
		return 0
	}

	// x² (high 64 bits of 128-bit product)
	x2, _ := bits.Mul64(x, x)

	// (1+x)/2 in fixed-point: shift right by 1, set MSB to represent +0.5
	xHalf := (x >> 1) | (1 << 63)

	// x² × (1+x)/2 (high 64 bits)
	cubic, _ := bits.Mul64(x2, xHalf)

	// Scale: × 255/256 + x/256
	scaled := (cubic/256)*255 + x/256

	// FastRange to bucket
	return intbits.FastRange32(scaled, numBuckets)
}

// =============================================================================
// Remap Table (for overflow slot remapping)
// =============================================================================
//
// The remap table maps "overflow" slots to "holes" in the valid range.
//
// Background:
//   - With load factor alpha (0.99), we have numSlots = ceil(numKeys / alpha)
//   - This means numSlots > numKeys, creating (numSlots - numKeys) overflow slots
//   - Slots in [0, numKeys) are "valid" positions for output
//   - Slots in [numKeys, numSlots) are "overflow" slots that can't be used directly
//   - The remap table maps each overflow slot to a "hole" (unused slot in valid range)
//
// Binary format:
//   - Header: 2 bytes (uint16 little-endian) = number of entries
//   - Entries: 2 bytes each (uint16 little-endian) = remapped slot index
//   - Total size: 2 + numEntries*2 bytes
//
// Example with numKeys=100, alpha=0.99:
//   - numSlots = ceil(100/0.99) = 102
//   - Overflow slots: [100, 101] (2 slots)
//   - Remap table: [count=2][slot_100_maps_to][slot_101_maps_to]
//   - Size: 2 + 2*2 = 6 bytes

// encodeRemapTableInto encodes a remap table into a pre-allocated destination.
// Returns the number of bytes written.
func encodeRemapTableInto(table []uint16, dst []byte) int {
	binary.LittleEndian.PutUint16(dst[0:remapTableHeaderSize], uint16(len(table)))
	for i, v := range table {
		offset := remapTableHeaderSize + i*remapTableEntrySize
		binary.LittleEndian.PutUint16(dst[offset:], v)
	}
	return remapTableHeaderSize + len(table)*remapTableEntrySize
}

// lookupRemap looks up the remapped slot for an overflow slot.
// If slot < numKeys, returns slot unchanged (already in valid range).
// If slot >= numKeys, returns the remapped value from the table.
func lookupRemap(data []byte, slot uint32, numKeys uint32) uint32 {
	if slot < numKeys {
		return slot
	}
	// slot is in overflow range [numKeys, numSlots)
	// Table index = slot - numKeys (e.g., slot 100 with numKeys=100 -> index 0)
	idx := int(slot - numKeys)
	offset := remapTableHeaderSize + idx*remapTableEntrySize
	return uint32(binary.LittleEndian.Uint16(data[offset:]))
}
