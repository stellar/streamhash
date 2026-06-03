package ptrhash

import (
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/tamirms/streamhash/internal/encoding"
	"github.com/tamirms/streamhash/internal/sherr"
)

// Builder builds a block using the PTRHash algorithm.
//
// Block format:
//   - Pilots: 8 bits x numBuckets (direct bytes)
//   - RemapCount: 2 bytes
//   - RemapTable: remapCount x 2 bytes (uint16_le slot index)
//   - Payloads: stored in separate region at slot positions
//
// Key differences from Bijection algorithm:
//   - Uses lambda=3.16 (vs 3.0) for fewer buckets
//   - Uses alpha=0.99 for ~1% slot slack
//   - Uses 8-bit pilots (0-255) instead of variable-length seeds
//   - No EF/GR encoding - pilots are stored directly
type Builder struct {
	// Configuration (set at construction, used for blockBuilder interface)
	numBlocks       uint32
	globalSeed      uint64
	payloadSize     int
	fingerprintSize int

	// Keys grouped by bucket
	buckets [][]bucketEntry

	// Metadata
	maxKeysPerBlock int
	keysInBlock     int

	// Reusable solver (avoids allocation per block)
	solver *solver

	// Solver output
	numSlots int
	pilots   []uint8
	remap    []uint16 // Direct lookup table: remap[slot-numKeys] = hole in [0, numKeys)
}

// bucketEntry holds a key and its optional payload during block building.
// 32-byte struct (3×uint64 + 1×uint32 + 4 bytes alignment padding).
// Field order optimized for cache efficiency: hot data (suffix) first.
type bucketEntry struct {
	suffix      uint64 // 8 bytes - k1: bytes 8-15 as little-endian for bucket assignment (HOT)
	k0          uint64 // 8 bytes - k0: bytes 0-7 as little-endian for slot computation with k0^k1
	payload     uint64 // 8 bytes - Inline payload (max 8 bytes, packed)
	fingerprint uint32 // 4 bytes - Pre-extracted fingerprint (max 4 bytes)
}

// NewBuilder creates a new PTRHash block builder.
// totalKeys is used to compute numBlocks (cached at construction).
// Uses package-level constants for configuration (lambda, bucketsPerBlock, alpha).
func NewBuilder(totalKeys uint64, globalSeed uint64, payloadSize, fingerprintSize int) *Builder {
	buckets := make([][]bucketEntry, bucketsPerBlock)
	// Pre-allocate for ~99.9th percentile of Poisson(lambda) to avoid most runtime growth
	expectedPerBucket := int(math.Ceil(lambda * bucketPreallocMultiplier))
	for i := range buckets {
		buckets[i] = make([]bucketEntry, 0, expectedPerBucket)
	}

	// 20% headroom above expected average to account for statistical variation across blocks
	solverMaxKeys := int(float64(bucketsPerBlock) * lambda * solverMaxKeysMultiplier)

	// Compute overflow limit: expectedAvg + 7σ.
	// Matches the unsorted mode's region margin (~10^-12 per block).
	// With bucketsPerBlock fixed, expectedAvg ≈ bucketsPerBlock × lambda,
	// so this limit is well below the uint16 capacity of solver internals.
	nb := numBlocks(totalKeys)
	expectedAvg := float64(totalKeys) / float64(nb)
	sigma := math.Sqrt(expectedAvg)
	maxKeys := int(expectedAvg + blockOverflowSigmas*sigma)

	return &Builder{
		numBlocks:       nb,
		globalSeed:      globalSeed,
		payloadSize:     payloadSize,
		fingerprintSize: fingerprintSize,
		buckets:         buckets,
		maxKeysPerBlock: maxKeys,
		solver:          newSolver(bucketsPerBlock, solverMaxKeys),
	}
}

// AddKey adds a key to the block.
// k0, k1: key bytes as little-endian uint64s
// payload: user data to store with this key
// fingerprint: pre-extracted from the key end for verification
func (b *Builder) AddKey(k0, k1 uint64, payload uint64, fingerprint uint32) {
	// k1 is the suffix (bytes 8-15 as little-endian uint64)
	// Bucket assignment uses suffix directly with CubicEps distribution.
	localBucketIdx := cubicEpsBucket(k1, bucketsPerBlock)

	b.buckets[localBucketIdx] = append(b.buckets[localBucketIdx], bucketEntry{
		suffix:      k1,
		k0:          k0,
		payload:     payload,
		fingerprint: fingerprint,
	})
	b.keysInBlock++
}

// KeysAdded returns the number of keys added to the builder so far.
func (b *Builder) KeysAdded() int {
	return b.keysInBlock
}

// Reset clears the builder for reuse.
func (b *Builder) Reset() {
	for i := range b.buckets {
		b.buckets[i] = b.buckets[i][:0]
	}
	b.keysInBlock = 0
	b.numSlots = 0
	b.pilots = nil
	b.remap = nil
}

// NumBlocks returns the number of blocks (cached at construction).
func (b *Builder) NumBlocks() uint32 {
	return b.numBlocks
}

// GlobalConfigSize returns the size of algorithm-specific global config.
// PTRHash currently uses hardcoded constants, so no config is needed.
func (b *Builder) GlobalConfigSize() int {
	return 0
}

// EncodeGlobalConfigInto writes algorithm config to dst, returns bytes written.
// PTRHash currently uses hardcoded constants, so nothing is written.
func (b *Builder) EncodeGlobalConfigInto(dst []byte) int {
	return 0
}

// MaxIndexMetadataSize returns the worst-case maximum metadata size for any block.
// Used for file size estimation before keys are added.
// Pilots: bucketsPerBlock bytes + Remap table overhead.
func (b *Builder) MaxIndexMetadataSize() int {
	// Pilots: bucketsPerBlock bytes (1 byte per bucket)
	// Remap table: at most ~1% of max keys per block (alpha=0.99)
	maxKeys := int(float64(bucketsPerBlock) * lambda * solverMaxKeysMultiplier)
	maxOverflow := int(math.Ceil(float64(maxKeys)*(1.0/alpha-1.0))) + 1
	// Conservative margin for rounding and edge cases in remap table estimation
	return bucketsPerBlock + remapTableHeaderSize + maxOverflow*remapTableEntrySize + metadataEstimateMargin
}

// MaxMetadataSizeForCurrentBlock returns the max metadata size for the current block.
func (b *Builder) MaxMetadataSizeForCurrentBlock() int {
	return estimateMetadataSize(b.keysInBlock)
}

// BuildSeparatedInto builds the block into buffers.
// Returns (metadataLen, payloadsLen, numKeys, error).
func (b *Builder) BuildSeparatedInto(metadataDst, payloadsDst []byte) (int, int, int, error) {
	if b.keysInBlock == 0 {
		metaLen := buildEmptyMetadataInto(metadataDst)
		return metaLen, 0, 0, nil
	}

	if b.keysInBlock > b.maxKeysPerBlock {
		return 0, 0, 0, fmt.Errorf("%w: block has %d keys (max %d)",
			sherr.ErrBlockOverflow, b.keysInBlock, b.maxKeysPerBlock)
	}

	// Solve using PTRHash algorithm - pass metadataDst so pilots are written directly
	if err := b.solve(metadataDst); err != nil {
		return 0, 0, 0, err
	}

	// Encode metadata (pilots already written by solver, just encode remap table)
	metadataLen := b.encodeMetadataInto(metadataDst)

	// Encode payloads
	payloadsLen := b.encodePayloadsInto(payloadsDst)

	return metadataLen, payloadsLen, b.keysInBlock, nil
}

// solve runs the PTRHash solver on this block.
// metadataDst is the destination buffer - pilots are written directly there (zero-copy).
// Retries with different random seeds if eviction limit is hit (rare, <1 in 100K blocks).
func (b *Builder) solve(metadataDst []byte) error {
	var err error
	for attempt := range maxGlobalRetries {
		// Reset solver state for this block
		b.solver.reset(b.buckets, b.keysInBlock, b.globalSeed, metadataDst)
		b.pilots, b.remap, err = b.solver.solve(attempt)
		if err == nil {
			b.numSlots = int(b.solver.numSlots)
			return nil
		}
		// Only retry on eviction limit exceeded
		if !errors.Is(err, errEvictionLimitExceeded) {
			return err
		}
		// Retry (solve creates a new RNG for pilot start offsets on each call)
	}
	return err // Return last error after all retries exhausted
}

// encodeMetadataInto encodes PTRHash block metadata.
// Format: [Pilots (8-bit direct)][RemapTable]
// Note: Pilots are already written directly by the solver (zero-copy to destination).
// This function only needs to encode the remap table after the pilots.
func (b *Builder) encodeMetadataInto(dst []byte) int {
	// Pilots already written directly by solver - just skip past them
	// (8 bits per pilot = 1 byte per pilot)
	offset := len(b.pilots)

	// Encode remap table
	remapLen := encodeRemapTableInto(b.remap, dst[offset:])
	offset += remapLen

	return offset
}

// encodePayloadsInto encodes payloads using PTRHash slot assignments.
// Dispatches to encodePayloadsWithWriter for the actual single-pass encoding.
func (b *Builder) encodePayloadsInto(dst []byte) int {
	entrySize := b.entrySize()
	if entrySize == 0 || b.keysInBlock == 0 {
		return 0
	}

	requiredLen := b.keysInBlock * entrySize
	if len(dst) < requiredLen {
		panic("ptrhash: encodePayloadsInto: dst buffer too small")
	}

	switch entrySize {
	case 1, 4, 5, 8:
		b.encodePayloadsWithWriter(dst, entrySize, false)
	default:
		b.encodePayloadsWithWriter(dst, entrySize, true)
	}

	return b.keysInBlock * entrySize
}

// encodePayloadsWithWriter is the single-pass payload encoder for ptrhash.
// Combines slot computation with entry writing in a single iteration.
// pilotHash is hoisted to per-bucket (depends only on pilot + globalSeed),
// saving ~20 instructions per key vs computing it inline.
// When generic is false, uses WriteEntry (inlineable, supports 1/4/5/8).
// When generic is true, uses WriteEntryGeneric (handles all sizes).
//
// Precondition: len(dst) >= b.keysInBlock * entrySize (checked by encodePayloadsInto).
func (b *Builder) encodePayloadsWithWriter(dst []byte, entrySize int, generic bool) {
	basePtr := unsafe.Pointer(&dst[0])
	fpShift := uint(b.fingerprintSize * 8)
	numKeys := uint32(b.keysInBlock)
	numSlots := uint32(b.numSlots)
	globalSeed := b.globalSeed
	remap := b.remap

	for bucketIdx, bucket := range b.buckets {
		hp := pilotHash(b.pilots[bucketIdx], globalSeed)

		for _, entry := range bucket {
			hFolded := foldSlotInput(entry.k0, entry.suffix)
			slot := pilotSlotFolded(hFolded, hp, numSlots)
			if slot >= numKeys {
				slot = uint32(remap[slot-numKeys])
			}

			if generic {
				encoding.WriteEntryGeneric(basePtr, int(slot), entrySize, entry.fingerprint, entry.payload, fpShift)
			} else {
				encoding.WriteEntry(basePtr, int(slot), entrySize, entry.fingerprint, entry.payload, fpShift)
			}
		}
	}
}

// entrySize returns bytes stored per key (fingerprint + payload).
func (b *Builder) entrySize() int {
	return b.fingerprintSize + b.payloadSize
}

// buildEmptyMetadataInto writes metadata for an empty PTRHash block.
func buildEmptyMetadataInto(dst []byte) int {
	// All zero pilots (bucketsPerBlock bytes, 1 byte per pilot)
	clear(dst[:bucketsPerBlock])

	// Empty remap table: just the header with count=0
	dst[bucketsPerBlock] = 0
	dst[bucketsPerBlock+1] = 0

	return bucketsPerBlock + remapTableHeaderSize
}

// estimateMetadataSize returns a safe upper bound on metadata size for a PTRHash block.
// The actual size after building will be <= this estimate.
//
// SAFETY ANALYSIS - why these bounds are guaranteed sufficient:
//
//  1. Pilots: exactly numBuckets bytes (8 bits per bucket, direct storage)
//
// 2. Remap table:
//
//   - 2 bytes for entry count (uint16)
//
//   - (S - N) × 2 bytes for entries, where S = ceil(N / alpha) and N = numKeys
//
//   - At alpha=0.99: S - N = ceil(N/0.99) - N ≈ 0.01 × N entries
//
//   - Each entry is 2 bytes (uint16 slot index)
//
//   - +1 entry margin handles ceil() rounding
//
//     Example: numKeys=1000 → numSlots=1011 → 11 entries → 2 + 11×2 = 24 bytes
//     Max reasonable: numKeys=31600 → ~320 entries → 2 + 321×2 = 644 bytes
func estimateMetadataSize(numKeys int) int {
	if numKeys == 0 {
		// Empty block: pilots (all zeros) + empty remap table header
		return bucketsPerBlock + remapTableHeaderSize
	}

	// Pilots: exactly bucketsPerBlock bytes (8 bits per bucket, 1 byte each)
	pilotBytes := bucketsPerBlock

	// Remap table size calculation:
	// - numSlots = ceil(numKeys / alpha), where alpha = 0.99
	// - Overflow slots = numSlots - numKeys (slots that exceed valid range)
	// - At alpha=0.99: overflow ≈ 1% of numKeys
	// - +1 safety margin handles ceil() rounding edge cases in estimation
	numSlots := int(computeNumSlots(numKeys))
	numOverflowSlots := numSlots - numKeys + 1 // +1 = safety margin for estimation
	remapBytes := remapTableHeaderSize + numOverflowSlots*remapTableEntrySize

	return pilotBytes + remapBytes
}
