package bijection

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	intbits "github.com/stellar/streamhash/internal/bits"
	"github.com/stellar/streamhash/internal/encoding"

	"github.com/stellar/streamhash/internal/sherr"
)

// Block metadata format (separated layout with 128-bucket checkpoint stride):
//
//	Offset              Size        Field
//	----------------------------------------------------------------------
//	0                   28B         Checkpoints (EFBitPos + SeedBitPos)
//	28                  efSize      EF data: [LowBits][HighBits]
//	28+efSize           var         Seeds (Golomb-Rice encoded)
//	...                 var         Fallback list
//	----------------------------------------------------------------------
//
// Payloads are stored separately in a contiguous region (numKeys * entrySize).
//
// Deterministic Splitting: Split buckets (size >= 8) use splitPoint = size/2.
// No split sizes are stored - the split is always balanced.
// Queries scan up to 128 buckets per checkpoint, with ~0.073 bits/key checkpoint overhead
// (224 bits / 3072 keys = 0.073).

// checkpoints enables 128-bucket stride decode instead of scanning all 1024 buckets.
// Stores both EF and Seed stream positions at 128-bucket segment boundaries.
type checkpoints struct {
	// efBitPos[i] = bit offset in EF high-bits bitvector at the START of bucket (i+1)*checkpointInterval
	efBitPos [numCheckpoints]uint16

	// seedBitPos[i] = bit offset in seed stream at the START of bucket (i+1)*checkpointInterval
	seedBitPos [numCheckpoints]uint16
}

// bucketEntry holds a key and its optional payload during block building.
type bucketEntry struct {
	mixParts    mixParts // 16 bytes - Pre-computed for bijection solving (HOT)
	payload     uint64   // 8 bytes - Inline payload (max 8 bytes, packed)
	fingerprint uint32   // 4 bytes - Pre-extracted fingerprint (max 4 bytes)
}

// Builder builds a single block's encoded data.
// Implements the blockBuilder interface defined in the main package.
type Builder struct {
	// Configuration (set at construction, used for blockBuilder interface)
	numBlocks       uint32
	globalSeed      uint64
	payloadSize     int
	fingerprintSize int

	// Keys grouped by bucket (local bucket index within this block)
	buckets [][]bucketEntry

	// Metadata for encoding
	maxKeysPerBlock int
	bucketSizes     []int    // Size of each bucket
	cumulative      []uint16 // Cumulative counts
	seeds           []uint32 // Seeds for non-split buckets
	splitSeeds      []uint32 // Seeds for split buckets (pairs)
	splitSeedIdx    []int    // Map bucket index to splitSeeds index
	fallbackList    []fallbackEntry
	keysInBlock     int

	// Reusable buffers to avoid allocations
	slots      []bool        // For seed search
	subBucket0 []bucketEntry // For split bucket solving
	subBucket1 []bucketEntry // For split bucket solving

	// Reusable encoders to reduce GC pressure
	seedEncoder *seedStreamEncoder // For seed stream encoding

	// Reusable output buffers (grow as needed, retained across blocks)
	efBuffer       []byte // For Elias-Fano encoding
	fallbackBuffer []byte // For fallback list encoding
}

// encode encodes checkpoints into buf.
func (cp *checkpoints) encode(buf []byte) {
	// EFBitPos
	for i := range numCheckpoints {
		binary.LittleEndian.PutUint16(buf[i*2:], cp.efBitPos[i])
	}
	// SeedBitPos
	for i := range numCheckpoints {
		binary.LittleEndian.PutUint16(buf[numCheckpoints*2+i*2:], cp.seedBitPos[i])
	}
}

// NewBuilder creates a new Builder for the bijection algorithm.
// totalKeys is used to compute numBlocks (cached at construction).
// payloadSize and fingerprintSize are per-instance configuration (bytes per key).
// Uses package-level constants for algorithm parameters (lambda, bucketsPerBlock).
func NewBuilder(totalKeys uint64, globalSeed uint64, payloadSize, fingerprintSize int) *Builder {
	// Pre-allocate bucket slices with expected capacity to avoid append() reallocations.
	buckets := make([][]bucketEntry, bucketsPerBlock)
	// ~4× lambda; over-provision to avoid reallocations in hot-path append
	const expectedPerBucket = 12
	for i := range buckets {
		buckets[i] = make([]bucketEntry, 0, expectedPerBucket)
	}

	// Compute overflow limit: expectedAvg + 7σ.
	// Matches the unsorted mode's region margin (~10^-12 per block).
	// With bucketsPerBlock fixed, expectedAvg ≈ bucketsPerBlock × lambda,
	// so this limit is well below the uint16 capacity of cumulative counts.
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
		bucketSizes:     make([]int, bucketsPerBlock),
		cumulative:      make([]uint16, bucketsPerBlock),
		seeds:           make([]uint32, bucketsPerBlock),
		splitSeedIdx:    make([]int, bucketsPerBlock),
		slots:           make([]bool, initialSlotCapacity),
		seedEncoder:     newSeedStreamEncoder(),
	}
}

// entrySize returns bytes stored per key (fingerprint + payload).
func (bb *Builder) entrySize() int {
	return bb.payloadSize + bb.fingerprintSize
}

// AddKey adds a key to the block.
// k0, k1: key bytes as little-endian uint64s
// payload: user data to store with this key
// fingerprint: pre-extracted from the key end for verification
func (bb *Builder) AddKey(k0, k1 uint64, payload uint64, fingerprint uint32) {
	// Compute mixParts from k0, k1 (bijection-specific)
	mp := mixParts{
		xoredK0: k0 ^ bb.globalSeed,
		xoredK1: k1 ^ bb.globalSeed,
	}

	// Compute local bucket using k0 for consistency with the original Bijection
	// design. Bucket assignment uses only k0, while fingerprint extraction uses
	// a mixer over both k0 and k1 — no correlation risk regardless of which
	// hash half is used for buckets.
	localBucketIdx := intbits.FastRange32(k0, bucketsPerBlock)

	bb.buckets[localBucketIdx] = append(bb.buckets[localBucketIdx], bucketEntry{
		mixParts:    mp,
		payload:     payload,
		fingerprint: fingerprint,
	})
	bb.keysInBlock++
}

// KeysAdded returns the number of keys added to the builder so far.
func (bb *Builder) KeysAdded() int {
	return bb.keysInBlock
}

// Reset clears the builder for reuse.
func (bb *Builder) Reset() {
	for i := range bb.buckets {
		bb.buckets[i] = bb.buckets[i][:0]
		bb.splitSeedIdx[i] = -1
	}
	clear(bb.bucketSizes)
	clear(bb.cumulative)
	clear(bb.seeds)
	bb.splitSeeds = bb.splitSeeds[:0]
	bb.fallbackList = bb.fallbackList[:0]
	bb.keysInBlock = 0
}

// BuildSeparatedInto builds the block directly into provided buffers (zero-copy).
// Returns (metadataLen, payloadsLen, numKeys, error).
func (bb *Builder) BuildSeparatedInto(metadataDst, payloadsDst []byte) (int, int, int, error) {
	if bb.keysInBlock == 0 {
		metaLen := bb.buildEmptyMetadataInto(metadataDst)
		return metaLen, 0, 0, nil
	}

	if bb.keysInBlock > bb.maxKeysPerBlock {
		return 0, 0, 0, fmt.Errorf("%w: block has %d keys (max %d)",
			sherr.ErrBlockOverflow, bb.keysInBlock, bb.maxKeysPerBlock)
	}

	// Step 1: Compute bucket sizes and cumulative counts
	bb.computeCumulativeCounts()

	// Step 2: Solve bijections for all buckets
	if err := bb.solveBijections(); err != nil {
		return 0, 0, 0, err
	}

	// Step 3: Encode the metadata directly into metadataDst
	metadataLen, err := bb.encodeSeparatedMetadataInto(metadataDst)
	if err != nil {
		return 0, 0, 0, err
	}

	// Step 4: Encode payloads directly into payloadsDst
	payloadsLen := bb.encodePayloadsInto(payloadsDst)

	return metadataLen, payloadsLen, bb.keysInBlock, nil
}

// NumBlocks returns the number of blocks (cached at construction).
func (bb *Builder) NumBlocks() uint32 {
	return bb.numBlocks
}

// GlobalConfigSize returns the size of algorithm-specific global config.
// Bijection currently uses hardcoded constants, so no config is needed.
func (bb *Builder) GlobalConfigSize() int {
	return 0
}

// EncodeGlobalConfigInto writes algorithm config to dst, returns bytes written.
// Bijection currently uses hardcoded constants, so nothing is written.
func (bb *Builder) EncodeGlobalConfigInto(dst []byte) int {
	return 0
}

// MaxIndexMetadataSize returns the worst-case maximum metadata size for any block.
// Used for file size estimation before keys are added.
// Delegates to estimateMetadataSize with a large numKeys to get the non-empty worst case.
func (bb *Builder) MaxIndexMetadataSize() int {
	return estimateMetadataSize(bb.maxKeysPerBlock)
}

// MaxMetadataSizeForCurrentBlock returns the max metadata size for the current block.
func (bb *Builder) MaxMetadataSizeForCurrentBlock() int {
	return estimateMetadataSize(bb.keysInBlock)
}

// encodeSeparatedMetadataInto encodes block metadata directly into dst buffer (zero-copy).
func (bb *Builder) encodeSeparatedMetadataInto(dst []byte) (int, error) {
	// Step 1: Encode Elias-Fano cumulative counts and compute EF checkpoints
	var cp checkpoints
	efData, err := bb.encodeEFWithCheckpoints(&cp, bucketsPerBlock)
	if err != nil {
		return 0, err
	}

	// Step 2: Encode SeedStream and compute Seed checkpoints
	bb.seedEncoder.resetEncoder()
	if err := bb.encodeSeedsWithCheckpoints(&cp, bucketsPerBlock); err != nil {
		return 0, err
	}
	seedStreamData := bb.seedEncoder.bytes()

	// Guard the hard format limit before encoding: the fallback list uses a
	// 1-byte count, so more than maxFallbackEntries cannot be represented. This
	// is unreachable for honest/hashed keys (observed max ~5 over thousands of
	// blocks) but is craftable against the fixed default global seed, so fail
	// with a clean ErrBlockOverflow instead of panicking inside the encoder.
	if len(bb.fallbackList) > maxFallbackEntries {
		return 0, fmt.Errorf("%w: block produced %d fallback entries (max %d)",
			sherr.ErrBlockOverflow, len(bb.fallbackList), maxFallbackEntries)
	}

	// Step 3: Encode FallbackList with B parameter for compact format
	fallbackData := encodeFallbackListIntoWithB(bb.fallbackList, blockBits, &bb.fallbackBuffer)

	// Calculate total size and verify buffer is large enough
	totalSize := checkpointsSize + len(efData) + len(seedStreamData) + len(fallbackData)
	if len(dst) < totalSize {
		return 0, fmt.Errorf("bijection: metadata buffer too small: need %d, have %d", totalSize, len(dst))
	}

	offset := 0

	// Checkpoints (28 bytes at offset 0)
	cp.encode(dst[offset:])
	offset += checkpointsSize

	// EFData
	copy(dst[offset:], efData)
	offset += len(efData)

	// SeedStream
	copy(dst[offset:], seedStreamData)
	offset += len(seedStreamData)

	// FallbackList
	copy(dst[offset:], fallbackData)
	offset += len(fallbackData)

	return offset, nil
}

// encodePayloadsInto writes payloads directly to dst (zero-copy).
// Dispatches to encodePayloadsWithWriter with WriteEntry for entry sizes
// 1/4/5/8 (inlineable) or WriteEntryGeneric for other sizes.
func (bb *Builder) encodePayloadsInto(dst []byte) int {
	entrySize := bb.entrySize()
	if entrySize == 0 || bb.keysInBlock == 0 {
		return 0
	}

	requiredLen := bb.keysInBlock * entrySize
	if len(dst) < requiredLen {
		panic("bijection: encodePayloadsInto: dst buffer too small")
	}

	switch entrySize {
	case 1, 4, 5, 8:
		bb.encodePayloadsWithWriter(dst, entrySize, false)
	default:
		bb.encodePayloadsWithWriter(dst, entrySize, true)
	}

	return bb.keysInBlock * entrySize
}

// encodePayloadsWithWriter is the single-pass payload encoder.
// Computes each key's slot and writes the entry immediately.
// When generic is false, uses WriteEntry (inlineable, supports 4/5/8).
// When generic is true, uses WriteEntryGeneric (handles all sizes).
// The generic flag is loop-invariant and perfectly predicted.
//
// Precondition: len(dst) >= bb.keysInBlock * entrySize (checked by encodePayloadsInto).
func (bb *Builder) encodePayloadsWithWriter(dst []byte, entrySize int, generic bool) {
	basePtr := unsafe.Pointer(&dst[0])
	fpShift := uint(bb.fingerprintSize * 8)

	localIdx := 0
	for bucketIdx, bucket := range bb.buckets {
		bucketStart := localIdx
		bucketSize := bb.bucketSizes[bucketIdx]

		if bucketSize == 0 {
			continue
		}

		if bucketSize >= splitThreshold {
			splitPoint := bucketSize / 2
			secondHalfSize := bucketSize - splitPoint

			seedIdx := bb.splitSeedIdx[bucketIdx]
			seed0 := bb.splitSeeds[seedIdx]
			seed1 := bb.splitSeeds[seedIdx+1]

			if seed0 == fallbackMarker {
				seed0 = bb.resolveFallbackSeed(bucketIdx, 0)
			}
			if seed1 == fallbackMarker {
				seed1 = bb.resolveFallbackSeed(bucketIdx, 1)
			}

			for _, entry := range bucket {
				h := mixFromParts(entry.mixParts, seed0, uint32(bucketSize))
				var slot int
				if h < uint32(splitPoint) {
					slot = int(h)
				} else {
					slot = splitPoint + int(mixFromParts(entry.mixParts, seed1, uint32(secondHalfSize)))
				}

				if generic {
					encoding.WriteEntryGeneric(basePtr, bucketStart+slot, entrySize, entry.fingerprint, entry.payload, fpShift)
				} else {
					encoding.WriteEntry(basePtr, bucketStart+slot, entrySize, entry.fingerprint, entry.payload, fpShift)
				}
			}
			localIdx += bucketSize
		} else {
			seed := bb.seeds[bucketIdx]
			if seed == fallbackMarker {
				seed = bb.resolveFallbackSeed(bucketIdx, 0)
			}

			for _, entry := range bucket {
				slot := 0
				if bucketSize > 1 {
					slot = int(mixFromParts(entry.mixParts, seed, uint32(bucketSize)))
				}

				if generic {
					encoding.WriteEntryGeneric(basePtr, bucketStart+slot, entrySize, entry.fingerprint, entry.payload, fpShift)
				} else {
					encoding.WriteEntry(basePtr, bucketStart+slot, entrySize, entry.fingerprint, entry.payload, fpShift)
				}
			}
			localIdx += bucketSize
		}
	}
}

// resolveFallbackSeed looks up the actual seed for a fallback entry.
// Panics if the entry is not found, since the fallback list is never truncated
// in the builder and a missing entry indicates a programming error.
func (bb *Builder) resolveFallbackSeed(bucketIdx int, subBucket uint8) uint32 {
	for _, fe := range bb.fallbackList {
		if fe.bucketIndex == uint16(bucketIdx) && fe.subBucket == subBucket {
			return fe.seed
		}
	}
	panic("bijection: fallback entry not found")
}

// encodeEFWithCheckpoints encodes Elias-Fano and computes EF checkpoints.
func (bb *Builder) encodeEFWithCheckpoints(cp *checkpoints, numBuckets int) ([]byte, error) {
	efData := encodeEliasFanoInto(bb.cumulative, uint32(bb.keysInBlock), &bb.efBuffer)

	lowBits := computeEFLowBits(numBuckets, bb.keysInBlock)

	for checkpointIdx := range numCheckpoints {
		bucketIdx := (checkpointIdx + 1) * checkpointInterval
		if bucketIdx > numBuckets {
			break
		}

		var highPart int
		prevCumulative := bb.cumulative[bucketIdx-1]
		if lowBits < 16 {
			highPart = int(prevCumulative) >> lowBits
		}

		// Checkpoints are stored as uint16. This shouldn't overflow for valid
		// blocks, but if it ever does, error out instead of clamping to a wrong
		// offset (which would silently corrupt query results). Mirrors the
		// seedBitPos check in encodeSeedsWithCheckpoints.
		bitPos := bucketIdx + highPart
		if bitPos > math.MaxUint16 {
			return nil, fmt.Errorf("bijection: EF bit position overflow: %d exceeds uint16 max", bitPos)
		}
		cp.efBitPos[checkpointIdx] = uint16(bitPos)
	}

	return efData, nil
}

// encodeSeedsWithCheckpoints encodes seeds and computes checkpoints.
func (bb *Builder) encodeSeedsWithCheckpoints(cp *checkpoints, numBuckets int) error {
	for bucketIdx := range numBuckets {
		if bucketIdx > 0 && bucketIdx%checkpointInterval == 0 {
			checkpointIdx := bucketIdx/checkpointInterval - 1
			if checkpointIdx < numCheckpoints {
				seedBitPos := bb.seedEncoder.bw.bitsWritten()
				if seedBitPos > math.MaxUint16 {
					return fmt.Errorf("bijection: seed bit position overflow: %d exceeds uint16 max", seedBitPos)
				}
				cp.seedBitPos[checkpointIdx] = uint16(seedBitPos)
			}
		}

		bucketSize := bb.bucketSizes[bucketIdx]
		if bucketSize <= 1 {
			continue
		}

		if bucketSize >= splitThreshold {
			splitPoint := bucketSize / 2
			secondHalfSize := bucketSize - splitPoint
			seedIdx := bb.splitSeedIdx[bucketIdx]

			seed0 := bb.splitSeeds[seedIdx]
			seed1 := bb.splitSeeds[seedIdx+1]

			if seed0 == fallbackMarker {
				bb.seedEncoder.encodeFallbackMarker()
			} else {
				bb.seedEncoder.encodeSeed(seed0, splitPoint)
			}

			if seed1 == fallbackMarker {
				bb.seedEncoder.encodeFallbackMarker()
			} else {
				bb.seedEncoder.encodeSeed(seed1, secondHalfSize)
			}
		} else {
			seed := bb.seeds[bucketIdx]
			if seed == fallbackMarker {
				bb.seedEncoder.encodeFallbackMarker()
			} else {
				bb.seedEncoder.encodeSeed(seed, bucketSize)
			}
		}
	}

	return nil
}

func (bb *Builder) computeCumulativeCounts() {
	cumulative := uint16(0)
	for i, bucket := range bb.buckets {
		bb.bucketSizes[i] = len(bucket)
		cumulative += uint16(len(bucket))
		bb.cumulative[i] = cumulative
	}
}

// buildEmptyMetadataInto writes minimal metadata for an empty block directly into dst.
func (bb *Builder) buildEmptyMetadataInto(dst []byte) int {
	efBytes := eliasFanoSize(bucketsPerBlock, 0)

	offset := 0

	var cp checkpoints
	cp.encode(dst[offset:])
	offset += checkpointsSize

	efEnd := offset + efBytes
	clear(dst[offset:efEnd])
	for i := range bucketsPerBlock {
		byteIdx := i / 8
		bitIdx := i % 8
		if offset+byteIdx < efEnd {
			dst[offset+byteIdx] |= 1 << bitIdx
		}
	}
	offset += efBytes

	dst[offset] = 0
	offset++

	return offset
}
