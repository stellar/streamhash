package bijection

import (
	"encoding/binary"
	"fmt"
	"math/bits"
	"unsafe"

	intbits "github.com/tamirms/streamhash/internal/bits"

	"github.com/tamirms/streamhash/internal/sherr"
)

// Decoder handles query-time slot computation for the bijection algorithm.
type Decoder struct {
	globalSeed uint64
}

// NewDecoder creates a Decoder for the bijection algorithm.
// globalConfig is algorithm-specific configuration (currently unused for bijection).
// Returns an error if globalConfig is unexpectedly non-empty.
func NewDecoder(globalConfig []byte, globalSeed uint64) (*Decoder, error) {
	if len(globalConfig) != 0 {
		return nil, fmt.Errorf("%w: bijection: unexpected non-empty global config (len=%d)", sherr.ErrCorruptedIndex, len(globalConfig))
	}
	return &Decoder{
		globalSeed: globalSeed,
	}, nil
}

// QuerySlot computes the slot for a key within a block.
// k0, k1: key representations (k0 is used for bucket assignment)
// metadata is the block's encoded metadata.
// keysInBlock is the number of keys in this block.
func (d *Decoder) QuerySlot(k0, k1 uint64, metadata []byte, keysInBlock int) (int, error) {
	if keysInBlock == 0 {
		return 0, sherr.ErrNotFound
	}

	if len(metadata) < checkpointsSize {
		return 0, sherr.ErrCorruptedIndex
	}

	// Step 1: Decode checkpoints
	var cp checkpoints
	decodeCheckpoints(metadata[0:checkpointsSize], &cp)

	// Step 2: Compute bucket index using k0 for consistency with the builder.
	// Fingerprint extraction uses a unified mixer over both k0 and k1,
	// so there is no correlation risk regardless of which half is used here.
	bucketIdx := int(intbits.FastRange32(k0, bucketsPerBlock))

	// Step 3: Determine segment and checkpoint position
	segment := bucketIdx / checkpointInterval
	segmentStart := segment * checkpointInterval

	// Step 4: EF data starts after checkpoints
	if checkpointsSize >= len(metadata) {
		return 0, sherr.ErrCorruptedIndex
	}
	efData := metadata[checkpointsSize:]

	// Step 5: Compute EF parameters for direct low bits access
	lowBits := computeEFLowBits(bucketsPerBlock, keysInBlock)

	// Get EF bit position from checkpoint for starting position in high bits
	var efBitPosInHigh int
	if segment > 0 {
		efBitPosInHigh = int(cp.efBitPos[segment-1])
	}

	// Step 6: Decode cumulative values for segment
	// Key: use cumulative[segmentStart:] as output, then access with absolute indices
	var cumulative [bucketsPerBlock]uint16
	decodeCount := decodeEFRangeInline(efData, bucketsPerBlock, keysInBlock, segmentStart, bucketIdx, efBitPosInHigh, cumulative[segmentStart:])

	if decodeCount == 0 {
		// Fallback to full decode if inline decode failed
		decodeEliasFanoUnrolled(efData, bucketsPerBlock, keysInBlock, &cumulative)
	}

	// Step 7: Compute cumulativeAtSegmentStart for segment boundary cases
	var cumulativeAtSegmentStart uint16
	if segment > 0 {
		// O(1) direct computation: cumulative[segmentStart-1]
		// high[segmentStart-1] = EFBitPos[segment-1] - segmentStart
		// low[segmentStart-1] = read from low bits at position (segmentStart-1) * lowBits
		highVal := max(efBitPosInHigh-segmentStart, 0)
		lowVal := readEFLowBits(efData, segmentStart-1, lowBits)
		cumulativeAtSegmentStart = uint16(highVal<<lowBits) | lowVal
	}

	// Step 8: Get bucket size using segment-aware cumulative indexing
	var bucketStart uint16
	if bucketIdx > 0 {
		if bucketIdx > segmentStart {
			// Within segment: use absolute index (works because we output to cumulative[segmentStart:])
			bucketStart = cumulative[bucketIdx-1]
		} else {
			// At segment boundary: use pre-computed value
			bucketStart = cumulativeAtSegmentStart
		}
	}
	bucketEnd := cumulative[bucketIdx]
	bucketSize := int(bucketEnd - bucketStart)

	if bucketSize == 0 {
		return 0, sherr.ErrNotFound
	}

	// Step 9: Get seed bit position from coarse checkpoint (128-bucket intervals)
	var seedBitPos int
	if segment > 0 {
		seedBitPos = int(cp.seedBitPos[segment-1])
	}

	// Step 10: Calculate seed stream offset
	efSize := eliasFanoSize(bucketsPerBlock, keysInBlock)
	seedStreamOffset := checkpointsSize + efSize
	if seedStreamOffset >= len(metadata) {
		return 0, sherr.ErrCorruptedIndex
	}

	// Step 11: Skip to segment start in seed stream using checkpoint
	seedData := metadata[seedStreamOffset:]
	br := newBitReaderFast(seedData)
	if seedBitPos > 0 {
		br.skipBits(seedBitPos)
	}

	// Step 12: Skip seeds from segment start to target bucket
	for i := segmentStart; i < bucketIdx; i++ {
		var bStart uint16
		if i > 0 {
			if i > segmentStart {
				bStart = cumulative[i-1]
			} else {
				bStart = cumulativeAtSegmentStart
			}
		}
		bSize := int(cumulative[i] - bStart)

		if bSize >= splitThreshold {
			// Deterministic splitting: splitPoint = size/2
			splitPoint := bSize / 2
			secondHalfSize := bSize - splitPoint

			// Skip first seed
			if splitPoint > 1 {
				q := br.readUnaryMax16()
				if q < 16 {
					k := golombParameter(splitPoint)
					if k > 0 {
						br.skipBits(int(k))
					}
				}
			}
			// Skip second seed
			if secondHalfSize > 1 {
				q := br.readUnaryMax16()
				if q < 16 {
					k := golombParameter(secondHalfSize)
					if k > 0 {
						br.skipBits(int(k))
					}
				}
			}
		} else if bSize >= 2 {
			// Skip seed
			q := br.readUnaryMax16()
			if q < 16 {
				k := golombParameter(bSize)
				if k > 0 {
					br.skipBits(int(k))
				}
			}
		}
	}

	// Step 13: Decode seed(s) for target bucket and compute slot
	mp := mixParts{
		xoredK0: k0 ^ d.globalSeed,
		xoredK1: k1 ^ d.globalSeed,
	}

	var localSlot uint32
	if bucketSize >= splitThreshold {
		// Deterministic splitting: splitPoint = size/2
		splitPoint := bucketSize / 2
		secondHalfSize := bucketSize - splitPoint

		// Decode both seeds
		seed0 := decodeGRSeedFast(br, splitPoint)
		seed1 := decodeGRSeedFast(br, secondHalfSize)

		// Handle fallback markers
		if seed0 == fallbackMarker || seed1 == fallbackMarker {
			seed0, seed1 = d.resolveFallbackSeeds(metadata, seedStreamOffset, bucketIdx, seed0, seed1)
		}

		// Deterministic splitting: use hash value to classify
		h := mixFromParts(mp, seed0, uint32(bucketSize))
		if h < uint32(splitPoint) {
			// First half: slot is the hash value directly
			localSlot = h
		} else {
			// Second half: use seed1 for bijection
			localSlot = uint32(splitPoint) + mixFromParts(mp, seed1, uint32(secondHalfSize))
		}
	} else if bucketSize >= 2 {
		// Non-split bucket
		seed := decodeGRSeedFast(br, bucketSize)

		// Handle fallback marker
		if seed == fallbackMarker {
			seed, _ = d.resolveFallbackSeeds(metadata, seedStreamOffset, bucketIdx, seed, 0)
		}

		localSlot = mixFromParts(mp, seed, uint32(bucketSize))
	} else {
		localSlot = 0
	}

	return int(bucketStart) + int(localSlot), nil
}

// resolveFallbackSeeds resolves fallback markers to actual seeds from the fallback list.
// The fallback list is located at the END of the metadata block, not right after the
// current bucket's seed. We search backwards from the end of metadata to find it.
func (d *Decoder) resolveFallbackSeeds(metadata []byte, seedStreamOffset, bucketIdx int, seed0, seed1 uint32) (uint32, uint32) {
	// Search backwards from end of metadata for the fallback list.
	// Format: [count][entries...][count XOR fallbackValidationXOR]
	// The validation byte at the end allows reliable detection.
	//
	// We look for an offset where:
	// 1. remaining bytes == 2 + count * 4
	// 2. metadata[end-1] == metadata[offset] ^ fallbackValidationXOR
	fallbackOffset := -1

	// Max fallback list size: 2 + 255 entries × 4 bytes = 1022 bytes.
	// The bound is inclusive (>=): a maximal (255-entry) list starts exactly at
	// len-maxFallbackSize, so a strict > would skip it and leave fallback seeds
	// unresolved for that block.
	maxFallbackSize := 2 + maxFallbackEntries*compactFallbackEntrySize
	if len(metadata) > seedStreamOffset {
		for tryOffset := len(metadata) - 2; tryOffset >= seedStreamOffset && tryOffset >= len(metadata)-maxFallbackSize; tryOffset-- {
			count := int(metadata[tryOffset])
			expectedSize := 2 + count*compactFallbackEntrySize
			remaining := len(metadata) - tryOffset
			if expectedSize == remaining {
				// Verify validation byte
				validationByte := metadata[len(metadata)-1]
				if validationByte == uint8(count)^fallbackValidationXOR {
					fallbackOffset = tryOffset
					break // Found valid fallback list
				}
			}
		}
	}

	if fallbackOffset >= 0 {
		fallbackList := decodeFallbackListWithB(metadata[fallbackOffset:], blockBits)
		for _, fe := range fallbackList {
			if fe.bucketIndex == uint16(bucketIdx) {
				if fe.subBucket == 0 && seed0 == fallbackMarker {
					seed0 = fe.seed
				} else if fe.subBucket == 1 && seed1 == fallbackMarker {
					seed1 = fe.seed
				}
			}
		}
	}
	return seed0, seed1
}

// =============================================================================
// Decode helpers
// =============================================================================

// decodeCheckpoints decodes checkpoints from buf.
func decodeCheckpoints(buf []byte, cp *checkpoints) {
	for i := range numCheckpoints {
		cp.efBitPos[i] = binary.LittleEndian.Uint16(buf[i*2:])
	}
	for i := range numCheckpoints {
		cp.seedBitPos[i] = binary.LittleEndian.Uint16(buf[numCheckpoints*2+i*2:])
	}
}

// readEFLowBits reads the low bits for a single element from EF data.
func readEFLowBits(efData []byte, element, lowBits int) uint16 {
	if lowBits == 0 || element < 0 {
		return 0
	}

	bitPos := element * lowBits
	byteIdx := bitPos / 8
	bitIdx := bitPos % 8

	if byteIdx >= len(efData) {
		return 0
	}

	lowMask := uint64((1 << lowBits) - 1)

	if byteIdx+8 <= len(efData) {
		val := binary.LittleEndian.Uint64(efData[byteIdx:])
		return uint16((val >> bitIdx) & lowMask)
	}

	val := uint64(efData[byteIdx])
	if byteIdx+1 < len(efData) {
		val |= uint64(efData[byteIdx+1]) << 8
	}
	if byteIdx+2 < len(efData) && lowBits+bitIdx > 8 {
		val |= uint64(efData[byteIdx+2]) << 16
	}

	return uint16((val >> bitIdx) & lowMask)
}

// decodeEFRangeInline decodes cumulative values from startElement to endElement.
func decodeEFRangeInline(efData []byte, numBuckets, keysInBlock int,
	startElement, endElement int, startBitInHighBits int, out []uint16) int {

	if startElement > endElement || startElement >= numBuckets {
		return 0
	}
	if endElement >= numBuckets {
		endElement = numBuckets - 1
	}
	if len(efData) < 8 {
		return 0
	}

	lowBits := computeEFLowBits(numBuckets, keysInBlock)
	lowBitsTotal := numBuckets * lowBits

	lowMask := uint64(0)
	if lowBits > 0 {
		lowMask = (uint64(1) << lowBits) - 1
	}

	count := 0
	bitPos := startBitInHighBits
	efDataLen := len(efData)

	// Base pointer for bounds-check-free 64-bit loads in the decode loop.
	// Safe because all byte indices are derived from EF parameters that
	// are bounded by efDataLen (checked at each access site).
	efDataPtr := unsafe.Pointer(&efData[0])

	currentHigh := uint16(0)
	if startElement > 0 && bitPos >= startElement {
		currentHigh = uint16(bitPos - startElement)
	}

	for elem := startElement; elem <= endElement; elem++ {
		var lowVal uint16
		if lowBits > 0 {
			lowBitPos := elem * lowBits
			lowByteIdx := lowBitPos / 8
			lowBitIdx := lowBitPos % 8
			if lowByteIdx+8 <= efDataLen {
				lowVal = uint16((*(*uint64)(unsafe.Pointer(uintptr(efDataPtr) + uintptr(lowByteIdx))) >> lowBitIdx) & lowMask)
			} else if lowByteIdx < efDataLen {
				val := uint64(efData[lowByteIdx])
				if lowByteIdx+1 < efDataLen {
					val |= uint64(efData[lowByteIdx+1]) << 8
				}
				if lowByteIdx+2 < efDataLen && lowBits+lowBitIdx > 8 {
					val |= uint64(efData[lowByteIdx+2]) << 16
				}
				lowVal = uint16((val >> lowBitIdx) & lowMask)
			}
		}

		for {
			absBitPos := lowBitsTotal + bitPos
			byteIdx := absBitPos / 8
			bitInByte := absBitPos % 8

			if byteIdx >= efDataLen {
				break
			}

			var window uint64
			if byteIdx+8 <= efDataLen {
				window = *(*uint64)(unsafe.Pointer(uintptr(efDataPtr) + uintptr(byteIdx))) >> bitInByte
			} else {
				window = loadWindow64Safe(efData, byteIdx) >> bitInByte
			}

			if window == 0 {
				bitsAvailable := 64 - bitInByte
				currentHigh += uint16(bitsAvailable)
				bitPos += bitsAvailable
				continue
			}

			zeros := bits.TrailingZeros64(window)
			currentHigh += uint16(zeros)
			bitPos += zeros + 1
			break
		}

		out[count] = (currentHigh << lowBits) | lowVal
		count++
	}

	return count
}

// loadWindow64Safe loads up to 64 bits from data starting at byteIdx.
// Padding bytes beyond the data are filled with 0xFF so that the EF high-bit
// scanner (TrailingZeros64 to find set bits) always terminates without
// looping past the buffer boundary.
func loadWindow64Safe(data []byte, byteIdx int) uint64 {
	remaining := len(data) - byteIdx
	if remaining >= 8 {
		return binary.LittleEndian.Uint64(data[byteIdx:])
	}
	var window uint64
	for i := range remaining {
		window |= uint64(data[byteIdx+i]) << (i * 8)
	}
	for i := remaining; i < 8; i++ {
		window |= uint64(0xFF) << (i * 8)
	}
	return window
}

// decodeEliasFanoCrossBoundary handles the rare case where bits span two 64-bit windows.
//
//go:noinline
func decodeEliasFanoCrossBoundary(current *uint64, bitPos *int, bytePos *int, data []byte, dataLen int, lowBits int) uint16 {
	bitsFromCurrent := 64 - *bitPos
	lowPart := *current >> *bitPos

	// Refill
	if *bytePos+8 <= dataLen {
		*current = binary.LittleEndian.Uint64(data[*bytePos:])
		*bytePos += 8
	} else {
		*current = 0
		for j := 0; j < dataLen-*bytePos; j++ {
			*current |= uint64(data[*bytePos+j]) << (j * 8)
		}
		*bytePos = dataLen
	}

	bitsFromNext := lowBits - bitsFromCurrent
	highPart := *current & ((1 << bitsFromNext) - 1)
	*bitPos = bitsFromNext

	return uint16(lowPart | (highPart << bitsFromCurrent))
}

// decodeEliasFanoUnrolled decodes Elias-Fano with loop unrolling for low bits.
func decodeEliasFanoUnrolled(data []byte, n, U int, out *[bucketsPerBlock]uint16) {
	if n == 0 || n > bucketsPerBlock {
		return
	}
	if len(data) < 8 {
		// In production, EF data is always >= 129 bytes (1024 buckets with minimum 1 key).
		// The unrolled decoder requires at least 8 bytes for its initial 64-bit load.
		return
	}

	_ = data[len(data)-1]

	lowBits := 0
	if U > n {
		lowBits = bits.Len(uint(U/n)) - 1
	}

	dataLen := len(data)
	bytePos := 0
	bitPos := 0
	current := binary.LittleEndian.Uint64(data[0:8])
	bytePos = 8

	// Precompute mask once to avoid repeated (1 << lowBits) - 1 in the unrolled loop.
	lowMask := uint64(0)
	if lowBits > 0 {
		lowMask = (uint64(1) << lowBits) - 1
	}

	// === PHASE 1: Read low bits with 8x unrolling ===
	i := 0

	for ; i+8 <= n; i += 8 {
		if lowBits == 0 {
			out[i], out[i+1], out[i+2], out[i+3] = 0, 0, 0, 0
			out[i+4], out[i+5], out[i+6], out[i+7] = 0, 0, 0, 0
		} else if bitPos+8*lowBits <= 64 {
			out[i] = uint16((current >> bitPos) & lowMask)
			out[i+1] = uint16((current >> (bitPos + lowBits)) & lowMask)
			out[i+2] = uint16((current >> (bitPos + 2*lowBits)) & lowMask)
			out[i+3] = uint16((current >> (bitPos + 3*lowBits)) & lowMask)
			out[i+4] = uint16((current >> (bitPos + 4*lowBits)) & lowMask)
			out[i+5] = uint16((current >> (bitPos + 5*lowBits)) & lowMask)
			out[i+6] = uint16((current >> (bitPos + 6*lowBits)) & lowMask)
			out[i+7] = uint16((current >> (bitPos + 7*lowBits)) & lowMask)
			bitPos += 8 * lowBits
		} else {
			for j := range 8 {
				if bitPos >= 64 {
					if bytePos+8 <= dataLen {
						current = binary.LittleEndian.Uint64(data[bytePos:])
						bytePos += 8
					} else {
						current = 0
						for k := 0; k < dataLen-bytePos; k++ {
							current |= uint64(data[bytePos+k]) << (k * 8)
						}
						bytePos = dataLen
					}
					bitPos = 0
				}

				if bitPos+lowBits <= 64 {
					out[i+j] = uint16((current >> bitPos) & lowMask)
					bitPos += lowBits
				} else {
					out[i+j] = decodeEliasFanoCrossBoundary(&current, &bitPos, &bytePos, data, dataLen, lowBits)
				}
			}
		}
	}

	// Handle remainder
	for ; i < n; i++ {
		if bitPos >= 64 {
			if bytePos+8 <= dataLen {
				current = binary.LittleEndian.Uint64(data[bytePos:])
				bytePos += 8
			} else {
				current = 0
				for k := 0; k < dataLen-bytePos; k++ {
					current |= uint64(data[bytePos+k]) << (k * 8)
				}
				bytePos = dataLen
			}
			bitPos = 0
		}

		if lowBits == 0 {
			out[i] = 0
		} else if bitPos+lowBits <= 64 {
			out[i] = uint16((current >> bitPos) & lowMask)
			bitPos += lowBits
		} else {
			out[i] = decodeEliasFanoCrossBoundary(&current, &bitPos, &bytePos, data, dataLen, lowBits)
		}
	}

	// === PHASE 2: Read high bits ===
	currentHigh := uint16(0)
	for i := range n {
		gap := uint32(0)
		for {
			if bitPos >= 64 {
				if bytePos+8 <= dataLen {
					current = binary.LittleEndian.Uint64(data[bytePos:])
					bytePos += 8
				} else {
					current = 0
					for j := 0; j < dataLen-bytePos; j++ {
						current |= uint64(data[bytePos+j]) << (j * 8)
					}
					bytePos = dataLen
					if current == 0 {
						break
					}
				}
				bitPos = 0
			}

			remaining := current >> bitPos
			zeros := bits.TrailingZeros64(remaining)
			available := 64 - bitPos

			if zeros < available {
				bitPos += zeros + 1
				gap += uint32(zeros)
				break
			}

			gap += uint32(available)
			bitPos = 64
		}

		currentHigh += uint16(gap)
		out[i] |= currentHigh << lowBits
	}
}

// decodeGRSeedFast decodes a single Golomb-Rice encoded seed.
func decodeGRSeedFast(br *bitReaderFast, bucketSize int) uint32 {
	if bucketSize <= 1 {
		return 0
	}

	q := br.readUnaryMax16()
	if q >= grMaxQuotient {
		return fallbackMarker
	}

	k := golombParameter(bucketSize)
	r := uint32(0)
	if k > 0 {
		r = uint32(br.readBits(int(k)))
	}

	return (q << k) | r
}

// decodeFallbackListWithB decodes a fallback list from data.
func decodeFallbackListWithB(data []byte, blockBits uint32) []fallbackEntry {
	if len(data) < 2 {
		return nil
	}

	count := int(data[0])
	// Format: [count][entries...][validation] = 2 + count*4 bytes
	if count == 0 || len(data) < 2+count*compactFallbackEntrySize {
		return nil
	}

	seedBits := 31 - blockBits
	seedMask := uint32((1 << seedBits) - 1)
	bucketMask := uint32((1 << blockBits) - 1)

	entries := make([]fallbackEntry, count)
	for i := range entries {
		offset := 1 + i*compactFallbackEntrySize
		packed := uint32(data[offset]) |
			uint32(data[offset+1])<<8 |
			uint32(data[offset+2])<<16 |
			uint32(data[offset+3])<<24

		entries[i] = fallbackEntry{
			bucketIndex: uint16((packed >> (1 + seedBits)) & bucketMask),
			subBucket:   uint8((packed >> seedBits) & 1),
			seed:        packed & seedMask,
		}
	}

	return entries
}
