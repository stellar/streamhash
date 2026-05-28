package bijection

import (
	"encoding/binary"
	"math/bits"
)

// --- Golomb-Rice constants ---

// grMaxQuotient is the maximum unary quotient in Golomb-Rice encoding.
// When the quotient reaches this value, the encoder writes a fallback marker
// (grMaxQuotient consecutive 1-bits) instead of the normal code.
const grMaxQuotient = 16

// golombParameterTable maps bucket size to Golomb-Rice k parameter.
// Index 0-1 are unused (bucket sizes 0-1 don't encode seeds).
// The jump from k=5 (size 6) to k=7 (size 7) skipping k=6 is empirically
// optimized: at bucket size 7, the expected seed distribution produces
// shorter codes with k=7 than k=6.
var golombParameterTable = [9]uint8{0, 0, 1, 2, 3, 4, 5, 7, 8}

// --- Fallback list constants ---

const compactFallbackEntrySize = 4
const fallbackValidationXOR = 0x55 // XOR pattern for validation byte
const maxFallbackEntries = 255     // 1-byte count field limits entries

// fallbackEntry represents an entry in the fallback list.
type fallbackEntry struct {
	bucketIndex uint16
	subBucket   uint8
	seed        uint32
}

// seedStreamEncoder encodes seed values using Golomb-Rice coding.
type seedStreamEncoder struct {
	bw *bitWriter
}

// =============================================================================
// Elias-Fano Encoding
// =============================================================================

// Elias-Fano Encoding Overview:
//
// Elias-Fano is a succinct encoding for monotonically increasing sequences.
// For n values in range [0, U), it uses approximately n*floor(log2(U/n)) + 2n bits.
//
// Structure:
//   - Low bits: For each value v, store (v & lowMask) using L bits, where L = floor(log2(U/n))
//   - High bits: Unary-coded gaps between (v >> L) values, with selector 1-bits

// computeEFLowBits computes the low bits width for Elias-Fano encoding.
// L = max(0, floor(log2(U/n))) where U = keysInBlock and n = numBuckets.
func computeEFLowBits(n, U int) int {
	if U <= n || n == 0 {
		return 0
	}
	return bits.Len(uint(U)/uint(n)) - 1
}

// encodeEliasFanoInto encodes using a reusable buffer to avoid allocations.
func encodeEliasFanoInto(cumulative []uint16, U uint32, buf *[]byte) []byte {
	n := len(cumulative)
	if n == 0 {
		return nil
	}

	lowBits := computeEFLowBits(n, int(U))

	// Calculate sizes
	highBound := 0
	if lowBits < 16 {
		highBound = int(U >> lowBits)
	}
	highBitsSize := n + highBound
	lowBitsSize := n * lowBits
	totalBits := highBitsSize + lowBitsSize
	totalBytes := (totalBits + 7) / 8

	// Grow buffer if needed
	if cap(*buf) < totalBytes {
		*buf = make([]byte, totalBytes)
	} else {
		*buf = (*buf)[:totalBytes]
		clear(*buf) // Zero out for reuse
	}
	b := *buf

	// Write low bits using 64-bit word batching: pack multiple values into a
	// single uint64 and flush to the byte slice only when the word is full.
	// The common path (value fits in current word) is a single OR + shift;
	// the rare cross-boundary path splits the value across two words.
	if lowBits > 0 {
		var word uint64
		wordBitPos := 0
		bytePos := 0
		lowMask := uint16((1 << lowBits) - 1)

		for _, v := range cumulative {
			lowVal := uint64(v & lowMask)

			if wordBitPos+lowBits <= 64 {
				word |= lowVal << wordBitPos
				wordBitPos += lowBits
			} else {
				bitsInCurrent := 64 - wordBitPos
				word |= lowVal << wordBitPos
				binary.LittleEndian.PutUint64(b[bytePos:], word)
				bytePos += 8
				word = lowVal >> bitsInCurrent
				wordBitPos = lowBits - bitsInCurrent
			}
		}

		if wordBitPos > 0 {
			numBytes := (wordBitPos + 7) / 8
			var tmpBuf [8]byte
			binary.LittleEndian.PutUint64(tmpBuf[:], word)
			copy(b[bytePos:bytePos+numBytes], tmpBuf[:numBytes])
		}
	}

	// Write high bits (unary encoding of gaps)
	bitPos := lowBitsSize
	prevHigh := 0

	for _, v := range cumulative {
		high := int(v) >> lowBits
		gap := high - prevHigh
		bitPos += gap
		b[bitPos/8] |= 1 << (bitPos % 8)
		bitPos++
		prevHigh = high
	}

	return b
}

// eliasFanoSize returns the size in bytes for encoding n values with universe U.
func eliasFanoSize(n, U int) int {
	if n == 0 {
		return 0
	}

	lowBits := computeEFLowBits(n, U)

	highBound := 0
	if lowBits < 16 {
		highBound = U >> lowBits
	}
	highBitsSize := n + highBound
	lowBitsSize := n * lowBits
	totalBits := highBitsSize + lowBitsSize

	return (totalBits + 7) / 8
}

// =============================================================================
// Golomb-Rice Encoding
// =============================================================================

// Golomb-Rice Encoding Overview:
//
// Golomb-Rice encodes non-negative integers using a tunable parameter k.
// Each value v is split into:
//   - Quotient q = v >> k (encoded in unary: q ones followed by a 0)
//   - Remainder r = v & ((1 << k) - 1) (encoded in k bits)

func golombParameter(bucketSize int) uint8 {
	if bucketSize < len(golombParameterTable) {
		return golombParameterTable[bucketSize]
	}
	return 8
}

// maxEncodableSeed returns the maximum seed that can be Golomb-Rice encoded.
func maxEncodableSeed(bucketSize int) uint32 {
	k := golombParameter(bucketSize)
	if k == 0 {
		return 0
	}
	return grMaxQuotient << k
}

func newSeedStreamEncoder() *seedStreamEncoder {
	return &seedStreamEncoder{
		bw: newBitWriterBuffer(),
	}
}

func (e *seedStreamEncoder) encodeSeed(seed uint32, bucketSize int) {
	if bucketSize <= 1 {
		return
	}

	k := golombParameter(bucketSize)
	q := seed >> k
	r := seed & ((1 << k) - 1)

	if q >= grMaxQuotient || bucketSize > 8 {
		e.bw.writeOnes(grMaxQuotient)
		return
	}

	if q > 0 {
		e.bw.writeOnes(int(q))
	}
	e.bw.writeBit(0)

	if k > 0 {
		e.bw.writeBits(uint64(r), int(k))
	}
}

func (e *seedStreamEncoder) encodeFallbackMarker() {
	e.bw.writeOnes(grMaxQuotient)
}

func (e *seedStreamEncoder) bytes() []byte {
	return e.bw.flush()
}

func (e *seedStreamEncoder) resetEncoder() {
	e.bw.reset()
}

// =============================================================================
// Fallback List Encoding
// =============================================================================

func encodeFallbackListIntoWithB(entries []fallbackEntry, blockBits uint32, buf *[]byte) []byte {
	if len(entries) > maxFallbackEntries {
		panic("bijection: fallback list exceeds maximum entries")
	}

	// Format: [count][entries...][count XOR 0x55]
	// The validation byte at the end allows reliable detection
	needed := 2 + len(entries)*compactFallbackEntrySize
	if cap(*buf) < needed {
		*buf = make([]byte, needed)
	} else {
		*buf = (*buf)[:needed]
	}
	b := *buf

	count := uint8(len(entries))
	b[0] = count

	// Compact entry packs three fields into 32 bits (little-endian):
	//   bits [0, seedBits)        = seed
	//   bit  [seedBits]           = subBucket (0 or 1)
	//   bits [seedBits+1, 32)     = bucketIndex
	// seedBits = 31 - blockBits, so seed (seedBits) + subBucket (1) +
	// bucketIndex (blockBits) = 32 bits.
	seedBits := 31 - blockBits

	for i, e := range entries {
		packed := (uint32(e.bucketIndex) << (1 + seedBits)) |
			(uint32(e.subBucket&1) << seedBits) |
			(e.seed & ((1 << seedBits) - 1))

		offset := 1 + i*compactFallbackEntrySize
		b[offset] = byte(packed)
		b[offset+1] = byte(packed >> 8)
		b[offset+2] = byte(packed >> 16)
		b[offset+3] = byte(packed >> 24)
	}

	// Write validation byte at the end
	b[len(b)-1] = count ^ fallbackValidationXOR

	return b
}

// maxExtendedSeedForB returns the exclusive upper bound for extended seed search.
func maxExtendedSeedForB(blockBits uint32) uint32 {
	seedBits := 31 - blockBits
	return 1 << seedBits
}
