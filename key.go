package streamhash

import (
	"encoding/binary"
)

// MinKeySize is the minimum key length required for routing and hash computation.
// Keys must have at least 16 bytes to extract k0 (bytes 0-7) and k1 (bytes 8-15).
const MinKeySize = 16

// unpackFingerprintFromBytes unpacks fingerprint bytes into a uint32.
// Optimized for common fingerprint sizes (0, 1, 2, 4 bytes).
// Precondition: len(src) >= size.
func unpackFingerprintFromBytes(src []byte, size int) uint32 {
	switch size {
	case 0:
		return 0
	case 1:
		return uint32(src[0])
	case 2:
		return uint32(binary.LittleEndian.Uint16(src))
	case 4:
		return binary.LittleEndian.Uint32(src)
	default:
		// Generic fallback for size 3 (only non-standard size since max is 4)
		var v uint32
		for i := 0; i < size && i < 4; i++ {
			v |= uint32(src[i]) << (i * 8)
		}
		return v
	}
}

// packPayloadToBytes packs a uint64 payload into a byte slice.
// Optimized for common payload sizes (4, 8 bytes).
// Precondition: len(dst) >= size.
func packPayloadToBytes(dst []byte, payload uint64, size int) {
	switch size {
	case 4:
		binary.LittleEndian.PutUint32(dst, uint32(payload))
	case 8:
		binary.LittleEndian.PutUint64(dst, payload)
	default:
		for i := range size {
			dst[i] = byte(payload >> (i * 8))
		}
	}
}

// unpackPayloadFromBytes unpacks a uint64 payload from a byte slice.
// Optimized for common payload sizes (4, 8 bytes).
// Precondition: len(src) >= size.
func unpackPayloadFromBytes(src []byte, size int) uint64 {
	switch size {
	case 4:
		return uint64(binary.LittleEndian.Uint32(src))
	case 8:
		return binary.LittleEndian.Uint64(src)
	default:
		var payload uint64
		for i := range size {
			payload |= uint64(src[i]) << (i * 8)
		}
		return payload
	}
}

// fingerprintMixer is the WyHash wyp1 constant used to mix k0 and k1 for
// fingerprint extraction. Being odd, multiplication by this constant is a
// bijection on uint64, ensuring the mix preserves entropy from both inputs.
const fingerprintMixer = 0x517cc1b727220a95

// extractFingerprint computes a fingerprint from both hash halves using a mixer.
//
// The mixer `k0 ^ (k1 * C)` is provably uniform: if either k0 or k1 is
// uniformly distributed and independent of the other, the XOR output is
// uniform. This follows from the XOR uniformity lemma — for independent
// random variables X and Y where X is uniform, X^Y is uniform regardless
// of Y's distribution. Since C is odd, k1*C is a bijection on uint64,
// preserving k1's uniformity.
//
// We extract from the high 32 bits (>> 32) because the low bits of k0 are
// constrained by block assignment: the BigEndian prefix (bytes 0-7) maps to
// blocks via FastRange32, which constrains k0's low byte (key[0], the LSB
// in LE and MSB of the big-endian prefix). k0's high bits are not free either,
// since Bijection picks buckets from them. k1 reaches the slot only through a
// mix that picks among a bucket's few slots, so k1*C stays nearly independent
// of it; tests measure the result at chance (spec §2.5).
//
// This unified extraction replaces the previous per-algorithm approach where
// bijection used k1>>32 and PTRHash used k0>>32. The mixer is algorithm-
// independent, eliminating a class of bugs where swapping k0/k1 was
// undetectable.
func extractFingerprint(k0, k1 uint64, fpSize int) uint32 {
	if fpSize == 0 {
		return 0
	}
	h := k0 ^ (k1 * fingerprintMixer)
	fp := uint32(h >> 32)
	mask := uint32((uint64(1) << (fpSize * 8)) - 1)
	return fp & mask
}

// Fingerprint returns the 32-bit fingerprint streamhash computes for key, used
// to reduce false positives. An index built WithFingerprint(n) stores its low n
// bytes. If you store data for each key yourself, outside the index, you can
// store the fingerprint with it and check it on lookup.
//
// Returns ErrKeyTooShort if key is shorter than MinKeySize.
func Fingerprint(key []byte) (uint32, error) {
	if len(key) < MinKeySize {
		return 0, ErrKeyTooShort
	}
	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	return extractFingerprint(k0, k1, maxFingerprintSize), nil
}
