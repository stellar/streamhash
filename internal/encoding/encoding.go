// Package encoding provides serialization utilities for fingerprints and payloads.
//
// Both WriteEntry and WriteEntryGeneric use unsafe native-endian writes and
// are only correct on little-endian architectures (amd64, arm64).
package encoding

import "unsafe"

// WriteEntry packs a fingerprint and payload into a little-endian entry of
// entrySize bytes at position pos in the buffer starting at basePtr.
// The entry is stored as: fp | (payload << fpShift), where fpShift is
// fingerprintSize * 8. Callers should compute fpShift once before the loop.
//
// Only supports entrySize 1, 4, 5, and 8; panics for other sizes.
// Use WriteEntryGeneric for arbitrary entry sizes. WriteEntry does not
// delegate to WriteEntryGeneric in the default case because adding that
// call pushes the function over the compiler's inlining budget,
// preventing inlining and causing a ~6x regression in the fast paths.
// When inlined at call sites that pass a constant entrySize, the compiler
// eliminates the unused switch branches.
//
// Inlining budget: current cost 71, budget 80. Adding another case would
// likely exceed the budget and silently de-inline all paths.
func WriteEntry(basePtr unsafe.Pointer, pos, entrySize int, fp uint32, payload uint64, fpShift uint) {
	val := uint64(fp) | (payload << fpShift)
	switch entrySize {
	case 1:
		*(*uint8)(unsafe.Add(basePtr, pos)) = uint8(val)
	case 4:
		*(*uint32)(unsafe.Add(basePtr, pos*4)) = uint32(val)
	case 5:
		ptr := unsafe.Add(basePtr, pos*5)
		*(*uint32)(ptr) = uint32(val)
		*(*uint8)(unsafe.Add(ptr, 4)) = uint8(val >> 32)
	case 8:
		*(*uint64)(unsafe.Add(basePtr, pos*8)) = val
	default:
		panic("encoding: WriteEntry: unsupported entrySize")
	}
}

// ReadFP reads a little-endian fingerprint of fpSize bytes from buf.
// This is the safe read counterpart to the fp portion of WriteEntry/WriteEntryGeneric.
//
// ReadFP, ReadPayload, and ReadEntry exist to verify the WriteEntry round-trip
// in tests; the production query path reads fingerprints/payloads via the
// streamhash package's own unpack helpers, not these.
func ReadFP(buf []byte, fpSize int) uint32 {
	var v uint32
	for i := range fpSize {
		v |= uint32(buf[i]) << (i * 8)
	}
	return v
}

// ReadPayload reads a little-endian payload of payloadSize bytes from buf.
// This is the safe read counterpart to the payload portion of WriteEntry/WriteEntryGeneric.
func ReadPayload(buf []byte, payloadSize int) uint64 {
	var v uint64
	for i := range payloadSize {
		v |= uint64(buf[i]) << (i * 8)
	}
	return v
}

// ReadEntry reads a fingerprint and payload from buf at the given slot position.
// This is the safe read counterpart to WriteEntry/WriteEntryGeneric.
func ReadEntry(buf []byte, slot, entrySize, fpSize, payloadSize int) (uint32, uint64) {
	offset := slot * entrySize
	fp := ReadFP(buf[offset:], fpSize)
	payload := ReadPayload(buf[offset+fpSize:], payloadSize)
	return fp, payload
}

// WriteEntryGeneric packs a fingerprint and payload into a little-endian entry
// of arbitrary size at position pos in the buffer starting at basePtr.
// Same calling convention as WriteEntry but handles all entry sizes.
// Prefer WriteEntry for sizes 1, 4, 5, and 8 — it uses wider writes and is
// inlineable, so the compiler can eliminate dead branches when entrySize is
// a constant.
func WriteEntryGeneric(basePtr unsafe.Pointer, pos, entrySize int, fp uint32, payload uint64, fpShift uint) {
	fpSize := int(fpShift / 8)
	ptr := unsafe.Add(basePtr, pos*entrySize)

	// Write fingerprint (optimized for common sizes)
	switch fpSize {
	case 0:
		// no fingerprint
	case 1:
		*(*uint8)(ptr) = uint8(fp)
	case 2:
		*(*uint16)(ptr) = uint16(fp)
	case 4:
		*(*uint32)(ptr) = fp
	default:
		for i := range fpSize {
			*(*uint8)(unsafe.Add(ptr, i)) = uint8(fp >> (i * 8))
		}
	}

	// Write payload (optimized for common sizes)
	payloadSize := entrySize - fpSize
	if payloadSize == 0 {
		return
	}
	payloadPtr := unsafe.Add(ptr, fpSize)
	switch payloadSize {
	case 4:
		*(*uint32)(payloadPtr) = uint32(payload)
	case 8:
		*(*uint64)(payloadPtr) = payload
	default:
		for i := range payloadSize {
			*(*uint8)(unsafe.Add(payloadPtr, i)) = uint8(payload >> (i * 8))
		}
	}
}
