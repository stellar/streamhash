package streamhash

import "unsafe"

// directAlign is the alignment direct reads need of the buffer's address,
// the file offset and the length.
const directAlign = 4096

// alignedBuffer returns a size-byte slice aligned to directAlign. Go's GC does
// not move it, so the alignment holds.
func alignedBuffer(size int) []byte {
	backing := make([]byte, size+directAlign)
	misalign := int(uintptr(unsafe.Pointer(&backing[0])) % directAlign)
	off := (directAlign - misalign) % directAlign
	return backing[off : off+size : off+size]
}
