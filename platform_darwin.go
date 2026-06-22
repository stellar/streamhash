//go:build darwin

package streamhash

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocFile pre-allocates disk blocks without changing the file size.
// On macOS, uses fcntl F_PREALLOCATE to reserve contiguous blocks.
// No ftruncate: the file size stays at 0 and pwrite extends it incrementally.
// With chunk-split files (~683 regions per chunk), pwrite costs ~2-3μs/call
// on sparse pre-allocated files. ftruncate would be slightly faster (~1.7μs)
// but causes page cache pressure proportional to file size, which thrashes
// under memory pressure.
func preallocFile(f *os.File, size int64) error {
	fst := unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	return unix.FcntlFstore(f.Fd(), unix.F_PREALLOCATE, &fst)
}
