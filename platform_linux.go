//go:build linux

package streamhash

import (
	"os"

	"golang.org/x/sys/unix"
)

// preallocFile pre-allocates disk blocks and extends the file to the given size.
func preallocFile(f *os.File, size int64) error {
	return unix.Fallocate(int(f.Fd()), 0, 0, size)
}
