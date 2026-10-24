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

// startWriteback starts writing the file's dirty pages to disk without waiting
// for them. It is advisory, so an error is ignored: the final Sync still makes
// the file durable.
func startWriteback(f *os.File) {
	_ = unix.SyncFileRange(int(f.Fd()), 0, 0, unix.SYNC_FILE_RANGE_WRITE)
}
