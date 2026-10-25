//go:build linux

package streamhash

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// openDirect opens path for reading with O_DIRECT, so reading a file once
// does not fill the page cache. A filesystem that refuses O_DIRECT gets a
// plain open; reads into aligned buffers at aligned offsets work either way.
func openDirect(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|unix.O_DIRECT, 0)
	if errors.Is(err, unix.EINVAL) {
		return os.Open(path)
	}
	return f, err
}
