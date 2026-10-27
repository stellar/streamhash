//go:build !linux

package streamhash

import "os"

// openDirect opens path for reading. Off Linux there is no O_DIRECT, so the
// reads go through the page cache.
func openDirect(path string) (*os.File, error) {
	return os.Open(path)
}
