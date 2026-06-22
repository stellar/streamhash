//go:build !linux && !darwin

package streamhash

import "os"

// preallocFile extends the file via Truncate — the fallback for platforms
// without fallocate/F_PREALLOCATE (other Unixes, Windows).
func preallocFile(f *os.File, size int64) error {
	return f.Truncate(size)
}
