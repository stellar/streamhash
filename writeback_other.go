//go:build !linux

package streamhash

import "os"

// startWriteback is a no-op off Linux, which has no sync_file_range; the
// kernel writes the pages back on its own schedule.
func startWriteback(*os.File) {}
