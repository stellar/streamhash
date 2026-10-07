//go:build linux || darwin

package streamhash

import "golang.org/x/sys/unix"

// Advice is a hint, so errors are ignored: a failure (e.g. EAGAIN when the
// mapping split hits vm.max_map_count) only leaves the default read-around.

// adviseRandom marks the mapping random-access: a fault reads only the page touched.
func adviseRandom(mapping []byte) {
	if len(mapping) > 0 {
		_ = unix.Madvise(mapping, unix.MADV_RANDOM)
	}
}

// adviseSequential marks the mapping sequential-access, restoring read-around for a walk.
func adviseSequential(mapping []byte) {
	if len(mapping) > 0 {
		_ = unix.Madvise(mapping, unix.MADV_SEQUENTIAL)
	}
}
