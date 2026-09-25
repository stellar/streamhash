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

const canAdvise = true

// adviseWillNeed starts reading mapping[start:end]. Linux reads at most
// max(read_ahead_kb, max_sectors_kb) per call, at least 128 KiB on common
// disks, so it asks in 128 KiB pieces.
func adviseWillNeed(mapping []byte, start, end uint64) {
	const piece = 128 << 10
	for s := start &^ uint64(unix.Getpagesize()-1); s < end; s += piece {
		_ = unix.Madvise(mapping[s:min(s+piece, end)], unix.MADV_WILLNEED)
	}
}
