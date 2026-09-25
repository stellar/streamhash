//go:build linux || darwin

package streamhash

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// adviseRandom marks the mapping random-access: a fault reads only the page touched.
func adviseRandom(mapping []byte) error {
	if len(mapping) == 0 {
		return nil
	}
	if err := unix.Madvise(mapping, unix.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise MADV_RANDOM: %w", err)
	}
	return nil
}

// adviseSequential marks the mapping sequential-access, restoring read-around for a walk.
func adviseSequential(mapping []byte) error {
	if len(mapping) == 0 {
		return nil
	}
	if err := unix.Madvise(mapping, unix.MADV_SEQUENTIAL); err != nil {
		return fmt.Errorf("madvise MADV_SEQUENTIAL: %w", err)
	}
	return nil
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
