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
