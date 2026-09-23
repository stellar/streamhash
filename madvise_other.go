//go:build !linux && !darwin

package streamhash

// No madvise on this platform; read-around stays at the OS default.
func adviseRandom([]byte) error { return nil }

func adviseSequential([]byte) error { return nil }
