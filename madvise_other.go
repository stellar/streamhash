//go:build !linux && !darwin

package streamhash

// No madvise on this platform; read-around stays at the OS default.
func adviseRandom([]byte) {}

func adviseSequential([]byte) {}

const canAdvise = false

func adviseWillNeed([]byte, uint64, uint64) {}
