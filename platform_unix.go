//go:build !windows

package streamhash

import "os"

func openWriterFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
}

// unlinkWhileOpen unlinks the still-open file (POSIX anonymous-file idiom).
func unlinkWhileOpen(path string) error {
	return os.Remove(path)
}
