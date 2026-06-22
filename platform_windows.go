//go:build windows

package streamhash

import "os"

// fileFlagDeleteOnClose is Win32 FILE_FLAG_DELETE_ON_CLOSE; os.OpenFile forwards
// high flag bits to CreateFile, so the OS deletes the file on last-handle close.
const fileFlagDeleteOnClose = 0x04000000

func openWriterFile(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_RDWR|os.O_CREATE|fileFlagDeleteOnClose, 0600)
}

// unlinkWhileOpen is a no-op: openWriterFile already set DELETE_ON_CLOSE.
func unlinkWhileOpen(string) error { return nil }
