package streamhash

import (
	"encoding/binary"
	"errors"
	"testing"
)

// Byte order and width, pinned to TestExtractFingerprintKnownValues' tuples.
func TestFingerprintReadsKey(t *testing.T) {
	for _, tc := range []struct {
		k0, k1 uint64
		want   uint32
	}{
		{0xDEADBEEFCAFEBABE, 0x0, 0xDEADBEEF},
		{0x123456789ABCDEF0, 0xFEDCBA9876543210, 0x4582A5E3},
		{0x0, 0xFFFFFFFFFFFFFFFF, 0xAE833E48},
	} {
		key := make([]byte, MinKeySize+4) // bytes past MinKeySize are ignored
		binary.LittleEndian.PutUint64(key[0:], tc.k0)
		binary.LittleEndian.PutUint64(key[8:], tc.k1)
		copy(key[MinKeySize:], "tail")
		for _, k := range [][]byte{key[:MinKeySize], key} {
			if fp, err := Fingerprint(k); err != nil || fp != tc.want {
				t.Fatalf("Fingerprint = 0x%X, %v; want 0x%X", fp, err, tc.want)
			}
		}
	}
	if _, err := Fingerprint(make([]byte, MinKeySize-1)); !errors.Is(err, ErrKeyTooShort) {
		t.Fatalf("short key: got %v, want ErrKeyTooShort", err)
	}
}
