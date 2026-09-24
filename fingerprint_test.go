package streamhash

import (
	"encoding/binary"
	"errors"
	"testing"
)

// The index stores the low n bytes of Fingerprint, for every width.
func TestFingerprintMatchesStored(t *testing.T) {
	keys := generateRandomKeys(newTestRNG(t), 2000, MinKeySize)
	for n := 1; n <= maxFingerprintSize; n++ {
		idx := buildAndOpenUnsorted(t, keys, nil, t.TempDir(), WithFingerprint(n))
		mask := uint32(uint64(1)<<(8*n) - 1)
		for _, key := range keys {
			rank, err := idx.QueryRank(key)
			if err != nil {
				t.Fatal(err)
			}
			fp, _ := Fingerprint(key)
			off := idx.payloadRegionOffset + rank*uint64(idx.entrySize)
			if stored := unpackFingerprintFromBytes(idx.data[off:], n); stored != fp&mask {
				t.Fatalf("n=%d: stored 0x%X, Fingerprint 0x%X", n, stored, fp&mask)
			}
		}
		idx.Close()
	}
}

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

// A key outside the build set matches its slot owner's fingerprint only by
// chance, under both algorithms. Raw key bytes 0 and 7 are a positive control:
// routing and Bijection's bucket pin them.
func TestFingerprintIndependentOfSlot(t *testing.T) {
	if testing.Short() {
		t.Skip("samples 600k lookups")
	}
	const members, probes = 100_000, 300_000
	for _, algo := range []Algorithm{AlgoBijection, AlgoPTRHash} {
		t.Run(algo.String(), func(t *testing.T) {
			rng := newTestRNG(t)
			keys := generateRandomKeys(rng, members, MinKeySize)
			idx := buildAndOpenUnsorted(t, keys, nil, t.TempDir(), WithAlgorithm(algo))
			defer idx.Close()
			owner := make([][]byte, members)
			for _, key := range keys {
				rank, err := idx.QueryRank(key)
				if err != nil {
					t.Fatal(err)
				}
				owner[rank] = key
			}

			var collisions, joint int
			var fpByte [4]int
			var raw [MinKeySize]int
			probe := make([]byte, MinKeySize)
			for range probes {
				fillFromRNG(rng, probe)
				rank, err := idx.QueryRank(probe)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				collisions++
				ours, _ := Fingerprint(probe)
				theirs, _ := Fingerprint(owner[rank])
				for b := range fpByte {
					if byte(ours>>(8*b)) == byte(theirs>>(8*b)) {
						fpByte[b]++
					}
				}
				if uint16(ours) == uint16(theirs) {
					joint++
				}
				for b := range raw {
					if probe[b] == owner[rank][b] {
						raw[b]++
					}
				}
			}

			chance := float64(collisions) / 256
			for b, n := range fpByte {
				if float64(n) > 1.5*chance {
					t.Errorf("fingerprint byte %d matches %.1fx chance", b, float64(n)/chance)
				}
			}
			if want := float64(collisions) / 65536; float64(joint) > 10*want {
				t.Errorf("two-byte matches: %d, want ~%.0f", joint, want)
			}
			if float64(raw[0]) < 2*chance {
				t.Errorf("control: raw byte 0 matches only %.1fx chance", float64(raw[0])/chance)
			}
			if algo == AlgoBijection && float64(raw[7]) < 32*chance {
				t.Errorf("control: raw byte 7 matches only %.1fx chance", float64(raw[7])/chance)
			}
		})
	}
}
