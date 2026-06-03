package streamhash

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// This file fuzzes the reader's robustness to corrupt / untrusted index bytes.
// The contract (enshrined by TestCorruptionDetection) is that a corrupt index
// must yield ErrCorruptedIndex or a safe-but-undefined result -- it must NEVER
// panic. Round 1 of the audit found three latent panics in this class by hand
// (NumBlocks wrap, non-monotonic KeysBefore, ptrhash remap OOB); this harness
// hunts the rest by mutating real index bytes and driving every decode path.
//
// Only a panic fails the test. Wrong-but-safe query results on corrupt input
// are explicitly allowed (the index makes no correctness promise without an
// explicit Verify()), so there are no false positives from result mismatches.

type fuzzBaseIndex struct {
	data       []byte
	keys       [][]byte
	hasPayload bool
	label      string
}

var (
	fuzzBasesOnce sync.Once
	fuzzBases     []fuzzBaseIndex
)

// buildFuzzBaseIndex builds a valid index with the given parameters and returns
// its on-disk bytes plus the keys it contains.
func buildFuzzBaseIndex(dir, name string, keys [][]byte, algo Algorithm, payload, fp int) (fuzzBaseIndex, error) {
	opts := []BuildOption{WithAlgorithm(algo)}
	if payload > 0 {
		opts = append(opts, WithPayload(payload))
	}
	if fp > 0 {
		opts = append(opts, WithFingerprint(fp))
	}

	sortKeysByBlock(keys, uint64(len(keys)), opts)

	path := filepath.Join(dir, name)
	b, err := NewSortedBuilder(context.Background(), path, uint64(len(keys)), opts...)
	if err != nil {
		return fuzzBaseIndex{}, err
	}
	for i, k := range keys {
		pv := uint64(0)
		if payload > 0 {
			pv = uint64(i)
		}
		if err := b.AddKey(k, pv); err != nil {
			b.Close()
			return fuzzBaseIndex{}, err
		}
	}
	if err := b.Finish(); err != nil {
		return fuzzBaseIndex{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fuzzBaseIndex{}, err
	}
	return fuzzBaseIndex{
		data:       data,
		keys:       keys,
		hasPayload: payload > 0,
		label:      fmt.Sprintf("%s/p%d/fp%d", algo, payload, fp),
	}, nil
}

// fuzzBaseIndexes lazily builds a small set of valid base indexes spanning both
// algorithms and the payload/fingerprint decode paths. Corruption is layered on
// top of these to drive Open/Query/Verify through real code paths.
func fuzzBaseIndexes() []fuzzBaseIndex {
	fuzzBasesOnce.Do(func() {
		dir, err := os.MkdirTemp("", "sh-corrupt-fuzz")
		if err != nil {
			panic(err)
		}
		configs := []struct {
			algo        Algorithm
			payload, fp int
		}{
			{AlgoBijection, 4, 1}, // default-style: payload + fingerprint
			{AlgoPTRHash, 4, 1},
			{AlgoBijection, 0, 0}, // MPHF-only: no payload, no fingerprint
			{AlgoPTRHash, 8, 2},   // wide payload + multi-byte fingerprint
		}
		rng := rand.New(rand.NewPCG(0x5eed, 0x1dea))
		for i, c := range configs {
			keys := generateRandomKeys(rng, 500, 16)
			base, err := buildFuzzBaseIndex(dir, fmt.Sprintf("base%d.idx", i), keys, c.algo, c.payload, c.fp)
			if err != nil {
				panic(fmt.Sprintf("build base %d (%v): %v", i, c, err))
			}
			fuzzBases = append(fuzzBases, base)
		}
	})
	return fuzzBases
}

// exerciseCorruptIndex drives every reader decode path over the given (possibly
// corrupt) bytes. It must never panic; any panic is reported by the caller's
// recover. Errors and wrong results are acceptable.
func exerciseCorruptIndex(data []byte, keys [][]byte, hasPayload bool) {
	idx, err := OpenBytes(data)
	if err == nil {
		// Verify must terminate with an error or nil -- never panic.
		_ = idx.Verify()
		for i := 0; i < len(keys) && i < 48; i++ {
			_, _ = idx.QueryRank(keys[i])
		}
		_, _ = idx.QueryRank(make([]byte, 16)) // non-member probe
		idx.Close()
	}

	if hasPayload {
		if pidx, err := OpenPayloadBytes(data); err == nil {
			for i := 0; i < len(keys) && i < 48; i++ {
				_, _, _ = pidx.QueryPayload(keys[i])
			}
			_, _, _ = pidx.QueryPayload(make([]byte, 16))
			pidx.Close()
		}
	}
}

// FuzzOpenCorruptIndex mutates valid index bytes (sparse byte edits + optional
// truncation) and asserts the reader never panics. This is the systematic
// continuation of the round-1 untrusted-input findings.
func FuzzOpenCorruptIndex(f *testing.F) {
	bases := fuzzBaseIndexes()

	// Seed corpus: target the structurally interesting regions (header fields
	// near the front, plus a mid-file edit), and a truncation.
	f.Add(0, -1, []byte{14, 0, 0, 0xFF})                  // bijection: flip a NumBlocks byte
	f.Add(1, -1, []byte{14, 0, 0, 0xFF})                  // ptrhash:   flip a NumBlocks byte
	f.Add(3, -1, []byte{22, 0, 0, 0xFF})                  // flip the FingerprintSize byte
	f.Add(0, -1, []byte{0, 0, 0, 0x01})                   // flip a magic byte
	f.Add(2, 304, []byte{})                               // truncate to minFileSize-ish
	f.Add(1, -1, []byte{100, 0, 0, 0x7F, 50, 1, 0, 0x33}) // two edits deeper in the file

	f.Fuzz(func(t *testing.T, baseSel int, truncTo int, edits []byte) {
		base := bases[((baseSel%len(bases))+len(bases))%len(bases)]

		data := make([]byte, len(base.data))
		copy(data, base.data)

		if truncTo >= 0 && truncTo < len(data) {
			data = data[:truncTo]
		}

		// Apply sparse byte edits: each 4-byte group is (offset[0:3], xorMask).
		if len(data) > 0 {
			for i := 0; i+3 < len(edits); i += 4 {
				off := (int(edits[i]) | int(edits[i+1])<<8 | int(edits[i+2])<<16) % len(data)
				data[off] ^= edits[i+3]
			}
		}

		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reader panicked on corrupt input (base=%s, trunc=%d, len=%d): %v",
					base.label, truncTo, len(data), r)
			}
		}()

		exerciseCorruptIndex(data, base.keys, base.hasPayload)
	})
}
