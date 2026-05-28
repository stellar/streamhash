package bijection

import (
	"fmt"
	"testing"
)

// TestEncodeFallbackMarker verifies encodeFallbackMarker writes exactly 16 one-bits.
func TestEncodeFallbackMarker(t *testing.T) {
	encoder := newSeedStreamEncoder()

	// Check bitsWritten before and after
	beforeBits := encoder.bw.bitsWritten()
	encoder.encodeFallbackMarker()
	afterBits := encoder.bw.bitsWritten()

	bitsWritten := afterBits - beforeBits
	if bitsWritten != 16 {
		t.Errorf("expected 16 bits written, got %d", bitsWritten)
	}

	data := encoder.bytes()

	// The fallback marker should be 16 one-bits = 0xFFFF as little-endian
	if len(data) != 2 {
		t.Errorf("expected 2 bytes, got %d", len(data))
	} else if data[0] != 0xFF || data[1] != 0xFF {
		t.Errorf("expected data [ff ff], got %x", data)
	}
}

// TestVerifyNoFallbackDuplicates verifies that the builder never creates duplicate fallback entries.
func TestVerifyNoFallbackDuplicates(t *testing.T) {
	globalSeed := uint64(testSeed1)
	builder := NewBuilder(100000, globalSeed, 0, 0)
	rng := newTestRNG(t)

	// Simulate building multiple blocks
	for block := range 20 {
		builder.Reset()

		// Add keys to this block (varying sizes up to ~3072, the natural block size)
		numKeys := 2000 + block*50
		for i := range numKeys {
			k0 := rng.Uint64()
			k1 := rng.Uint64()
			builder.AddKey(k0, k1, uint64(i), 0)
		}

		// Build
		metadataBuf := make([]byte, estimateMetadataSize(builder.KeysAdded())+1000)
		payloadsBuf := make([]byte, builder.KeysAdded()*5)
		_, _, _, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
		if err != nil {
			t.Fatalf("Block %d build failed: %v", block, err)
		}

		// Verify no duplicates
		err = buildAndVerifyNoDuplicates(builder)
		if err != nil {
			t.Errorf("Block %d: %v", block, err)
		}
	}
}

// buildAndVerifyNoDuplicates checks that the fallback list has no duplicate entries
func buildAndVerifyNoDuplicates(bb *Builder) error {
	seen := make(map[uint32]bool) // (bucketIndex << 8) | subBucket
	for _, fe := range bb.fallbackList {
		key := (uint32(fe.bucketIndex) << 8) | uint32(fe.subBucket)
		if seen[key] {
			return fmt.Errorf("duplicate fallback entry for bucket %d", fe.bucketIndex)
		}
		seen[key] = true
	}
	return nil
}

// TestFallbackListFromMetadata verifies that fallback entries encoded by the builder
// can be resolved by the production decoder, and that no duplicate entries exist.
// Tests both payload+fingerprint and MPHF-only configurations.
func TestFallbackListFromMetadata(t *testing.T) {
	configs := []struct {
		name        string
		numKeys     int
		payloadSize int
		fpSize      int
	}{
		{"with_payload_fp", 3072, 4, 1},
		{"mphf_only", 3000, 0, 0},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			rng := newTestRNG(t)

			// Some seeds produce no fallback entries, especially in MPHF-only mode.
			// Try multiple globalSeeds until we find one that generates fallbacks.
			var builder *Builder
			var metadata []byte
			var globalSeed uint64
			const maxAttempts = 20
			for range maxAttempts {
				globalSeed = rng.Uint64()
				builder = NewBuilder(100000, globalSeed, cfg.payloadSize, cfg.fpSize)

				for i := 0; i < cfg.numKeys; i++ {
					k0 := rng.Uint64()
					k1 := rng.Uint64()
					var fp uint32
					if cfg.fpSize > 0 {
						fp = uint32(i & 0xFF)
					}
					builder.AddKey(k0, k1, uint64(i), fp)
				}

				metadataBuf := make([]byte, estimateMetadataSize(builder.KeysAdded())+1000)
				payloadsBuf := make([]byte, builder.KeysAdded()*5)
				metaLen, _, _, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
				if err != nil {
					t.Fatalf("Build failed: %v", err)
				}
				metadata = metadataBuf[:metaLen]

				if len(builder.fallbackList) > 0 {
					break
				}
				builder.Reset()
			}

			if len(builder.fallbackList) == 0 {
				t.Fatal("No fallback entries generated after 20 attempts")
			}

			// Check for duplicates in builder's fallback list
			if err := buildAndVerifyNoDuplicates(builder); err != nil {
				t.Errorf("Duplicate check failed: %v", err)
			}

			// Verify each fallback entry resolves correctly via production decoder
			efDataLen := eliasFanoSize(bucketsPerBlock, builder.KeysAdded())
			seedStreamOffset := checkpointsSize + efDataLen
			decoder, err := NewDecoder(nil, globalSeed)
			if err != nil {
				t.Fatalf("NewDecoder failed: %v", err)
			}

			for i, fe := range builder.fallbackList {
				resolved0, resolved1 := decoder.resolveFallbackSeeds(
					metadata, seedStreamOffset, int(fe.bucketIndex), fallbackMarker, fallbackMarker,
				)
				var resolved uint32
				if fe.subBucket == 0 {
					resolved = resolved0
				} else {
					resolved = resolved1
				}
				if resolved != fe.seed {
					t.Errorf("Entry %d: bucket %d subBucket %d: builder seed=%d, resolved=%d",
						i, fe.bucketIndex, fe.subBucket, fe.seed, resolved)
				}
			}
		})
	}
}

// TestFallbackListRoundtrip verifies encode/decode of random fallback lists.
func TestFallbackListRoundtrip(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	seedBits := 31 - blockBits
	seedMask := uint32((1 << seedBits) - 1)

	for i := range iterations {
		n := 1 + rng.IntN(20)
		entries := make([]fallbackEntry, n)
		for j := range entries {
			entries[j] = fallbackEntry{
				bucketIndex: uint16(rng.IntN(bucketsPerBlock)),
				subBucket:   uint8(rng.IntN(2)),
				seed:        rng.Uint32() & seedMask,
			}
		}

		var buf []byte
		encoded := encodeFallbackListIntoWithB(entries, blockBits, &buf)
		decoded := decodeFallbackListWithB(encoded, blockBits)

		if len(decoded) != len(entries) {
			t.Fatalf("iter %d: len mismatch: got %d, want %d", i, len(decoded), len(entries))
		}

		for j := range entries {
			if decoded[j].bucketIndex != entries[j].bucketIndex {
				t.Fatalf("iter %d entry %d: bucketIndex got %d, want %d",
					i, j, decoded[j].bucketIndex, entries[j].bucketIndex)
			}
			if decoded[j].subBucket != entries[j].subBucket {
				t.Fatalf("iter %d entry %d: subBucket got %d, want %d",
					i, j, decoded[j].subBucket, entries[j].subBucket)
			}
			if decoded[j].seed != entries[j].seed {
				t.Fatalf("iter %d entry %d: seed got %d, want %d",
					i, j, decoded[j].seed, entries[j].seed)
			}
		}
	}
}

// TestFallbackListBoundaries tests the count=0 (empty list) and count=255
// (maximum capacity) boundaries of the 1-byte count field in fallback encoding.
func TestFallbackListBoundaries(t *testing.T) {
	seedBits := 31 - blockBits
	seedMask := uint32((1 << seedBits) - 1)

	t.Run("count=0", func(t *testing.T) {
		// The decoder returns nil for count=0 (no entries to decode).
		// Encode a buffer with count byte = 0 and a valid validation byte.
		data := []byte{0, 0 ^ fallbackValidationXOR}
		decoded := decodeFallbackListWithB(data, blockBits)
		if decoded != nil {
			t.Errorf("expected nil for count=0, got %d entries", len(decoded))
		}
	})

	t.Run("count=255", func(t *testing.T) {
		rng := newTestRNG(t)
		entries := make([]fallbackEntry, maxFallbackEntries)
		for j := range entries {
			entries[j] = fallbackEntry{
				bucketIndex: uint16(rng.IntN(bucketsPerBlock)),
				subBucket:   uint8(rng.IntN(2)),
				seed:        rng.Uint32() & seedMask,
			}
		}

		var buf []byte
		encoded := encodeFallbackListIntoWithB(entries, blockBits, &buf)
		decoded := decodeFallbackListWithB(encoded, blockBits)

		if len(decoded) != maxFallbackEntries {
			t.Fatalf("len mismatch: got %d, want %d", len(decoded), maxFallbackEntries)
		}
		for j := range entries {
			if decoded[j].bucketIndex != entries[j].bucketIndex ||
				decoded[j].subBucket != entries[j].subBucket ||
				decoded[j].seed != entries[j].seed {
				t.Fatalf("entry %d mismatch: got {%d, %d, %d}, want {%d, %d, %d}",
					j,
					decoded[j].bucketIndex, decoded[j].subBucket, decoded[j].seed,
					entries[j].bucketIndex, entries[j].subBucket, entries[j].seed)
			}
		}
	})
}

// TestResolveFallbackSeedsMaxEntries verifies that the backward scan in
// resolveFallbackSeeds LOCATES a maximum-size (255-entry) fallback list. Unlike
// TestFallbackListBoundaries, which decodes a list whose start is already known,
// this exercises the scan that finds the list start. A 255-entry list begins
// exactly maxFallbackSize bytes from the end, so the scan's lower bound must be
// inclusive — a strict bound skips it and leaves the seeds unresolved.
func TestResolveFallbackSeedsMaxEntries(t *testing.T) {
	seedBits := 31 - blockBits
	seedMask := uint32((1 << seedBits) - 1)

	entries := make([]fallbackEntry, maxFallbackEntries) // 255
	for j := range entries {
		entries[j] = fallbackEntry{
			bucketIndex: uint16(j),              // distinct buckets 0..254
			subBucket:   0,                      //
			seed:        uint32(j+1) & seedMask, // distinct, non-marker seeds
		}
	}

	var buf []byte
	fallbackData := encodeFallbackListIntoWithB(entries, blockBits, &buf)

	// Place the list at the end of the metadata, after a non-empty seed-stream
	// prefix, so resolveFallbackSeeds must scan back to find it. The seed stream
	// starts at offset 0, so seedStreamOffset = 0.
	seedStream := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	metadata := append(append([]byte(nil), seedStream...), fallbackData...)

	decoder, err := NewDecoder(nil, 0)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}

	// Resolve an entry deep in the list to exercise the full scan + decode.
	target := entries[200]
	got0, _ := decoder.resolveFallbackSeeds(metadata, 0, int(target.bucketIndex), fallbackMarker, fallbackMarker)
	if got0 != target.seed {
		t.Fatalf("255-entry fallback list not located: bucket %d resolved to %d (fallbackMarker=%d), want %d",
			target.bucketIndex, got0, fallbackMarker, target.seed)
	}
}
