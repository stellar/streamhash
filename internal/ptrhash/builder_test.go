package ptrhash

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stellar/streamhash/internal/encoding"
	"github.com/stellar/streamhash/internal/sherr"
)

// NOTE: TestEstimateMetadataSizeIsUpperBound, TestEmptyBlockMetadataSize,
// TestEstimateMetadataSizeRandomBlocks, TestBlockOverflowReturnsError,
// TestPayloadRoundtripAllSizes, and TestBuilderReset are mirrored in
// internal/bijection/builder_test.go.

// TestEstimateMetadataSizeIsUpperBound verifies that estimateMetadataSize
// returns a value >= the actual encoded metadata size for all cases.
func TestEstimateMetadataSizeIsUpperBound(t *testing.T) {
	testCases := []struct {
		name    string
		numKeys int
	}{
		{"empty_block", 0},
		{"single_key", 1},
		{"small_block", 10},
		{"medium_block", 100},
		{"typical_block", 500},
		{"large_block", 1000},
		{"very_large_block", 2000},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			estimate := estimateMetadataSize(tc.numKeys)

			// Build an actual block and measure the real size
			builder := NewBuilder(10000, testSeed1, 4, 1)

			// Add keys if non-empty
			rng := newTestRNG(t)
			for i := 0; i < tc.numKeys; i++ {
				k0 := rng.Uint64()
				k1 := rng.Uint64()
				builder.AddKey(k0, k1, uint64(i), uint32(i&0xFF))
			}

			// Build into buffers
			metadataBuf := make([]byte, estimate+1000) // Extra space to detect overflow
			payloadsBuf := make([]byte, tc.numKeys*5+1000)

			metaLen, _, _, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
			if err != nil {
				t.Fatalf("BuildSeparatedInto failed: %v", err)
			}

			if metaLen > estimate {
				t.Errorf("estimateMetadataSize(%d) = %d, but actual size was %d (estimate too small!)",
					tc.numKeys, estimate, metaLen)
			}
		})
	}
}

// TestEmptyBlockMetadataSize tests the empty block case.
func TestEmptyBlockMetadataSize(t *testing.T) {
	estimate := estimateMetadataSize(0)

	// For ptrhash, empty block needs:
	// - pilots: bucketsPerBlock bytes
	// - remap table: 2 bytes (empty count)
	minExpected := int(bucketsPerBlock) + 2

	if estimate < minExpected {
		t.Errorf("estimateMetadataSize(0) = %d, but minimum required is %d", estimate, minExpected)
	}

	// Actually build an empty block to verify
	builder := NewBuilder(10000, testSeed1, 4, 1)
	metadataBuf := make([]byte, estimate)
	payloadsBuf := make([]byte, 0)

	metaLen, _, numKeys, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
	if err != nil {
		t.Fatalf("BuildSeparatedInto for empty block failed: %v", err)
	}

	if numKeys != 0 {
		t.Errorf("expected 0 keys, got %d", numKeys)
	}

	if metaLen > estimate {
		t.Errorf("empty block metadata %d bytes exceeds estimate %d", metaLen, estimate)
	}
}

// TestEstimateMetadataSizeRandomBlocks tests with random key distributions.
func TestEstimateMetadataSizeRandomBlocks(t *testing.T) {
	globalSeeds := []uint64{47, 46, 12345, 999999}

	rng := newTestRNG(t)
	for _, globalSeed := range globalSeeds {
		for trial := range 10 {
			numKeys := rng.IntN(2000) + 1 // 1 to 2000 keys

			estimate := estimateMetadataSize(numKeys)
			builder := NewBuilder(10000, globalSeed, 4, 1)

			for i := range numKeys {
				k0 := rng.Uint64()
				k1 := rng.Uint64()
				builder.AddKey(k0, k1, uint64(i), uint32(i&0xFF))
			}

			metadataBuf := make([]byte, estimate)
			payloadsBuf := make([]byte, numKeys*5)

			metaLen, _, _, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
			if err != nil {
				t.Fatalf("globalSeed=%d trial=%d numKeys=%d: BuildSeparatedInto failed: %v",
					globalSeed, trial, numKeys, err)
			}

			if metaLen > estimate {
				t.Errorf("globalSeed=%d trial=%d numKeys=%d: actual %d > estimate %d",
					globalSeed, trial, numKeys, metaLen, estimate)
			}
		}
	}
}

// TestBlockOverflowReturnsError verifies that BuildSeparatedInto returns
// ErrBlockOverflow when a block has more keys than maxKeysPerBlock.
func TestBlockOverflowReturnsError(t *testing.T) {
	// Use a small totalKeys so maxKeysPerBlock is low:
	// numBlocks(100) = 2, expectedAvg = 50, maxKeys = 50 + 7*√50 ≈ 99
	builder := NewBuilder(100, testSeed1, 4, 1)

	rng := newTestRNG(t)
	for i := range 200 {
		builder.AddKey(rng.Uint64(), rng.Uint64(), uint64(i), uint32(i&0xFF))
	}

	metadataBuf := make([]byte, estimateMetadataSize(200)+1000)
	payloadsBuf := make([]byte, 200*5)
	_, _, _, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
	if err == nil {
		t.Fatal("expected ErrBlockOverflow, got nil")
	}
	if !errors.Is(err, sherr.ErrBlockOverflow) {
		t.Fatalf("expected ErrBlockOverflow, got: %v", err)
	}
}

// TestPayloadRoundtripAllSizes tests payload round-trip for ALL entry sizes,
// including both fast-path (4, 5, 8) and generic-path sizes.
func TestPayloadRoundtripAllSizes(t *testing.T) {
	configs := []struct {
		payloadSize     int
		fingerprintSize int
	}{
		{1, 0}, // entrySize=1, generic path
		{2, 0}, // entrySize=2, generic path
		{2, 1}, // entrySize=3, generic path
		{3, 1}, // entrySize=4, fast path
		{4, 1}, // entrySize=5, fast path
		{5, 1}, // entrySize=6, generic path
		{6, 1}, // entrySize=7, generic path
		{4, 4}, // entrySize=8, fast path
	}

	const numKeys = 500
	const globalSeed = uint64(0xABCDEF0123456789)

	for _, cfg := range configs {
		entrySize := cfg.payloadSize + cfg.fingerprintSize
		name := fmt.Sprintf("entry%d_pay%d_fp%d", entrySize, cfg.payloadSize, cfg.fingerprintSize)
		t.Run(name, func(t *testing.T) {
			builder := NewBuilder(100000, globalSeed, cfg.payloadSize, cfg.fingerprintSize)

			payloadMask := uint64((1 << (cfg.payloadSize * 8)) - 1)

			type keyRecord struct {
				k0, k1      uint64
				payload     uint64
				fingerprint uint32
			}
			keys := make([]keyRecord, numKeys)

			rng := newTestRNG(t)
			for i := range numKeys {
				k0 := rng.Uint64()
				k1 := rng.Uint64()
				payload := uint64(i) & payloadMask
				// Simplified deterministic fingerprint for storage round-trip testing.
				// Actual FP rate validated in integration tests.
				var fp uint32
				if cfg.fingerprintSize > 0 {
					fp = uint32(k0>>32) & uint32((uint64(1)<<(cfg.fingerprintSize*8))-1)
				}
				keys[i] = keyRecord{k0, k1, payload, fp}
				builder.AddKey(k0, k1, payload, fp)
			}

			metadataBuf := make([]byte, estimateMetadataSize(numKeys))
			payloadsBuf := make([]byte, numKeys*entrySize)

			metaLen, _, _, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
			if err != nil {
				t.Fatalf("BuildSeparatedInto failed: %v", err)
			}
			metadata := metadataBuf[:metaLen]

			decoder, err := NewDecoder(nil, globalSeed)
			if err != nil {
				t.Fatalf("NewDecoder failed: %v", err)
			}

			slotsSeen := make(map[int]int) // slot -> key index
			for i, key := range keys {
				slot, err := decoder.QuerySlot(key.k0, key.k1, metadata, numKeys)
				if err != nil {
					t.Fatalf("key %d: QuerySlot failed: %v", i, err)
				}
				if slot < 0 || slot >= numKeys {
					t.Errorf("key %d: slot %d out of range [0, %d)", i, slot, numKeys)
					continue
				}

				if prev, exists := slotsSeen[slot]; exists {
					t.Errorf("key %d: duplicate slot %d (also key %d)", i, slot, prev)
					continue
				}
				slotsSeen[slot] = i

				// Read back the entry and verify fingerprint + payload
				gotFP, gotPayload := encoding.ReadEntry(payloadsBuf, slot, entrySize, cfg.fingerprintSize, cfg.payloadSize)
				if gotFP != key.fingerprint {
					t.Errorf("key %d slot %d: fp mismatch: got 0x%X, want 0x%X",
						i, slot, gotFP, key.fingerprint)
				}
				if gotPayload != key.payload {
					t.Errorf("key %d slot %d: payload mismatch: got 0x%X, want 0x%X",
						i, slot, gotPayload, key.payload)
				}
			}

			if len(slotsSeen) != numKeys {
				t.Errorf("expected %d unique slots, got %d", numKeys, len(slotsSeen))
			}
		})
	}
}

// TestBuilderReset verifies that Reset() properly clears state so the builder
// can be reused for a second block with different keys.
func TestBuilderReset(t *testing.T) {
	const totalKeys = 10000
	const globalSeed = uint64(0xABCDEF0123456789)
	const payloadSize = 4
	const fpSize = 1
	const numKeysBlock1 = 200
	const numKeysBlock2 = 300
	const entrySize = payloadSize + fpSize

	builder := NewBuilder(totalKeys, globalSeed, payloadSize, fpSize)

	// --- Block 1 ---
	rng := newTestRNG(t)
	type keyRecord struct {
		k0, k1      uint64
		payload     uint64
		fingerprint uint32
	}
	keys1 := make([]keyRecord, numKeysBlock1)
	for i := range numKeysBlock1 {
		k0 := rng.Uint64()
		k1 := rng.Uint64()
		payload := uint64(i) & 0xFFFFFFFF
		fp := uint32(k0>>32) & 0xFF
		keys1[i] = keyRecord{k0, k1, payload, fp}
		builder.AddKey(k0, k1, payload, fp)
	}

	metaBuf1 := make([]byte, estimateMetadataSize(numKeysBlock1))
	payBuf1 := make([]byte, numKeysBlock1*entrySize)
	metaLen1, _, nk1, err := builder.BuildSeparatedInto(metaBuf1, payBuf1)
	if err != nil {
		t.Fatalf("BuildSeparatedInto block 1 failed: %v", err)
	}
	if nk1 != numKeysBlock1 {
		t.Fatalf("block 1: got %d keys, want %d", nk1, numKeysBlock1)
	}

	// Verify block 1 MPHF
	decoder, err := NewDecoder(nil, globalSeed)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}
	slots1 := make(map[int]bool)
	for i, key := range keys1 {
		slot, err := decoder.QuerySlot(key.k0, key.k1, metaBuf1[:metaLen1], numKeysBlock1)
		if err != nil {
			t.Fatalf("block 1 key %d: QuerySlot failed: %v", i, err)
		}
		if slots1[slot] {
			t.Fatalf("block 1 key %d: duplicate slot %d", i, slot)
		}
		slots1[slot] = true
	}
	if len(slots1) != numKeysBlock1 {
		t.Fatalf("block 1: got %d unique slots, want %d", len(slots1), numKeysBlock1)
	}

	// Verify payloads round-trip for block 1
	for i, key := range keys1 {
		slot, _ := decoder.QuerySlot(key.k0, key.k1, metaBuf1[:metaLen1], numKeysBlock1)
		gotFP, gotPayload := encoding.ReadEntry(payBuf1, slot, entrySize, fpSize, payloadSize)
		if gotFP != key.fingerprint {
			t.Errorf("block 1 key %d: fp mismatch: got 0x%X, want 0x%X", i, gotFP, key.fingerprint)
		}
		if gotPayload != key.payload {
			t.Errorf("block 1 key %d: payload mismatch: got 0x%X, want 0x%X", i, gotPayload, key.payload)
		}
	}

	// --- Reset and Block 2 ---
	builder.Reset()

	if builder.KeysAdded() != 0 {
		t.Fatalf("after Reset: KeysAdded() = %d, want 0", builder.KeysAdded())
	}

	keys2 := make([]keyRecord, numKeysBlock2)
	for i := range numKeysBlock2 {
		k0 := rng.Uint64()
		k1 := rng.Uint64()
		payload := uint64(i+1000) & 0xFFFFFFFF
		fp := uint32(k0>>32) & 0xFF
		keys2[i] = keyRecord{k0, k1, payload, fp}
		builder.AddKey(k0, k1, payload, fp)
	}

	if builder.KeysAdded() != numKeysBlock2 {
		t.Fatalf("after adding block 2: KeysAdded() = %d, want %d", builder.KeysAdded(), numKeysBlock2)
	}

	metaBuf2 := make([]byte, estimateMetadataSize(numKeysBlock2))
	payBuf2 := make([]byte, numKeysBlock2*entrySize)
	metaLen2, _, nk2, err := builder.BuildSeparatedInto(metaBuf2, payBuf2)
	if err != nil {
		t.Fatalf("BuildSeparatedInto block 2 failed: %v", err)
	}
	if nk2 != numKeysBlock2 {
		t.Fatalf("block 2: got %d keys, want %d", nk2, numKeysBlock2)
	}

	// Verify block 2 MPHF
	slots2 := make(map[int]bool)
	for i, key := range keys2 {
		slot, err := decoder.QuerySlot(key.k0, key.k1, metaBuf2[:metaLen2], numKeysBlock2)
		if err != nil {
			t.Fatalf("block 2 key %d: QuerySlot failed: %v", i, err)
		}
		if slots2[slot] {
			t.Fatalf("block 2 key %d: duplicate slot %d", i, slot)
		}
		slots2[slot] = true
	}
	if len(slots2) != numKeysBlock2 {
		t.Fatalf("block 2: got %d unique slots, want %d", len(slots2), numKeysBlock2)
	}

	// Verify payloads round-trip for block 2
	for i, key := range keys2 {
		slot, _ := decoder.QuerySlot(key.k0, key.k1, metaBuf2[:metaLen2], numKeysBlock2)
		gotFP, gotPayload := encoding.ReadEntry(payBuf2, slot, entrySize, fpSize, payloadSize)
		if gotFP != key.fingerprint {
			t.Errorf("block 2 key %d: fp mismatch: got 0x%X, want 0x%X", i, gotFP, key.fingerprint)
		}
		if gotPayload != key.payload {
			t.Errorf("block 2 key %d: payload mismatch: got 0x%X, want 0x%X", i, gotPayload, key.payload)
		}
	}
}
