package ptrhash

import (
	"errors"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// TestNewDecoder tests the NewDecoder constructor.
func TestNewDecoder(t *testing.T) {
	t.Run("nil_config_succeeds", func(t *testing.T) {
		d, err := NewDecoder(nil, 0x1234)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d == nil {
			t.Fatal("decoder is nil")
		}
	})

	t.Run("empty_config_succeeds", func(t *testing.T) {
		d, err := NewDecoder([]byte{}, 0x1234)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d == nil {
			t.Fatal("decoder is nil")
		}
	})

	t.Run("non_empty_config_errors", func(t *testing.T) {
		_, err := NewDecoder([]byte{0x01}, 0x1234)
		if err == nil {
			t.Fatal("expected error for non-empty config")
		}
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("expected ErrCorruptedIndex, got: %v", err)
		}
	})
}

// TestDecoderQuerySlotErrorPaths tests each early-return error path in QuerySlot.
func TestDecoderQuerySlotErrorPaths(t *testing.T) {
	dec, err := NewDecoder(nil, 0x1234)
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}

	t.Run("keysInBlock_zero", func(t *testing.T) {
		meta := make([]byte, bucketsPerBlock+10)
		_, err := dec.QuerySlot(0x1111, 0x2222, meta, 0)
		if !errors.Is(err, sherr.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("truncated_metadata", func(t *testing.T) {
		// Metadata shorter than numBuckets triggers ErrCorruptedIndex.
		truncated := make([]byte, bucketsPerBlock-1)
		_, err := dec.QuerySlot(0x1111, 0x2222, truncated, 100)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("expected ErrCorruptedIndex, got: %v", err)
		}
	})
}

// TestDecoderQuerySlotRoundTrip builds a block with the builder, encodes metadata,
// creates a decoder, queries all keys, and verifies unique slots in the correct range.
func TestDecoderQuerySlotRoundTrip(t *testing.T) {
	const globalSeed = uint64(0xABCDEF0123456789)
	const numKeys = 500

	builder := NewBuilder(100000, globalSeed, 0, 0)

	type keyRecord struct {
		k0, k1 uint64
	}
	keys := make([]keyRecord, numKeys)

	rng := newTestRNG(t)
	for i := range numKeys {
		k0 := rng.Uint64()
		k1 := rng.Uint64()
		keys[i] = keyRecord{k0, k1}
		builder.AddKey(k0, k1, 0, 0)
	}

	metadataBuf := make([]byte, estimateMetadataSize(numKeys))
	payloadsBuf := make([]byte, 0) // no payloads

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
	}

	if len(slotsSeen) != numKeys {
		t.Errorf("expected %d unique slots, got %d", numKeys, len(slotsSeen))
	}
}

// TestDecoderQuerySlotEmptyBlock verifies QuerySlot returns ErrNotFound for empty blocks.
func TestDecoderQuerySlotEmptyBlock(t *testing.T) {
	const globalSeed = uint64(0x1234567890ABCDEF)

	builder := NewBuilder(100000, globalSeed, 0, 0)

	metadataBuf := make([]byte, estimateMetadataSize(0))
	payloadsBuf := make([]byte, 0)

	metaLen, _, numKeys, err := builder.BuildSeparatedInto(metadataBuf, payloadsBuf)
	if err != nil {
		t.Fatalf("BuildSeparatedInto failed: %v", err)
	}
	if numKeys != 0 {
		t.Fatalf("expected 0 keys, got %d", numKeys)
	}
	metadata := metadataBuf[:metaLen]

	decoder, err := NewDecoder(nil, globalSeed)
	if err != nil {
		t.Fatalf("NewDecoder failed: %v", err)
	}

	_, err = decoder.QuerySlot(0x1111, 0x2222, metadata, 0)
	if !errors.Is(err, sherr.ErrNotFound) {
		t.Errorf("expected ErrNotFound for empty block, got: %v", err)
	}
}
