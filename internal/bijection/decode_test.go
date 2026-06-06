package bijection

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// TestReadEFLowBits tests the Elias-Fano low bits reading at buffer boundaries.
func TestReadEFLowBits(t *testing.T) {
	t.Run("ZeroLowBits", func(t *testing.T) {
		data := []byte{0xFF, 0xFF, 0xFF}
		result := readEFLowBits(data, 5, 0)
		if result != 0 {
			t.Errorf("Zero lowBits should return 0, got %d", result)
		}
	})

	t.Run("NegativeElement", func(t *testing.T) {
		data := []byte{0xFF, 0xFF, 0xFF}
		result := readEFLowBits(data, -1, 4)
		if result != 0 {
			t.Errorf("Negative element should return 0, got %d", result)
		}
	})

	t.Run("ElementBeyondData", func(t *testing.T) {
		// 3 bytes = 24 bits, with lowBits=4, max element = 6 (bit 24)
		data := []byte{0xFF, 0xFF, 0xFF}
		result := readEFLowBits(data, 100, 4)
		if result != 0 {
			t.Errorf("Element beyond data should return 0, got %d", result)
		}
	})

	t.Run("FastPath_64BitLoad", func(t *testing.T) {
		// 10 bytes available, reading element 0 with lowBits=4
		data := []byte{0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE}
		result := readEFLowBits(data, 0, 4)
		expected := uint16(0xAB & 0x0F) // Low 4 bits
		if result != expected {
			t.Errorf("Fast path element 0: got %d, want %d", result, expected)
		}
	})

	t.Run("BoundaryFallback_TwoBytes", func(t *testing.T) {
		// Only 2 bytes available, force boundary path
		data := []byte{0xAB, 0xCD}
		// Element 0, lowBits=4: bitPos=0, byteIdx=0
		// byteIdx+8 > len(data), so fallback path
		result := readEFLowBits(data, 0, 4)
		expected := uint16(0xAB & 0x0F)
		if result != expected {
			t.Errorf("Boundary 2-byte element 0: got %d, want %d", result, expected)
		}
	})

	t.Run("BoundaryFallback_CrossByte", func(t *testing.T) {
		// 3 bytes, reading element that crosses byte boundary
		// Element 1 with lowBits=6: bitPos=6, byteIdx=0, bitIdx=6
		// Value crosses from byte 0 into byte 1
		data := []byte{0b11000000, 0b00000011, 0xFF} // bits 6-11 = 0b001111 = 15
		result := readEFLowBits(data, 1, 6)
		expected := uint16(15)
		if result != expected {
			t.Errorf("Boundary cross-byte: got %d, want %d", result, expected)
		}
	})

	t.Run("BoundaryFallback_NeedsThirdByte", func(t *testing.T) {
		data := []byte{0x00, 0b11111100, 0b00001111, 0x00}
		result := readEFLowBits(data, 1, 10)
		expected := uint16(1023)
		if result != expected {
			t.Errorf("Boundary needs third byte: got %d, want %d", result, expected)
		}
	})
}

// TestDecodeEFRangeInlineDirect tests decodeEFRangeInline with direct unit tests.
func TestDecodeEFRangeInlineDirect(t *testing.T) {
	t.Run("StartGreaterThanEnd", func(t *testing.T) {
		// startElement > endElement should return 0
		efData := make([]byte, 16)
		out := make([]uint16, 10)
		count := decodeEFRangeInline(efData, 10, 100, 5, 3, 0, out)
		if count != 0 {
			t.Errorf("Expected 0 for start > end, got %d", count)
		}
	})

	t.Run("StartBeyondBuckets", func(t *testing.T) {
		// startElement >= numBuckets should return 0
		efData := make([]byte, 16)
		out := make([]uint16, 10)
		count := decodeEFRangeInline(efData, 10, 100, 15, 20, 0, out)
		if count != 0 {
			t.Errorf("Expected 0 for start >= numBuckets, got %d", count)
		}
	})

	t.Run("EndCappedAtBuckets", func(t *testing.T) {
		// endElement >= numBuckets should be capped
		efData := make([]byte, 32)
		for i := range efData {
			efData[i] = 0xFF
		}
		out := make([]uint16, 20)
		count := decodeEFRangeInline(efData, 10, 50, 0, 100, 0, out)
		if count != 10 {
			t.Errorf("Expected 10 elements (capped), got %d", count)
		}
	})

	t.Run("TooSmallData", func(t *testing.T) {
		// len(efData) < 8 should return 0
		efData := make([]byte, 5)
		out := make([]uint16, 10)
		count := decodeEFRangeInline(efData, 10, 100, 0, 5, 0, out)
		if count != 0 {
			t.Errorf("Expected 0 for small data, got %d", count)
		}
	})

	t.Run("ZeroLowBits", func(t *testing.T) {
		// When keysInBlock == numBuckets, lowBits = 0.
		// High bits: 10 consecutive 1-bits at positions 0-9.
		// Each decoded value = highPart (which is 0 since no 0-bits precede each 1-bit).
		efData := make([]byte, 16)
		efData[0] = 0xFF
		efData[1] = 0x03 // bits 8-9
		out := make([]uint16, 10)
		count := decodeEFRangeInline(efData, 10, 10, 0, 9, 0, out)
		if count != 10 {
			t.Fatalf("Expected count=10, got %d", count)
		}
		for i := range count {
			if out[i] != 0 {
				t.Errorf("out[%d] = %d, want 0", i, out[i])
			}
		}
	})
}

// TestDecodeEFRangeInlineSlowPaths tests the slow paths in decodeEFRangeInline.
func TestDecodeEFRangeInlineSlowPaths(t *testing.T) {
	t.Run("SlowPathLowBits", func(t *testing.T) {
		efData := make([]byte, 10)
		for i := range efData {
			efData[i] = 0xFF
		}
		out := make([]uint16, 10)
		count := decodeEFRangeInline(efData, 5, 20, 0, 4, 0, out)
		if count != 5 {
			t.Errorf("expected 5 decoded elements, got %d", count)
		}
		for i := range count {
			if out[i] != 3 {
				t.Errorf("out[%d] = %d, want 3", i, out[i])
			}
		}
	})

	t.Run("WindowZeroCase", func(t *testing.T) {
		// Synthetic EF data: mostly zeros with a non-zero byte at offset 24
		// to exercise the code path when the 64-bit window read is zero.
		efData := make([]byte, 32)
		efData[24] = 0xFF
		out := make([]uint16, 10)
		count := decodeEFRangeInline(efData, 5, 10, 0, 4, 0, out)
		if count != 5 {
			t.Errorf("expected 5 decoded elements, got %d", count)
		}
		// Decoded EF values must be monotonically non-decreasing
		for i := 1; i < count; i++ {
			if out[i] < out[i-1] {
				t.Errorf("not monotonic at %d: %d < %d", i, out[i], out[i-1])
			}
		}
	})

	t.Run("SafeWindowLoad", func(t *testing.T) {
		// The safe window load path in decodeEFRangeInline triggers when
		// byteIdx+8 > len(efData), i.e., for elements whose high-bit positions
		// are in the last 7 bytes. This requires decoding elements near the END
		// of the bucket range.
		rng := newTestRNG(t)
		numKeys := 20
		globalSeed := uint64(testSeed1)

		bb := NewBuilder(uint64(numKeys*2), globalSeed, 0, 0)
		for range numKeys {
			bb.AddKey(rng.Uint64(), rng.Uint64(), 0, 0)
		}

		metaDst := make([]byte, bb.MaxMetadataSizeForCurrentBlock())
		metaLen, _, _, err := bb.BuildSeparatedInto(metaDst, nil)
		if err != nil {
			t.Fatalf("build failed: %v", err)
		}
		meta := metaDst[:metaLen]

		efData := meta[checkpointsSize:]
		var fullOut [bucketsPerBlock]uint16
		decodeEliasFanoUnrolled(efData, bucketsPerBlock, numKeys, &fullOut)

		// Decode the last checkpoint segment where high-bit positions are near the
		// end of the EF data, triggering the safe window load path.
		var cp checkpoints
		decodeCheckpoints(meta[0:checkpointsSize], &cp)
		lastSeg := numCheckpoints
		start := lastSeg * checkpointInterval
		var startBitInHighBits int
		if lastSeg > 0 {
			startBitInHighBits = int(cp.efBitPos[lastSeg-1])
		}
		end := bucketsPerBlock - 1

		out := make([]uint16, bucketsPerBlock)
		count := decodeEFRangeInline(efData, bucketsPerBlock, numKeys, start, end, startBitInHighBits, out[start:])
		expectedCount := end - start + 1
		if count != expectedCount {
			t.Errorf("expected %d decoded elements, got %d", expectedCount, count)
		}
		for i := start; i <= end; i++ {
			if out[i] != fullOut[i] {
				t.Errorf("out[%d]=%d != fullOut[%d]=%d", i, out[i], i, fullOut[i])
			}
		}
	})
}

// TestVerySmallEFData tests EF decoding with minimal data sizes.
func TestVerySmallEFData(t *testing.T) {
	testCases := []struct {
		name     string
		dataLen  int
		element  int
		lowBits  int
		wantZero bool
	}{
		{"1 byte, element 0, 4 bits", 1, 0, 4, false},
		{"1 byte, element 1, 4 bits (partial)", 1, 1, 4, false},
		{"1 byte, element 2, 4 bits (beyond)", 1, 2, 4, true},
		{"2 bytes, element 0, 8 bits", 2, 0, 8, false},
		{"2 bytes, element 1, 8 bits", 2, 1, 8, false},
		{"3 bytes, element 5, 4 bits", 3, 5, 4, false},
		{"3 bytes, element 6, 4 bits (boundary)", 3, 6, 4, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, tc.dataLen)
			for i := range data {
				data[i] = 0xFF
			}

			result := readEFLowBits(data, tc.element, tc.lowBits)

			if tc.wantZero && result != 0 {
				t.Errorf("Expected 0 for out-of-bounds, got %d", result)
			}
			if !tc.wantZero {
				// For all-0xFF data, low bits should be all ones
				expectedLow := uint16((1 << tc.lowBits) - 1)
				if result != expectedLow {
					t.Errorf("Expected %d for in-bounds element with all-0xFF data, got %d", expectedLow, result)
				}
			}
		})
	}
}

// TestNewDecoderErrorPath tests NewDecoder error handling.
func TestNewDecoderErrorPath(t *testing.T) {
	t.Run("non_empty_globalConfig_returns_error", func(t *testing.T) {
		_, err := NewDecoder([]byte{1}, 12345)
		if err == nil {
			t.Fatal("expected error for non-empty globalConfig, got nil")
		}
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("expected ErrCorruptedIndex, got: %v", err)
		}
	})

	t.Run("nil_globalConfig_succeeds", func(t *testing.T) {
		dec, err := NewDecoder(nil, 12345)
		if err != nil {
			t.Fatalf("expected nil error, got: %v", err)
		}
		if dec == nil {
			t.Fatal("expected non-nil decoder")
		}
	})
}

// TestQuerySlotErrorPaths tests each early-return error path in QuerySlot.
func TestQuerySlotErrorPaths(t *testing.T) {
	dec, err := NewDecoder(nil, testSeed1)
	if err != nil {
		t.Fatalf("NewDecoder: %v", err)
	}

	// Build a real block to get valid metadata for selective corruption.
	rng := newTestRNG(t)
	const numKeys = 100
	bb := NewBuilder(uint64(numKeys*2), testSeed1, 0, 0)
	keys := make([]keyPair, numKeys)
	for i := range keys {
		keys[i] = keyPair{rng.Uint64(), rng.Uint64()}
		bb.AddKey(keys[i].k0, keys[i].k1, 0, 0)
	}
	metaDst := make([]byte, bb.MaxMetadataSizeForCurrentBlock())
	metaLen, _, _, buildErr := bb.BuildSeparatedInto(metaDst, nil)
	if buildErr != nil {
		t.Fatalf("build: %v", buildErr)
	}
	validMeta := metaDst[:metaLen]

	t.Run("keysInBlock_zero", func(t *testing.T) {
		_, err := dec.QuerySlot(keys[0].k0, keys[0].k1, validMeta, 0)
		if !errors.Is(err, sherr.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got: %v", err)
		}
	})

	t.Run("metadata_shorter_than_checkpoints", func(t *testing.T) {
		// checkpointsSize is 28 bytes; pass fewer
		truncated := make([]byte, checkpointsSize-1)
		_, err := dec.QuerySlot(keys[0].k0, keys[0].k1, truncated, numKeys)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("expected ErrCorruptedIndex, got: %v", err)
		}
	})

	t.Run("metadata_exactly_checkpoints_size", func(t *testing.T) {
		// len(metadata) == checkpointsSize triggers the second guard
		// (checkpointsSize >= len(metadata))
		exactCP := make([]byte, checkpointsSize)
		copy(exactCP, validMeta[:checkpointsSize])
		_, err := dec.QuerySlot(keys[0].k0, keys[0].k1, exactCP, numKeys)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("expected ErrCorruptedIndex, got: %v", err)
		}
	})

	t.Run("seed_stream_offset_beyond_metadata", func(t *testing.T) {
		// Truncate metadata after checkpoints + EF data but before seed stream.
		// This triggers the seedStreamOffset >= len(metadata) guard.
		efSize := eliasFanoSize(bucketsPerBlock, numKeys)
		truncLen := min(
			// exactly at seedStreamOffset boundary
			checkpointsSize+efSize, len(validMeta))
		truncated := make([]byte, truncLen)
		copy(truncated, validMeta[:truncLen])

		// All inserted keys hash to non-empty buckets, so they all reach the
		// seedStreamOffset guard and return ErrCorruptedIndex.
		_, err := dec.QuerySlot(keys[0].k0, keys[0].k1, truncated, numKeys)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("expected ErrCorruptedIndex, got: %v", err)
		}
	})

	t.Run("bucket_size_zero_returns_not_found", func(t *testing.T) {
		// With 100 keys across 1024 buckets, most buckets are empty.
		// Query with a key whose bucket has 0 keys.
		gotNotFound := false
		probeRNG := newTestRNG(t)
		for range uint64(10000) {
			k0 := probeRNG.Uint64()
			k1 := probeRNG.Uint64()
			_, err := dec.QuerySlot(k0, k1, validMeta, numKeys)
			if errors.Is(err, sherr.ErrNotFound) {
				gotNotFound = true
				break
			}
		}
		if !gotNotFound {
			t.Error("expected at least one probe to hit an empty bucket (ErrNotFound)")
		}
	})
}

// TestLoadWindow64Safe tests loadWindow64Safe with pseudo-random inputs.
func TestLoadWindow64Safe(t *testing.T) {
	rng := newTestRNG(t)

	t.Run("RandomInputs", func(t *testing.T) {
		const iterations = 10000
		for i := range iterations {
			dataLen := 1 + rng.IntN(16)
			data := make([]byte, dataLen)
			for j := range data {
				data[j] = byte(rng.Uint32())
			}
			byteIdx := rng.IntN(dataLen)

			got := loadWindow64Safe(data, byteIdx)

			// Compute reference
			remaining := dataLen - byteIdx
			var want uint64
			for j := 0; j < min(remaining, 8); j++ {
				want |= uint64(data[byteIdx+j]) << (j * 8)
			}
			// Padding bytes filled with 0xFF
			for j := remaining; j < 8; j++ {
				want |= uint64(0xFF) << (j * 8)
			}

			if got != want {
				t.Fatalf("iter %d: byteIdx=%d dataLen=%d: got=0x%X want=0x%X",
					i, byteIdx, dataLen, got, want)
			}
		}
	})

	t.Run("EmptyRemaining", func(t *testing.T) {
		data := []byte{0x01, 0x02, 0x03}
		got := loadWindow64Safe(data, 3) // byteIdx == len(data)
		// All 0xFF padding
		if got != 0xFFFFFFFFFFFFFFFF {
			t.Errorf("empty remaining: got 0x%X, want 0xFFFFFFFFFFFFFFFF", got)
		}
	})

	t.Run("FullLoad", func(t *testing.T) {
		data := make([]byte, 16)
		binary.LittleEndian.PutUint64(data[0:8], 0xDEADBEEFCAFEBABE)
		binary.LittleEndian.PutUint64(data[8:16], 0x1234567890ABCDEF)

		got := loadWindow64Safe(data, 0)
		want := binary.LittleEndian.Uint64(data[0:8])
		if got != want {
			t.Errorf("full load at 0: got 0x%X, want 0x%X", got, want)
		}

		got2 := loadWindow64Safe(data, 8)
		want2 := binary.LittleEndian.Uint64(data[8:16])
		if got2 != want2 {
			t.Errorf("full load at 8: got 0x%X, want 0x%X", got2, want2)
		}
	})
}

// TestDecodeEFRangeVsFullDecode builds blocks and compares
// decodeEFRangeInline with full EF decode at checkpoint-aligned boundaries.
// decodeEFRangeInline requires the correct startBitInHighBits, which is only
// available at checkpoint boundaries (every 128 buckets) or at offset 0.
func TestDecodeEFRangeVsFullDecode(t *testing.T) {
	rng := newTestRNG(t)

	for block := range 30 {
		numKeys := 500 + rng.IntN(2500)
		globalSeed := testSeed1 ^ uint64(block)

		bb := NewBuilder(uint64(numKeys*2), globalSeed, 0, 0)
		for range numKeys {
			bb.AddKey(rng.Uint64(), rng.Uint64(), 0, 0)
		}

		metaDst := make([]byte, bb.MaxMetadataSizeForCurrentBlock())
		metaLen, _, _, err := bb.BuildSeparatedInto(metaDst, nil)
		if err != nil {
			t.Fatalf("block %d: build failed: %v", block, err)
		}
		meta := metaDst[:metaLen]

		// Decode checkpoints to get high-bit positions
		var cp checkpoints
		decodeCheckpoints(meta[0:checkpointsSize], &cp)

		// Full decode
		efData := meta[checkpointsSize:]
		var fullOut [bucketsPerBlock]uint16
		decodeEliasFanoUnrolled(efData, bucketsPerBlock, numKeys, &fullOut)

		// Test ranges starting at each checkpoint boundary
		for seg := 0; seg <= numCheckpoints; seg++ {
			start := seg * checkpointInterval
			var startBitInHighBits int
			if seg > 0 {
				startBitInHighBits = int(cp.efBitPos[seg-1])
			}

			// Random end within this segment or beyond
			end := start + rng.IntN(bucketsPerBlock-start)
			if end >= bucketsPerBlock {
				end = bucketsPerBlock - 1
			}

			var partialOut [bucketsPerBlock]uint16
			decodeEFRangeInline(efData, bucketsPerBlock, numKeys, start, end, startBitInHighBits, partialOut[start:])

			for j := start; j <= end; j++ {
				if partialOut[j] != fullOut[j] {
					t.Fatalf("block %d seg %d range [%d,%d] idx=%d: partial=%d full=%d",
						block, seg, start, end, j, partialOut[j], fullOut[j])
				}
			}
		}
	}
}
