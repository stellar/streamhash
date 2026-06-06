// corruption_context_test.go tests failure modes and operational safety:
// corruption detection (byte-level index tampering), context cancellation
// propagation through build pipelines, and goroutine leak detection after
// builder lifecycle operations. These share the pattern of building a valid
// index then verifying behavior under adverse conditions.
package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// ============================================================================
// Corruption Detection
// ============================================================================

func TestCorruptionDetection(t *testing.T) {
	// Build a valid index to corrupt
	numKeys := 1000
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 32)

	ctx := context.Background()
	tmpDir := t.TempDir()
	validPath := filepath.Join(tmpDir, "valid.idx")
	if err := quickBuild(ctx, validPath, keys); err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	validData, err := os.ReadFile(validPath)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("CorruptDataBytes", func(t *testing.T) {
		// Corrupt a byte in the middle of the file
		corruptedPath := filepath.Join(t.TempDir(), "corrupt.idx")
		corrupted := make([]byte, len(validData))
		copy(corrupted, validData)
		corrupted[len(corrupted)/2] ^= 0xFF
		if err := os.WriteFile(corruptedPath, corrupted, 0644); err != nil {
			t.Fatal(err)
		}

		idx, err := Open(corruptedPath)
		if err != nil {
			return // Footer checksum caught it
		}
		defer idx.Close()
		if err := idx.Verify(); err == nil {
			t.Error("Expected Verify() to detect corruption")
		}
	})

	t.Run("CorruptFooterChecksum", func(t *testing.T) {
		corruptedPath := filepath.Join(t.TempDir(), "corrupt-footer.idx")
		corrupted := make([]byte, len(validData))
		copy(corrupted, validData)
		// Footer is last 32 bytes; corrupt MetadataRegionHash at offset 8
		footerStart := len(corrupted) - 32
		corrupted[footerStart+8] ^= 0xFF
		if err := os.WriteFile(corruptedPath, corrupted, 0644); err != nil {
			t.Fatal(err)
		}

		idx, err := Open(corruptedPath)
		if err != nil {
			return // Acceptable
		}
		defer idx.Close()
		if err := idx.Verify(); err == nil {
			t.Error("Expected Verify to fail with corrupted footer checksum")
		}
	})

	t.Run("CorruptRAMIndexOffset", func(t *testing.T) {
		corruptedPath := filepath.Join(t.TempDir(), "corrupt-ram.idx")
		corrupted := make([]byte, len(validData))
		copy(corrupted, validData)
		// RAM index starts after header(64) + userMetaLen(4) + algoConfigLen(4) = 72
		// First entry's MetadataOffset at byte 5-9 within first entry
		ramIndexStart := 64 + 4 + 4
		ramIndexOffset := ramIndexStart + 5
		for i := 0; i < 5 && ramIndexOffset+i < len(corrupted); i++ {
			corrupted[ramIndexOffset+i] = 0xFF
		}
		if err := os.WriteFile(corruptedPath, corrupted, 0644); err != nil {
			t.Fatal(err)
		}

		idx, err := Open(corruptedPath)
		if err != nil {
			return // Open detected corruption
		}
		defer idx.Close()
		for _, key := range keys {
			if _, err := idx.QueryRank(key); errors.Is(err, sherr.ErrCorruptedIndex) {
				return // Query detected corruption
			}
		}
		// Queries didn't hit the corrupted entry; Verify should catch it
		if err := idx.Verify(); err == nil {
			t.Error("Expected corruption to be detected by Open, Query, or Verify")
		}
	})

	t.Run("CorruptAtVariousOffsets", func(t *testing.T) {
		offsets := []struct {
			name   string
			offset int
		}{
			{"header_magic", 0},
			{"block_area_start", 64},
			{"block_area_middle", len(validData) / 2},
			{"before_footer", len(validData) - 100},
		}

		for _, tc := range offsets {
			t.Run(tc.name, func(t *testing.T) {
				corruptedPath := filepath.Join(t.TempDir(), "corrupt.idx")
				corrupted := make([]byte, len(validData))
				copy(corrupted, validData)
				if tc.offset < len(corrupted) {
					corrupted[tc.offset] ^= 0xFF
				}
				if err := os.WriteFile(corruptedPath, corrupted, 0644); err != nil {
					t.Fatal(err)
				}
				idx, err := Open(corruptedPath)
				if err != nil {
					return // Open detected corruption
				}
				defer idx.Close()

				queryErrors := 0
				for i := range 100 {
					if _, err := idx.QueryRank(keys[i%numKeys]); err != nil {
						queryErrors++
					}
				}
				if queryErrors > 0 {
					return // Queries detected corruption
				}
				// Neither Open nor queries caught corruption; Verify should
				if err := idx.Verify(); err == nil {
					t.Errorf("Corruption at %s: not detected by Open, Query, or Verify", tc.name)
				}
			})
		}
	})

	t.Run("MetadataRegionCorruption", func(t *testing.T) {
		hdr, err := decodeHeader(validData[:headerSize])
		if err != nil {
			t.Fatal(err)
		}
		// Compute metadata region offset matching the on-disk layout:
		// [Header 64B][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index][Payload][Metadata]
		userMetaLen := binary.LittleEndian.Uint32(validData[headerSize:])
		algoConfigOffset := uint64(headerSize) + 4 + uint64(userMetaLen)
		algoConfigLen := binary.LittleEndian.Uint32(validData[algoConfigOffset:])
		ramIndexOffset := algoConfigOffset + 4 + uint64(algoConfigLen)
		numRAMEntries := hdr.NumBlocks + 1
		ramIndexSize := uint64(numRAMEntries) * uint64(ramIndexEntrySize)
		payloadRegionOffset := ramIndexOffset + ramIndexSize
		payloadRegionSize := hdr.TotalKeys * uint64(hdr.fingerprintSizeInt()+hdr.payloadSizeInt())
		metadataRegionOffset := payloadRegionOffset + payloadRegionSize

		corruptedPath := filepath.Join(t.TempDir(), "corrupt-meta.idx")
		corrupted := make([]byte, len(validData))
		copy(corrupted, validData)
		if int(metadataRegionOffset)+10 < len(corrupted)-footerSize {
			corrupted[metadataRegionOffset+10] ^= 0xFF
		}
		if err := os.WriteFile(corruptedPath, corrupted, 0644); err != nil {
			t.Fatal(err)
		}

		idx, err := Open(corruptedPath)
		if err != nil {
			return // Open detected corruption
		}
		defer idx.Close()
		if err := idx.Verify(); err == nil {
			t.Error("Expected Verify() to detect metadata corruption")
		}
	})
}

// ============================================================================
// Context Cancellation
// ============================================================================

func TestContextCancellation(t *testing.T) {
	t.Run("SortedBuild", func(t *testing.T) {
		tmpDir := t.TempDir()
		indexPath := filepath.Join(tmpDir, "cancel.idx")

		numKeys := 100000
		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, numKeys, 32)
		sortKeysByBlock(keys, uint64(numKeys), nil)

		ctx, cancel := context.WithCancel(context.Background())
		keyIdx := 0
		keyIter := func(yield func([]byte, []byte) bool) {
			for _, k := range keys {
				if keyIdx == 1000 {
					cancel()
				}
				if !yield(k, nil) {
					return
				}
				keyIdx++
			}
		}

		err := buildSorted(ctx, indexPath, uint64(numKeys), keyIter, WithWorkers(2))
		if err == nil {
			os.Remove(indexPath)
		} else if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled or nil, got: %v", err)
		}
	})

	t.Run("ParallelBuild", func(t *testing.T) {
		tmpDir := t.TempDir()
		indexPath := filepath.Join(tmpDir, "cancel.idx")

		numKeys := 10000
		keys := make([][]byte, numKeys)
		for i := range keys {
			keys[i] = make([]byte, 24)
			binary.BigEndian.PutUint64(keys[i][0:8], uint64(i)*0x0123456789ABCDEF)
			binary.BigEndian.PutUint64(keys[i][8:16], uint64(i)*0xFEDCBA9876543210)
		}

		ctx, cancel := context.WithCancel(context.Background())
		keyIdx := 0
		keyIter := func(yield func([]byte, []byte) bool) {
			for _, k := range keys {
				if keyIdx == 500 {
					cancel()
				}
				if !yield(k, nil) {
					return
				}
				keyIdx++
			}
		}

		err := buildParallelBytes(ctx, indexPath, keyIter)
		if err == nil {
			os.Remove(indexPath)
		} else if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled or nil, got: %v", err)
		}
	})

	t.Run("UnsortedBuild", func(t *testing.T) {
		tmpDir := t.TempDir()
		indexPath := filepath.Join(tmpDir, "cancel.idx")

		numKeys := 10000
		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, numKeys, 32)

		ctx, cancel := context.WithCancel(context.Background())
		keyIter := func(yield func([]byte, uint64) bool) {
			for i, k := range keys {
				if i == numKeys/2 {
					cancel()
				}
				if !yield(k, 0) {
					return
				}
			}
		}

		err := buildUnsortedFromIter(ctx, indexPath, keyIter)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled or nil, got: %v", err)
		}
	})

	t.Run("UnsortedReplay", func(t *testing.T) {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
				tmpDir := t.TempDir()
				output := filepath.Join(tmpDir, "cancel.idx")

				numKeys := 50000
				ctx, cancel := context.WithCancel(context.Background())

				builder, err := NewUnsortedBuilder(ctx, output, uint64(numKeys),
					tmpDir, WithPayload(4), WithWorkers(workers))
				if err != nil {
					t.Fatalf("newBuilder failed: %v", err)
				}

				rng := newTestRNG(t)
				for i := range numKeys {
					key := make([]byte, 16)
					fillFromRNG(rng, key)
					if err := builder.AddKey(key, uint64(i)); err != nil {
						builder.Close()
						t.Fatalf("AddKey failed at %d: %v", i, err)
					}
				}

				cancel()
				err = builder.Finish()
				if err == nil {
					t.Fatal("expected error from cancelled context, got nil")
				}
				if !errors.Is(err, context.Canceled) {
					t.Errorf("expected context.Canceled, got: %v", err)
				}
			})
		}
	})

	t.Run("NestedContext", func(t *testing.T) {
		tmpDir := t.TempDir()
		indexPath := filepath.Join(tmpDir, "nested.idx")

		parentCtx, parentCancel := context.WithCancel(context.Background())
		childCtx, childCancel := context.WithCancel(parentCtx)
		defer parentCancel()
		defer childCancel()

		numKeys := 500
		keys := make([][]byte, numKeys)
		for i := range keys {
			src := make([]byte, 20)
			binary.BigEndian.PutUint64(src[0:8], uint64(i))
			binary.BigEndian.PutUint64(src[8:16], uint64(i*7919))
			for j := 16; j < 20; j++ {
				src[j] = byte(i + j)
			}
			keys[i] = PreHash(src)
		}
		sortKeysByBlock(keys, uint64(numKeys), nil)

		builder, err := NewSortedBuilder(childCtx, indexPath, uint64(numKeys), WithWorkers(2))
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}

		for i := 0; i < numKeys/2; i++ {
			if err := builder.AddKey(keys[i], 0); err != nil {
				builder.Close()
				t.Fatalf("AddKey failed: %v", err)
			}
		}

		parentCancel()

		var addErr error
		for i := numKeys / 2; i < numKeys; i++ {
			if err := builder.AddKey(keys[i], 0); err != nil {
				addErr = err
				break
			}
		}

		if addErr == nil {
			addErr = builder.Finish()
		}
		builder.Close()

		// After parent cancel, either AddKey or Finish should fail
		if addErr != nil && !errors.Is(addErr, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", addErr)
		}
	})
}

// ============================================================================
// Goroutine Leak Detection
// ============================================================================

func TestGoroutineLeaks(t *testing.T) {
	t.Run("SuccessPath", func(t *testing.T) {
		initialGoroutines := runtime.NumGoroutine()

		for iter := range 5 {
			tmpDir := t.TempDir()
			indexPath := filepath.Join(tmpDir, "leak_test.idx")

			numKeys := 500
			keys := make([][]byte, numKeys)
			for i := range keys {
				src := make([]byte, 20)
				binary.BigEndian.PutUint64(src[0:8], uint64(i+iter*1000))
				binary.BigEndian.PutUint64(src[8:16], uint64(i*7919))
				for j := 16; j < 20; j++ {
					src[j] = byte(i + j)
				}
				keys[i] = PreHash(src)
			}
			sortKeysByBlock(keys, uint64(numKeys), nil)

			builder, err := NewSortedBuilder(context.Background(), indexPath, uint64(numKeys), WithWorkers(4))
			if err != nil {
				t.Fatalf("newBuilder failed: %v", err)
			}
			for _, key := range keys {
				if err := builder.AddKey(key, 0); err != nil {
					builder.Close()
					t.Fatalf("AddKey failed: %v", err)
				}
			}
			if err := builder.Finish(); err != nil {
				t.Fatalf("Finish failed: %v", err)
			}
			builder.Close()
		}

		// No sleep needed: builder shutdown is synchronous (workerGroup.Wait +
		// <-writerDone block until all goroutines exit).
		if final := runtime.NumGoroutine(); final > initialGoroutines+3 {
			t.Errorf("Potential goroutine leak: started with %d, ended with %d",
				initialGoroutines, final)
		}
	})

	t.Run("ErrorPath", func(t *testing.T) {
		initialGoroutines := runtime.NumGoroutine()

		for iter := range 5 {
			tmpDir := t.TempDir()
			indexPath := filepath.Join(tmpDir, "error_leak.idx")

			ctx, cancel := context.WithCancel(context.Background())
			builder, err := NewSortedBuilder(ctx, indexPath, 1000, WithWorkers(4))
			if err != nil {
				cancel()
				t.Fatalf("newBuilder failed: %v", err)
			}

			for i := range 100 {
				src := make([]byte, 20)
				binary.BigEndian.PutUint64(src[0:8], uint64(i+iter*1000))
				key := PreHash(src)
				builder.AddKey(key, 0)
			}

			cancel()
			builder.Close()
		}

		// No sleep needed: builder shutdown is synchronous (shutdownWorkers
		// blocks on workerGroup.Wait + <-writerDone).
		if final := runtime.NumGoroutine(); final > initialGoroutines+3 {
			t.Errorf("Potential goroutine leak on error: started with %d, ended with %d",
				initialGoroutines, final)
		}
	})
}
