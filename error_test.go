package streamhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// ---------------------------------------------------------------------------
// Category 1: Open errors
// ---------------------------------------------------------------------------

func TestOpenNonExistentFilePath(t *testing.T) {
	_, err := Open("/nonexistent/path/to/file.idx")
	if err == nil {
		t.Error("Expected error for non-existent file path")
	}
}

func TestOpenDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	_, err := Open(tmpDir)
	if err == nil {
		t.Error("Expected error when opening a directory")
	}
}

func TestOpenEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.idx")
	f, err := os.Create(emptyFile)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = Open(emptyFile)
	if err == nil {
		t.Error("Expected error for empty file")
	}
	if !errors.Is(err, sherr.ErrTruncatedFile) {
		t.Errorf("Expected ErrTruncatedFile, got %v", err)
	}
}

// TestOpenCorruptedMagic verifies that corrupting the magic bytes causes Open to
// return ErrInvalidMagic. Complements TestCorruptionDetection/CorruptAtVariousOffsets
// in corruption_context_test.go, which tests the same offset through Verify().
func TestOpenCorruptedMagic(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")

	if err := createSmallValidIndex(indexPath); err != nil {
		t.Fatalf("Failed to create valid index: %v", err)
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt magic bytes
	data[0] = 0xFF
	data[1] = 0xFF
	if err := os.WriteFile(indexPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(indexPath)
	if !errors.Is(err, sherr.ErrInvalidMagic) {
		t.Errorf("Expected ErrInvalidMagic, got %v", err)
	}
}

func TestOpenCorruptedVersion(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")

	if err := createSmallValidIndex(indexPath); err != nil {
		t.Fatalf("Failed to create valid index: %v", err)
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt version (bytes 4-5 in header)
	data[4] = 0xFF
	data[5] = 0xFF
	if err := os.WriteFile(indexPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(indexPath)
	if !errors.Is(err, sherr.ErrInvalidVersion) {
		t.Errorf("Expected ErrInvalidVersion, got %v", err)
	}
}

func TestOpenTruncatedHeader(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")

	err := createSmallValidIndex(indexPath)
	if err != nil {
		t.Fatalf("Failed to create valid index: %v", err)
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	// Truncate to less than header size
	truncated := data[:32]
	if err := os.WriteFile(indexPath, truncated, 0644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(indexPath)
	if !errors.Is(err, sherr.ErrTruncatedFile) {
		t.Errorf("Expected ErrTruncatedFile, got %v", err)
	}
}

func TestOpenTruncatedFile(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")

	err := createSmallValidIndex(indexPath)
	if err != nil {
		t.Fatalf("Failed to create valid index: %v", err)
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) <= 200 {
		t.Fatal("Test fixture too small to test truncation")
	}
	// Truncate to just past header but not enough for full data
	truncated := data[:200]
	if err := os.WriteFile(indexPath, truncated, 0644); err != nil {
		t.Fatal(err)
	}
	_, err = Open(indexPath)
	if err == nil {
		t.Fatal("Expected error for truncated file")
	}
	if !errors.Is(err, sherr.ErrTruncatedFile) && !errors.Is(err, sherr.ErrCorruptedIndex) {
		t.Errorf("Expected ErrTruncatedFile or ErrCorruptedIndex, got %v", err)
	}
}

func TestOpenCorruptedFooter(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")

	err := createSmallValidIndex(indexPath)
	if err != nil {
		t.Fatalf("Failed to create valid index: %v", err)
	}

	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt last 32 bytes (footer region)
	for i := len(data) - 32; i < len(data); i++ {
		data[i] = 0xFF
	}

	corruptPath := filepath.Join(tmpDir, "corrupt_footer.idx")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	idx, err := Open(corruptPath)
	if err != nil {
		return // Footer corruption detected at Open time — good
	}
	defer idx.Close()
	// If Open succeeded, Verify must detect the corruption
	if err := idx.Verify(); err == nil {
		t.Error("Expected Verify() to detect corrupted footer")
	}
}

// ---------------------------------------------------------------------------
// Category 2: Builder errors
// ---------------------------------------------------------------------------

func TestBuilderReadOnlyDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0555); err != nil {
		t.Fatal(err)
	}

	indexPath := filepath.Join(readOnlyDir, "test.idx")
	ctx := context.Background()
	_, err := NewSortedBuilder(ctx, indexPath, 100)
	if err == nil {
		t.Error("Expected error for read-only directory")
	}
}

func TestBuilderInvalidPath(t *testing.T) {
	ctx := context.Background()
	_, err := NewSortedBuilder(ctx, "/nonexistent/directory/test.idx", 100)
	if err == nil {
		t.Error("Expected error for non-existent parent directory")
	}
}

func TestBuilderZeroKeys(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "zero.idx")
	ctx := context.Background()
	_, err := NewSortedBuilder(ctx, indexPath, 0)
	if err == nil {
		t.Error("Expected error for zero keys")
	}
	if !errors.Is(err, sherr.ErrEmptyIndex) {
		t.Errorf("Expected ErrEmptyIndex, got %v", err)
	}
}

func TestBuilderDuplicateKeys(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "dup.idx")
	ctx := context.Background()

	// Create 10 distinct keys and 1 duplicate
	rng := newTestRNG(t)
	keys := make([][]byte, 11)
	for i := range 10 {
		keys[i] = make([]byte, 16)
		fillFromRNG(rng, keys[i])
	}
	// Duplicate first key
	keys[10] = make([]byte, 16)
	copy(keys[10], keys[0])

	slices.SortFunc(keys, bytes.Compare)

	builder, err := NewSortedBuilder(ctx, indexPath, uint64(len(keys)))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}

	var dupErr error
	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			dupErr = err
			break
		}
	}

	if dupErr == nil {
		dupErr = builder.Finish()
	} else {
		builder.Close()
	}

	if dupErr == nil {
		t.Error("Expected error for duplicate keys")
	} else if !errors.Is(dupErr, sherr.ErrDuplicateKey) {
		t.Errorf("Expected ErrDuplicateKey, got: %v", dupErr)
	}
}

func TestBuilderKeyCountMismatch(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	t.Run("TooFewKeys", func(t *testing.T) {
		indexPath := filepath.Join(tmpDir, "toofew.idx")
		builder, err := NewSortedBuilder(ctx, indexPath, 100)
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}

		for i := range 50 {
			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key[0:8], uint64(i+1))
			binary.BigEndian.PutUint64(key[8:16], uint64(i)*0x123)
			if err := builder.AddKey(key, 0); err != nil {
				builder.Close()
				t.Fatalf("AddKey failed: %v", err)
			}
		}

		err = builder.Finish()
		if err == nil {
			t.Error("Expected error for key count mismatch")
		} else if !errors.Is(err, sherr.ErrKeyCountMismatch) {
			t.Errorf("Expected ErrKeyCountMismatch, got: %v", err)
		}
	})

	t.Run("TooManyKeys", func(t *testing.T) {
		indexPath := filepath.Join(tmpDir, "toomany2.idx")
		builder, err := NewSortedBuilder(ctx, indexPath, 5)
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}
		var addErr error
		for i := range 10 {
			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key[0:8], uint64(i+1))
			binary.BigEndian.PutUint64(key[8:16], uint64(i)*0x123)
			if err := builder.AddKey(key, 0); err != nil {
				addErr = err
				break
			}
		}
		if addErr == nil {
			addErr = builder.Finish()
		} else {
			builder.Close()
		}
		if addErr == nil {
			t.Error("Expected error for too many keys")
		} else if !errors.Is(addErr, sherr.ErrKeyCountMismatch) {
			t.Errorf("Expected ErrKeyCountMismatch, got: %v", addErr)
		}
	})
}

func TestParallelBuilderEmptyBlocksOnly(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "empty_blocks.idx")
	ctx := context.Background()
	builder, err := NewSortedBuilder(ctx, indexPath, 1000, WithWorkers(2))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}
	err = builder.Finish()
	if err == nil {
		t.Error("Expected error when finishing with no keys added")
	} else if !errors.Is(err, sherr.ErrKeyCountMismatch) {
		t.Errorf("Expected ErrKeyCountMismatch, got: %v", err)
	}
	builder.Close()
}

func TestFinishAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "finish_after_close.idx")
	ctx := context.Background()
	numKeys := 10
	builder, err := NewSortedBuilder(ctx, indexPath, uint64(numKeys))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}
	keys := make([][]byte, numKeys)
	for i := range numKeys {
		src := make([]byte, 20)
		src[0] = byte(i >> 8)
		src[1] = byte(i)
		for j := 2; j < 20; j++ {
			src[j] = byte(i*17 + j)
		}
		keys[i] = PreHash(src)
	}
	sortKeysByBlock(keys, uint64(numKeys), nil)
	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			builder.Close()
			t.Fatalf("AddKey failed: %v", err)
		}
	}
	builder.Close()
	err = builder.Finish()
	if !errors.Is(err, sherr.ErrBuilderClosed) {
		t.Errorf("Expected ErrBuilderClosed after Close+Finish, got: %v", err)
	}
}

func TestAddKeyAfterFinish(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "add_after_finish.idx")
	ctx := context.Background()
	numKeys := 10
	builder, err := NewSortedBuilder(ctx, indexPath, uint64(numKeys))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}
	keys := make([][]byte, numKeys)
	for i := range numKeys {
		src := make([]byte, 20)
		src[0] = byte(i >> 8)
		src[1] = byte(i)
		for j := 2; j < 20; j++ {
			src[j] = byte(i*17 + j)
		}
		keys[i] = PreHash(src)
	}
	sortKeysByBlock(keys, uint64(numKeys), nil)
	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			builder.Close()
			t.Fatalf("AddKey failed: %v", err)
		}
	}
	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	extraSrc := make([]byte, 20)
	extraSrc[0] = 0xFF
	for j := 1; j < 20; j++ {
		extraSrc[j] = byte(j * 31)
	}
	extraKey := PreHash(extraSrc)
	err = builder.AddKey(extraKey, 0)
	if !errors.Is(err, sherr.ErrBuilderClosed) {
		t.Errorf("Expected ErrBuilderClosed, got: %v", err)
	}
}

func TestUnsortedBuilder_KeyTooShort(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	output := filepath.Join(tmpDir, "unsorted_short.idx")
	builder, err := NewUnsortedBuilder(ctx, output, 100,
		tmpDir,
		WithPayload(4),
	)
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}
	defer builder.Close()
	shortKey := make([]byte, 8)
	err = builder.AddKey(shortKey, 0)
	if !errors.Is(err, sherr.ErrKeyTooShort) {
		t.Errorf("Expected ErrKeyTooShort, got %v", err)
	}
}

func TestUnsortedBuilder_KeyCountMismatch(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	t.Run("UnsortedTooFewKeys", func(t *testing.T) {
		output := filepath.Join(tmpDir, "unsorted_toofew.idx")
		builder, err := NewUnsortedBuilder(ctx, output, 100,
			tmpDir,
			WithPayload(4),
		)
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}
		for i := range 50 {
			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(key[8:], uint64(i*2))
			if err := builder.AddKey(key, uint64(i)); err != nil {
				builder.Close()
				t.Fatalf("AddKey failed: %v", err)
			}
		}
		err = builder.Finish()
		if err == nil {
			t.Error("Expected error for key count mismatch, got nil")
		} else if !errors.Is(err, sherr.ErrKeyCountMismatch) {
			t.Errorf("Expected ErrKeyCountMismatch, got: %v", err)
		}
	})
	t.Run("UnsortedTooManyKeys", func(t *testing.T) {
		output := filepath.Join(tmpDir, "unsorted_toomany.idx")
		builder, err := NewUnsortedBuilder(ctx, output, 50,
			tmpDir,
			WithPayload(4),
		)
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}
		var addErr error
		for i := range 100 {
			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key, uint64(i))
			binary.BigEndian.PutUint64(key[8:], uint64(i*2))
			if err := builder.AddKey(key, uint64(i)); err != nil {
				addErr = err
				break
			}
		}
		if addErr == nil {
			addErr = builder.Finish()
		} else {
			builder.Close()
		}
		if addErr == nil {
			t.Error("Expected error for adding too many keys, got nil")
		} else if !errors.Is(addErr, sherr.ErrKeyCountMismatch) {
			t.Errorf("Expected ErrKeyCountMismatch, got: %v", addErr)
		}
	})
}

func TestUnsortedModeInvalidTempDir(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")
	ctx := context.Background()
	ub, err := NewUnsortedBuilder(ctx, indexPath, 100,
		"/nonexistent/directory",
	)
	if err != nil {
		return // Constructor caught the error
	}
	defer ub.Close()
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], 1)
	binary.BigEndian.PutUint64(key[8:16], 2)
	err = ub.AddKey(key, 0)
	if err == nil {
		t.Error("Expected error for invalid temp directory")
	}
}

func TestUnsortedModeReadOnlyTempDir(t *testing.T) {
	tmpDir := t.TempDir()
	readOnlyDir := filepath.Join(tmpDir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0555); err != nil {
		t.Fatal(err)
	}

	indexPath := filepath.Join(tmpDir, "test.idx")
	ctx := context.Background()
	ub, err := NewUnsortedBuilder(ctx, indexPath, 100,
		readOnlyDir,
	)
	if err != nil {
		return // Constructor caught the error
	}
	defer ub.Close()
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], 1)
	binary.BigEndian.PutUint64(key[8:16], 2)
	err = ub.AddKey(key, 0)
	if err == nil {
		t.Error("Expected error for read-only temp directory")
	}
}

// ---------------------------------------------------------------------------
// Category 3: Configuration validation errors
// ---------------------------------------------------------------------------

func TestMaxPayloadSize(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	successPath := filepath.Join(tmpDir, "max_payload_ok.idx")
	builder, err := NewSortedBuilder(ctx, successPath, 1000, WithPayload(maxPayloadSize))
	if err != nil {
		t.Errorf("Expected success at max payload size, got %v", err)
	} else {
		builder.Close()
	}

	failPath := filepath.Join(tmpDir, "max_payload_fail.idx")
	_, err = NewSortedBuilder(ctx, failPath, 1000, WithPayload(maxPayloadSize+1))
	if !errors.Is(err, sherr.ErrPayloadTooLarge) {
		t.Errorf("Expected ErrPayloadTooLarge above max, got %v", err)
	}
}

func TestNegativePayloadSizeError(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	failPath := filepath.Join(tmpDir, "neg_payload.idx")
	_, err := NewSortedBuilder(ctx, failPath, 1000, WithPayload(-1))
	if !errors.Is(err, sherr.ErrPayloadTooLarge) {
		t.Errorf("Expected ErrPayloadTooLarge for negative payloadSize, got %v", err)
	}
}

func TestNegativeFingerprintSizeError(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()
	failPath := filepath.Join(tmpDir, "neg_fp.idx")
	_, err := NewSortedBuilder(ctx, failPath, 1000, WithFingerprint(-1))
	if !errors.Is(err, sherr.ErrFingerprintTooLarge) {
		t.Errorf("Expected ErrFingerprintTooLarge for negative fingerprintSize, got %v", err)
	}
}

func TestFingerprintSizeBoundary(t *testing.T) {
	tmpDir := t.TempDir()
	ctx := context.Background()

	successPath := filepath.Join(tmpDir, "fp4_ok.idx")
	builder, err := NewSortedBuilder(ctx, successPath, 1000, WithFingerprint(4))
	if err != nil {
		t.Errorf("FingerprintSize=4 should succeed, got error: %v", err)
	} else {
		builder.Close()
	}

	failPath := filepath.Join(tmpDir, "fp5_fail.idx")
	_, err = NewSortedBuilder(ctx, failPath, 1000, WithFingerprint(5))
	if !errors.Is(err, sherr.ErrFingerprintTooLarge) {
		t.Errorf("FingerprintSize=5 should return ErrFingerprintTooLarge, got: %v", err)
	}
}

func TestPayloadOverflow(t *testing.T) {
	tempDir := t.TempDir()
	tests := []struct {
		payloadSize int
		payload     uint64
		shouldFail  bool
	}{
		{1, 255, false},
		{1, 256, true},
		{2, 65535, false},
		{2, 65536, true},
		{4, 4294967295, false},
		{4, 4294967296, true},
		{8, ^uint64(0), false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("size%d_val%d", tc.payloadSize, tc.payload), func(t *testing.T) {
			path := filepath.Join(tempDir, fmt.Sprintf("test_%d_%d.idx", tc.payloadSize, tc.payload))
			builder, err := NewSortedBuilder(context.Background(), path, 10, WithPayload(tc.payloadSize))
			if err != nil {
				t.Fatal(err)
			}
			defer builder.Close()
			key := make([]byte, 16)
			binary.BigEndian.PutUint64(key, 1)
			err = builder.AddKey(key, tc.payload)
			if tc.shouldFail {
				if !errors.Is(err, sherr.ErrPayloadOverflow) {
					t.Errorf("Expected ErrPayloadOverflow for payload %d with size %d, got %v", tc.payload, tc.payloadSize, err)
				}
			} else {
				if err != nil {
					t.Errorf("Expected success for payload %d with size %d, got %v", tc.payload, tc.payloadSize, err)
				}
			}
		})
	}
}

func TestKeyTooLong(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := tmpDir + "/long_key.idx"
	longKey := make([]byte, 70000)
	for i := range longKey {
		longKey[i] = byte(i % 256)
	}
	ctx := context.Background()
	keyIter := func(yield func([]byte, uint64) bool) {
		yield(longKey, 0)
	}
	err := buildUnsortedFromIter(ctx, indexPath, keyIter)
	if !errors.Is(err, sherr.ErrKeyTooLong) {
		t.Errorf("Expected ErrKeyTooLong, got: %v", err)
	}
}

func TestKeyTooShortForFingerprint(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "short_key_fp.idx")
	ctx := context.Background()
	builder, err := NewSortedBuilder(ctx, indexPath, 10, WithFingerprint(4))
	if err != nil {
		t.Fatalf("newBuilder failed: %v", err)
	}
	shortKey := make([]byte, 2)
	err = builder.AddKey(shortKey, 0)
	if !errors.Is(err, sherr.ErrKeyTooShort) {
		t.Errorf("Expected ErrKeyTooShort, got: %v", err)
	}
	builder.Close()
}

// ---------------------------------------------------------------------------
// Category 4: Query errors
// ---------------------------------------------------------------------------

func TestQueryWrongKeySize(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "test.idx")

	err := createSmallValidIndex(indexPath)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Empty key
	_, err = idx.QueryRank([]byte{})
	if !errors.Is(err, sherr.ErrKeyTooShort) {
		t.Errorf("Expected ErrKeyTooShort for empty key, got %v", err)
	}

	// Short key
	_, err = idx.QueryRank([]byte{0x01, 0x02})
	if !errors.Is(err, sherr.ErrKeyTooShort) {
		t.Errorf("Expected ErrKeyTooShort for short key, got %v", err)
	}
}

func TestQueryNoPayload(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := tmpDir + "/no_payload.idx"
	numKeys := 100
	rng := newTestRNG(t)
	entries := make([]entry, numKeys)
	for i := range entries {
		entries[i].Key = make([]byte, 32)
		fillFromRNG(rng, entries[i].Key)
	}
	slices.SortFunc(entries, func(a, b entry) int {
		return bytes.Compare(a.Key, b.Key)
	})
	ctx := context.Background()
	err := buildFromEntries(ctx, indexPath, entries)
	if err != nil {
		t.Fatalf("buildFromEntries failed: %v", err)
	}
	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()
	_, err = idx.WithPayload()
	if !errors.Is(err, sherr.ErrNoPayload) {
		t.Errorf("QueryPayload on no-payload index: got %v, want ErrNoPayload", err)
	}
}

func TestCloseAndQuery(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := tmpDir + "/close_query.idx"
	numKeys := 1000
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 32)
	ctx := context.Background()
	err := quickBuild(ctx, indexPath, keys)
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Verify queries work before close
	if _, err := idx.QueryRank(keys[0]); err != nil {
		t.Fatalf("Query before close failed: %v", err)
	}

	// Close the index, then verify queries return ErrIndexClosed.
	// Per the documented thread safety contract, Close must only be
	// called after all queries have completed.
	if err := idx.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	_, err = idx.QueryRank(keys[0])
	if !errors.Is(err, sherr.ErrIndexClosed) {
		t.Errorf("Query after close: got %v, want ErrIndexClosed", err)
	}

	_, _, err = (&PayloadIndex{idx}).QueryPayload(keys[0])
	if !errors.Is(err, sherr.ErrIndexClosed) {
		t.Errorf("QueryPayload after close: got %v, want ErrIndexClosed", err)
	}

	err = idx.Verify()
	if !errors.Is(err, sherr.ErrIndexClosed) {
		t.Errorf("Verify after close: got %v, want ErrIndexClosed", err)
	}

	// Double close is safe (returns nil)
	if err := idx.Close(); err != nil {
		t.Errorf("Double close: got %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// Category 5: Decode errors
// ---------------------------------------------------------------------------

func TestDecodeHeaderErrors(t *testing.T) {
	t.Run("Truncated", func(t *testing.T) {
		data := make([]byte, 32)
		_, err := decodeHeader(data)
		if !errors.Is(err, sherr.ErrTruncatedFile) {
			t.Errorf("Expected ErrTruncatedFile, got %v", err)
		}
	})
	t.Run("InvalidMagic", func(t *testing.T) {
		hdr := &header{
			Magic:   0xDEADBEEF,
			Version: version,
		}
		data := make([]byte, headerSize)
		hdr.encodeTo(data)
		_, err := decodeHeader(data)
		if !errors.Is(err, sherr.ErrInvalidMagic) {
			t.Errorf("Expected ErrInvalidMagic, got %v", err)
		}
	})
	t.Run("InvalidVersion", func(t *testing.T) {
		hdr := &header{
			Magic:   magic,
			Version: 0x9999,
		}
		data := make([]byte, headerSize)
		hdr.encodeTo(data)
		_, err := decodeHeader(data)
		if !errors.Is(err, sherr.ErrInvalidVersion) {
			t.Errorf("Expected ErrInvalidVersion, got %v", err)
		}
	})

	// F10: decodeHeader corruption guards (header.go:100-108)
	t.Run("PayloadSizeTooLarge", func(t *testing.T) {
		hdr := &header{
			Magic:       magic,
			Version:     version,
			TotalKeys:   100,
			NumBlocks:   4,
			PayloadSize: uint32(maxPayloadSize) + 1,
		}
		data := make([]byte, headerSize)
		hdr.encodeTo(data)
		_, err := decodeHeader(data)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("Expected ErrCorruptedIndex for PayloadSize > maxPayloadSize, got %v", err)
		}
	})

	t.Run("FingerprintSizeTooLarge", func(t *testing.T) {
		hdr := &header{
			Magic:           magic,
			Version:         version,
			TotalKeys:       100,
			NumBlocks:       4,
			FingerprintSize: uint8(maxFingerprintSize) + 1,
		}
		data := make([]byte, headerSize)
		hdr.encodeTo(data)
		_, err := decodeHeader(data)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("Expected ErrCorruptedIndex for FingerprintSize > maxFingerprintSize, got %v", err)
		}
	})

	t.Run("ZeroBlocksWithKeys", func(t *testing.T) {
		hdr := &header{
			Magic:     magic,
			Version:   version,
			TotalKeys: 100,
			NumBlocks: 0,
		}
		data := make([]byte, headerSize)
		hdr.encodeTo(data)
		_, err := decodeHeader(data)
		if !errors.Is(err, sherr.ErrCorruptedIndex) {
			t.Errorf("Expected ErrCorruptedIndex for NumBlocks==0 with TotalKeys>0, got %v", err)
		}
	})
}

func TestDecodeFooterErrors(t *testing.T) {
	data := make([]byte, 16)
	_, err := decodeFooter(data)
	if !errors.Is(err, sherr.ErrTruncatedFile) {
		t.Errorf("Expected ErrTruncatedFile, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Category 6: GetStats errors
// ---------------------------------------------------------------------------

func TestGetStatsNonExistentFile(t *testing.T) {
	_, err := GetStats("/nonexistent/path/file.idx")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}

func TestGetStatsCorruptedFile(t *testing.T) {
	tmpDir := t.TempDir()
	corruptFile := filepath.Join(tmpDir, "corrupt.idx")
	if err := os.WriteFile(corruptFile, []byte("not a valid index file"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := GetStats(corruptFile)
	if err == nil {
		t.Fatal("Expected error for corrupted file")
	}
	if !errors.Is(err, sherr.ErrTruncatedFile) {
		t.Errorf("Expected ErrTruncatedFile for sub-header-size file, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// F7: ErrTooManyKeys (builder.go:117)
// ---------------------------------------------------------------------------

func TestErrTooManyKeys(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "toomany.idx")
	ctx := context.Background()

	_, err := NewSortedBuilder(ctx, indexPath, maxKeys+1)
	if err == nil {
		t.Fatal("Expected error for totalKeys exceeding maxKeys")
	}
	if !errors.Is(err, sherr.ErrTooManyKeys) {
		t.Errorf("Expected ErrTooManyKeys, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// F8: ErrChecksumFailed in Verify() (index.go:443-456)
// ---------------------------------------------------------------------------

func TestErrChecksumFailed(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "checksum.idx")
	ctx := context.Background()

	// Build an index with payload so there is a non-empty payload region to corrupt.
	numKeys := 200
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 32)
	payloads := make([]uint64, numKeys)
	for i := range payloads {
		payloads[i] = uint64(i + 1)
	}

	entries := make([]entry, numKeys)
	for i := range entries {
		entries[i] = entry{Key: keys[i], Payload: payloads[i]}
	}

	if err := buildFromEntries(ctx, indexPath, entries, WithPayload(4)); err != nil {
		t.Fatalf("buildFromEntries failed: %v", err)
	}

	// Read the file, corrupt a byte in the payload region, write back.
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}

	// Open the uncorrupted index first to find the payload region offset.
	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	payloadOff := idx.payloadRegionOffset
	idx.Close()

	// Flip a byte in the payload region.
	if payloadOff >= uint64(len(data))-footerSize {
		t.Fatal("payloadRegionOffset is beyond data bounds")
	}
	data[payloadOff] ^= 0xFF

	corruptPath := filepath.Join(tmpDir, "checksum_corrupt.idx")
	if err := os.WriteFile(corruptPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	idx2, err := Open(corruptPath)
	if err != nil {
		t.Fatalf("Open on corrupted file failed: %v", err)
	}
	defer idx2.Close()

	err = idx2.Verify()
	if !errors.Is(err, sherr.ErrChecksumFailed) {
		t.Errorf("Expected ErrChecksumFailed, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// F9: Non-member rejection via ErrNotFound (index.go:269,297)
// ---------------------------------------------------------------------------

func TestNonMemberRejection(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "nonmember.idx")
	ctx := context.Background()

	// Build an index with fingerprints enabled.
	numKeys := 500
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 32)

	if err := quickBuild(ctx, indexPath, keys, WithFingerprint(2)); err != nil {
		t.Fatalf("quickBuild failed: %v", err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	// Query many non-member keys. With 2-byte fingerprints (FPR ~ 1/65536),
	// virtually all should fail with ErrNotFound.
	numProbes := 1000
	notFoundCount := 0
	otherErrCount := 0
	for i := range numProbes {
		nonMember := make([]byte, 32)
		binary.BigEndian.PutUint64(nonMember[0:8], uint64(0xBADBAD0000000000)|uint64(i))
		fillFromRNG(rng, nonMember[8:])

		_, err := idx.QueryRank(nonMember)
		if errors.Is(err, sherr.ErrNotFound) {
			notFoundCount++
		} else if err != nil {
			otherErrCount++
		}
		// err == nil means false positive (non-member accepted), expected to be rare
	}

	// No unexpected error types should occur.
	if otherErrCount > 0 {
		t.Errorf("got %d unexpected errors (not ErrNotFound)", otherErrCount)
	}

	// Virtually all non-member queries should be rejected.
	if notFoundCount < numProbes*9/10 {
		t.Errorf("Non-member rejection too low: %d/%d rejected", notFoundCount, numProbes)
	}
}

// ---------------------------------------------------------------------------
// F11: Unknown algorithm ID (algorithm.go:187-214)
// ---------------------------------------------------------------------------

func TestUnknownAlgorithmID(t *testing.T) {
	t.Run("Builder", func(t *testing.T) {
		_, err := newBlockBuilder(Algorithm(99), 1000, 0, 0, 0)
		if err == nil {
			t.Fatal("Expected error for unknown algorithm ID")
		}
		if !errors.Is(err, sherr.ErrInvalidGeometry) {
			t.Errorf("Expected ErrInvalidGeometry, got %v", err)
		}
	})

	t.Run("Decoder", func(t *testing.T) {
		_, err := newBlockDecoder(Algorithm(99), nil, 0)
		if err == nil {
			t.Fatal("Expected error for unknown algorithm ID")
		}
		if !errors.Is(err, sherr.ErrInvalidGeometry) {
			t.Errorf("Expected ErrInvalidGeometry, got %v", err)
		}
	})
}
