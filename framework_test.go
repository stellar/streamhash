// framework_test.go tests the framework infrastructure of the streamhash package:
// prefix extraction, block routing, serialization (header, footer, pack/unpack),
// PreHash, index accessors, stats, fingerprint extraction, fingerprint separation
// verification, and empty-block dispatch. These are functions that don't
// individually warrant separate files but share the same test binary.
//
// Unsorted buffer tests live in builder_unsorted_test.go.
package streamhash

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/fnv"
	"math"
	randv2 "math/rand/v2"
	"slices"
	"testing"
)

// Named seeds for deterministic reproduction.
const (
	testSeed1 = 0x1234567890ABCDEF
	testSeed2 = 0xFEDCBA9876543210
)

// packFingerprintToBytes writes a uint32 fingerprint to a byte slice.
// Test-only helper; production code uses unpackFingerprintFromBytes for reading.
func packFingerprintToBytes(dst []byte, fp uint32, size int) {
	switch size {
	case 1:
		dst[0] = byte(fp)
	case 2:
		binary.LittleEndian.PutUint16(dst, uint16(fp))
	case 4:
		binary.LittleEndian.PutUint32(dst, fp)
	default:
		for i := 0; i < size && i < 4; i++ {
			dst[i] = byte(fp >> (i * 8))
		}
	}
}

func newTestRNG(t testing.TB) *randv2.Rand {
	t.Helper()
	h := fnv.New128a()
	h.Write([]byte(t.Name()))
	sum := h.Sum(nil)
	s1 := binary.LittleEndian.Uint64(sum[:8])
	s2 := binary.LittleEndian.Uint64(sum[8:])
	return randv2.New(randv2.NewPCG(testSeed1^s1, testSeed2^s2))
}

// =============================================================================
// ExtractPrefix tests
// =============================================================================

func TestExtractPrefix(t *testing.T) {
	key := []byte{0xA4, 0xCD, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0x11, 0x22}
	prefix := extractPrefix(key)
	expected := uint64(0xA4CD123456789ABC)
	if prefix != expected {
		t.Errorf("extractPrefix: expected 0x%X, got 0x%X", expected, prefix)
	}
}

// =============================================================================
// NumBlocks tests
// =============================================================================

func TestNumBlocksEdgeCases(t *testing.T) {
	algos := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}
	keyCounts := []uint64{1, 2, 3, 100, 1000, 10000, 1000000, 1 << 20, 1 << 25}

	for _, alg := range algos {
		t.Run(alg.name, func(t *testing.T) {
			numBlocksAt := make(map[uint64]uint32)
			for _, n := range keyCounts {
				nb, err := numBlocksForAlgo(alg.algo, n, 0, 0)
				if err != nil {
					t.Errorf("N=%d: unexpected error %v", n, err)
				} else {
					if nb == 0 {
						t.Errorf("N=%d: NumBlocks should not be 0", n)
					}
					numBlocksAt[n] = nb
				}
			}

			if numBlocksAt[10000] < numBlocksAt[100] {
				t.Errorf("numBlocks should increase with n: n=100 has %d blocks, n=10000 has %d blocks",
					numBlocksAt[100], numBlocksAt[10000])
			}
		})
	}
}

func TestNumBlocksOverflowProtection(t *testing.T) {
	algos := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}
	scales := []struct {
		name string
		n    uint64
	}{
		{"1M keys", 1_000_000},
		{"100M keys", 100_000_000},
		{"1B keys", 1_000_000_000},
		{"10B keys", 10_000_000_000},
		{"100B keys", 100_000_000_000},
		{"1T keys", 1_000_000_000_000},
	}

	for _, alg := range algos {
		t.Run(alg.name, func(t *testing.T) {
			for _, tc := range scales {
				t.Run(tc.name, func(t *testing.T) {
					nb, err := numBlocksForAlgo(alg.algo, tc.n, 0, 0)
					if err != nil {
						t.Errorf("Unexpected error for N=%d: %v", tc.n, err)
						return
					}
					if nb == 0 {
						t.Errorf("NumBlocks should not be 0 for N=%d", tc.n)
					}
				})
			}
		})
	}
}

// =============================================================================
// BlockIndex tests
// =============================================================================

func TestBlockIndexConsistency(t *testing.T) {
	numKeys := uint64(10000)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 0, 0)
	if err != nil {
		t.Fatalf("numBlocksForAlgo failed: %v", err)
	}

	rng := newTestRNG(t)
	for range 1000 {
		prefix := rng.Uint64()
		blockIdx := blockIndexFromPrefix(prefix, numBlocks)

		if blockIdx >= numBlocks {
			t.Errorf("Block index %d out of range [0, %d)", blockIdx, numBlocks)
		}

		blockIdx2 := blockIndexFromPrefix(prefix, numBlocks)
		if blockIdx != blockIdx2 {
			t.Errorf("Block routing not deterministic: %d != %d for prefix %x", blockIdx, blockIdx2, prefix)
		}
	}
}

func TestBlockOrderWithExtractPrefix(t *testing.T) {
	numKeys := 10000

	rng := newTestRNG(t)
	keys := make([][32]byte, numKeys)
	for i := range keys {
		for j := range 4 {
			binary.LittleEndian.PutUint64(keys[i][j*8:], rng.Uint64())
		}
	}

	slices.SortFunc(keys, func(a, b [32]byte) int {
		for i := range a {
			if a[i] != b[i] {
				if a[i] < b[i] {
					return -1
				}
				return 1
			}
		}
		return 0
	})

	numBlocks := uint32(6)

	lastBlockIdx := int64(-1)
	violations := 0
	for i, key := range keys {
		prefix := extractPrefix(key[:])
		blockIdx := blockIndexFromPrefix(prefix, numBlocks)

		if int64(blockIdx) < lastBlockIdx {
			if violations < 10 {
				t.Errorf("Monotonicity violation at key %d: block %d -> %d", i, lastBlockIdx, blockIdx)
			}
			violations++
		}
		lastBlockIdx = int64(blockIdx)
	}
	if violations > 10 {
		t.Errorf("%d total monotonicity violations (first 10 shown above)", violations)
	}
}

// =============================================================================
// DispatchEmptyBlock and VerifyFingerprint direct tests
// =============================================================================

func TestDispatchEmptyBlockDirect(t *testing.T) {
	tmpDir := t.TempDir()
	output := tmpDir + "/dispatch_direct.idx"

	t.Run("ContextCancelledParallel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		builder, err := NewSortedBuilder(ctx, output+"_cancel", 10000, WithWorkers(2))
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}

		src := make([]byte, 20)
		binary.BigEndian.PutUint64(src[:8], 0x1234567890ABCDEF)
		binary.BigEndian.PutUint64(src[8:16], 0x0FEDCBA987654321)
		key := PreHash(src)
		_ = builder.AddKey(key, 0) // Error ignored; test verifies dispatchEmptyBlock below.

		cancel()

		var lastErr error
		for i := range uint32(200) {
			err = builder.b.dispatchEmptyBlock(i)
			if err != nil {
				lastErr = err
				break
			}
		}

		if lastErr == nil {
			t.Error("Expected dispatchEmptyBlock to return error after context cancel")
		}

		builder.Close()
	})

	t.Run("NormalParallel", func(t *testing.T) {
		ctx := context.Background()

		builder, err := NewSortedBuilder(ctx, output+"_normal", 10000, WithWorkers(2))
		if err != nil {
			t.Fatalf("newBuilder failed: %v", err)
		}

		for i := range uint32(10) {
			err = builder.b.dispatchEmptyBlock(i)
			if err != nil {
				t.Errorf("dispatchEmptyBlock failed: %v", err)
				break
			}
		}

		builder.Close()
	})
}

func TestVerifyFingerprintSeparatedDirect(t *testing.T) {
	tmpDir := t.TempDir()
	output := tmpDir + "/verify_direct.idx"

	numKeys := 50
	fpSize := 2

	rng := newTestRNG(t)
	keys := make([][]byte, numKeys)
	for i := range keys {
		src := make([]byte, 20)
		fillFromRNG(rng, src)
		keys[i] = PreHash(src)
	}

	sortKeysByBlock(keys, uint64(numKeys), []BuildOption{WithFingerprint(fpSize)})

	ctx := context.Background()
	builder, err := NewSortedBuilder(ctx, output, uint64(numKeys), WithFingerprint(fpSize))
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

	idx, err := Open(output)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer idx.Close()

	t.Run("MatchingFingerprint", func(t *testing.T) {
		// First find the actual rank for keys[0]
		rank, err := idx.QueryRank(keys[0])
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		k0 := binary.LittleEndian.Uint64(keys[0][0:8])
		k1 := binary.LittleEndian.Uint64(keys[0][8:16])
		match, err := idx.verifyFingerprintSeparated(k0, k1, rank)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if !match {
			t.Error("Expected fingerprint match for member key at correct rank")
		}
	})

	t.Run("MismatchingFingerprint", func(t *testing.T) {
		// Create a non-member key and check its fingerprint is rejected
		nonMember := make([]byte, 16)
		binary.BigEndian.PutUint64(nonMember[0:8], 0xDEADDEADDEADDEAD)
		binary.BigEndian.PutUint64(nonMember[8:16], 0xBEEFBEEFBEEFBEEF)

		k0 := binary.LittleEndian.Uint64(nonMember[0:8])
		k1 := binary.LittleEndian.Uint64(nonMember[8:16])

		// Test with rank 0 — the fingerprint at slot 0 belongs to a member key,
		// so a non-member key's fingerprint should almost certainly not match.
		match, err := idx.verifyFingerprintSeparated(k0, k1, 0)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if match {
			// With 2-byte fingerprint, collision probability is 1/65536.
			// If this fires, the test key collided by pure chance — re-run.
			t.Error("Expected fingerprint mismatch for non-member key")
		}
	})
}

// =============================================================================
// PreHash tests
// =============================================================================

func TestPreHashFunction(t *testing.T) {
	key := []byte("test-key-for-prehash")
	hash1 := PreHash(key)
	hash2 := PreHash(key)

	if !bytes.Equal(hash1, hash2) {
		t.Error("PreHash is not deterministic")
	}

	if len(hash1) != 16 {
		t.Errorf("PreHash should return 16 bytes, got %d", len(hash1))
	}

	key2 := []byte("different-key-for-prehash")
	hash3 := PreHash(key2)

	if bytes.Equal(hash1, hash3) {
		t.Error("Different keys should produce different hashes")
	}
}

func TestPreHashInPlace(t *testing.T) {
	key := []byte("test-key-for-prehash")
	dst := make([]byte, 16)

	PreHashInPlace(dst, key)
	expected := PreHash(key)

	if !bytes.Equal(dst, expected) {
		t.Error("PreHashInPlace should produce same result as PreHash")
	}
}

func TestPreHashEmptyKey(t *testing.T) {
	hash := PreHash([]byte{})
	if len(hash) != 16 {
		t.Errorf("PreHash of empty key should return 16 bytes, got %d", len(hash))
	}
}

func TestPreHashVariousLengths(t *testing.T) {
	testCases := []int{1, 2, 4, 8, 15, 16, 17, 32, 64, 128, 256, 1024}

	for _, keyLen := range testCases {
		key := make([]byte, keyLen)
		for i := range key {
			key[i] = byte(i)
		}

		hash := PreHash(key)
		if len(hash) != 16 {
			t.Errorf("PreHash of %d-byte key should return 16 bytes, got %d", keyLen, len(hash))
		}

		hash2 := PreHash(key)
		if !bytes.Equal(hash, hash2) {
			t.Errorf("PreHash of %d-byte key is not deterministic", keyLen)
		}
	}
}

func TestPreHashInPlaceDoesNotModifySource(t *testing.T) {
	original := []byte("test-key-for-prehash")
	key := make([]byte, len(original))
	copy(key, original)

	dst := make([]byte, 16)
	PreHashInPlace(dst, key)

	if !bytes.Equal(key, original) {
		t.Error("PreHashInPlace should not modify source key")
	}
}

func TestPreHashUniqueness(t *testing.T) {
	n := 1000
	hashes := make(map[string]bool)

	rng := newTestRNG(t)
	for i := range n {
		key := make([]byte, 8)
		fillFromRNG(rng, key)

		hash := PreHash(key)
		hashStr := string(hash)

		if hashes[hashStr] {
			t.Errorf("Hash collision detected for key %d", i)
		}
		hashes[hashStr] = true
	}
}

// =============================================================================
// Index accessor tests
// =============================================================================

// TestIndexAccessors verifies trivial getters on Index.
func TestIndexAccessors(t *testing.T) {
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, 100, 24)
	slices.SortFunc(keys, bytes.Compare)

	payloads := make([]uint64, len(keys))
	for i := range payloads {
		payloads[i] = uint64(i)
	}

	idx := buildAndOpen(t, keys, payloads, WithPayload(4), WithFingerprint(1))
	defer idx.Close()

	if idx.NumBlocks() == 0 {
		t.Error("NumBlocks() returned 0")
	}
	if idx.PayloadSize() == 0 {
		t.Error("PayloadSize() returned 0 for index with payload")
	}
	if idx.PayloadSize() != 4 {
		t.Errorf("PayloadSize() = %d, want 4", idx.PayloadSize())
	}
}

// TestIndexAccessorsNoPayload verifies PayloadSize returns 0 for MPHF-only index.
func TestIndexAccessorsNoPayload(t *testing.T) {
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, 50, 24)
	slices.SortFunc(keys, bytes.Compare)

	idx := buildAndOpen(t, keys, nil)
	defer idx.Close()

	if idx.PayloadSize() != 0 {
		t.Error("PayloadSize() returned non-zero for MPHF-only index")
	}
	if idx.PayloadSize() != 0 {
		t.Errorf("PayloadSize() = %d, want 0", idx.PayloadSize())
	}
}

// TestAlgorithmString verifies Algorithm.String().
func TestAlgorithmString(t *testing.T) {
	tests := []struct {
		algo Algorithm
		want string
	}{
		{AlgoBijection, "bijection"},
		{AlgoPTRHash, "ptrhash"},
		{Algorithm(99), "unknown"},
	}
	for _, tc := range tests {
		got := tc.algo.String()
		if got != tc.want {
			t.Errorf("Algorithm(%d).String() = %q, want %q", tc.algo, got, tc.want)
		}
	}
}

// =============================================================================
// Serialization tests
// =============================================================================

// TestRAMIndexEntryRoundtrip verifies that ramIndexEntry round-trips
// through encode/decode for random 40-bit values.
func TestRAMIndexEntryRoundtrip(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 10000
	const mask40 = uint64(1)<<40 - 1

	buf := make([]byte, ramIndexEntrySize)

	for i := range iterations {
		e := ramIndexEntry{
			KeysBefore:     rng.Uint64() & mask40,
			MetadataOffset: rng.Uint64() & mask40,
		}

		encodeRAMIndexEntryTo(e, buf)
		got := decodeRAMIndexEntry(buf)

		if got.KeysBefore != e.KeysBefore {
			t.Fatalf("iter %d: KeysBefore: got %d, want %d", i, got.KeysBefore, e.KeysBefore)
		}
		if got.MetadataOffset != e.MetadataOffset {
			t.Fatalf("iter %d: MetadataOffset: got %d, want %d", i, got.MetadataOffset, e.MetadataOffset)
		}
	}
}

// TestHeaderRoundtrip verifies that header round-trips through
// encodeTo/decodeHeader for random valid field values.
func TestHeaderRoundtrip(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 1000

	buf := make([]byte, headerSize)

	for i := range iterations {
		h := header{
			Magic:           magic,
			Version:         version,
			TotalKeys:       rng.Uint64(),
			NumBlocks:       uint32(rng.IntN(10000-2)) + 2,                // [2, 10000)
			PayloadSize:     uint32(rng.IntN(int(maxPayloadSize) + 1)),    // [0, 8]
			FingerprintSize: uint8(rng.IntN(int(maxFingerprintSize) + 1)), // [0, 4]
			Seed:            rng.Uint64(),
			BlockAlgorithm:  Algorithm(rng.IntN(2)), // 0 or 1
		}

		h.encodeTo(buf)
		got, err := decodeHeader(buf)
		if err != nil {
			t.Fatalf("iter %d: decodeHeader failed: %v", i, err)
		}

		if got.Magic != h.Magic {
			t.Fatalf("iter %d: Magic: got %d, want %d", i, got.Magic, h.Magic)
		}
		if got.Version != h.Version {
			t.Fatalf("iter %d: Version: got %d, want %d", i, got.Version, h.Version)
		}
		if got.TotalKeys != h.TotalKeys {
			t.Fatalf("iter %d: TotalKeys: got %d, want %d", i, got.TotalKeys, h.TotalKeys)
		}
		if got.NumBlocks != h.NumBlocks {
			t.Fatalf("iter %d: NumBlocks: got %d, want %d", i, got.NumBlocks, h.NumBlocks)
		}
		if got.PayloadSize != h.PayloadSize {
			t.Fatalf("iter %d: PayloadSize: got %d, want %d", i, got.PayloadSize, h.PayloadSize)
		}
		if got.FingerprintSize != h.FingerprintSize {
			t.Fatalf("iter %d: FingerprintSize: got %d, want %d", i, got.FingerprintSize, h.FingerprintSize)
		}
		if got.Seed != h.Seed {
			t.Fatalf("iter %d: Seed: got %d, want %d", i, got.Seed, h.Seed)
		}
		if got.BlockAlgorithm != h.BlockAlgorithm {
			t.Fatalf("iter %d: BlockAlgorithm: got %d, want %d", i, got.BlockAlgorithm, h.BlockAlgorithm)
		}
	}
}

// =============================================================================
// Pack/Unpack tests
// =============================================================================

// TestFingerprintPackUnpack verifies that packFingerprintToBytes and
// unpackFingerprintFromBytes round-trip correctly for random values.
func TestFingerprintPackUnpack(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 10000

	for i := range iterations {
		fpSize := int(rng.IntN(5)) // 0-4
		if fpSize == 0 {
			// fpSize=0 -> unpack returns 0
			buf := make([]byte, 4)
			got := unpackFingerprintFromBytes(buf, 0)
			if got != 0 {
				t.Fatalf("iter %d: fpSize=0: got %d, want 0", i, got)
			}
			continue
		}

		var mask uint32
		if fpSize == 4 {
			mask = math.MaxUint32
		} else {
			mask = uint32(1)<<(fpSize*8) - 1
		}
		fp := rng.Uint32() & mask

		buf := make([]byte, 4)
		packFingerprintToBytes(buf, fp, fpSize)
		got := unpackFingerprintFromBytes(buf, fpSize)

		if got != fp {
			t.Fatalf("iter %d: fpSize=%d fp=0x%X: got 0x%X", i, fpSize, fp, got)
		}
	}

	// Explicit max values for each size
	for fpSize := 1; fpSize <= 4; fpSize++ {
		var fp uint32
		if fpSize == 4 {
			fp = math.MaxUint32
		} else {
			fp = uint32(1)<<(fpSize*8) - 1
		}
		buf := make([]byte, 4)
		packFingerprintToBytes(buf, fp, fpSize)
		got := unpackFingerprintFromBytes(buf, fpSize)
		if got != fp {
			t.Errorf("max fpSize=%d: got 0x%X, want 0x%X", fpSize, got, fp)
		}
	}
}

// TestPayloadPackUnpack verifies that packPayloadToBytes and
// unpackPayloadFromBytes round-trip correctly for random values.
func TestPayloadPackUnpack(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 10000

	for i := range iterations {
		payloadSize := int(rng.IntN(8)) + 1 // 1-8

		var mask uint64
		if payloadSize >= 8 {
			mask = math.MaxUint64
		} else {
			mask = uint64(1)<<(payloadSize*8) - 1
		}
		payload := rng.Uint64() & mask

		buf := make([]byte, 8)
		packPayloadToBytes(buf, payload, payloadSize)
		got := unpackPayloadFromBytes(buf, payloadSize)

		if got != payload {
			t.Fatalf("iter %d: payloadSize=%d payload=0x%X: got 0x%X", i, payloadSize, payload, got)
		}
	}

	// Explicit max values for each size
	for payloadSize := 1; payloadSize <= 8; payloadSize++ {
		var payload uint64
		if payloadSize >= 8 {
			payload = math.MaxUint64
		} else {
			payload = uint64(1)<<(payloadSize*8) - 1
		}
		buf := make([]byte, 8)
		packPayloadToBytes(buf, payload, payloadSize)
		got := unpackPayloadFromBytes(buf, payloadSize)
		if got != payload {
			t.Errorf("max payloadSize=%d: got 0x%X, want 0x%X", payloadSize, got, payload)
		}
	}
}

// =============================================================================
// extractFingerprint tests
// =============================================================================

// TestExtractFingerprintKnownValues verifies extractFingerprint against
// all 11 known-value tuples from the original test.
func TestExtractFingerprintKnownValues(t *testing.T) {
	testCases := []struct {
		k0, k1 uint64
		fpSize int
		want   uint32
	}{
		// k1=0 degenerate (fpSize 0-4)
		{0xDEADBEEFCAFEBABE, 0x0, 0, 0},
		{0xDEADBEEFCAFEBABE, 0x0, 1, 0xEF},
		{0xDEADBEEFCAFEBABE, 0x0, 2, 0xBEEF},
		{0xDEADBEEFCAFEBABE, 0x0, 3, 0xADBEEF},
		{0xDEADBEEFCAFEBABE, 0x0, 4, 0xDEADBEEF},
		// Both halves non-zero (fpSize 1-4)
		{0x123456789ABCDEF0, 0xFEDCBA9876543210, 1, 0xE3},
		{0x123456789ABCDEF0, 0xFEDCBA9876543210, 2, 0xA5E3},
		{0x123456789ABCDEF0, 0xFEDCBA9876543210, 3, 0x82A5E3},
		{0x123456789ABCDEF0, 0xFEDCBA9876543210, 4, 0x4582A5E3},
		// k0=0
		{0x0, 0xFFFFFFFFFFFFFFFF, 4, 0xAE833E48},
		// All-ones k0 + small k1
		{0xFFFFFFFFFFFFFFFF, 0x1, 4, 0xAE833E48},
	}

	for i, tc := range testCases {
		got := extractFingerprint(tc.k0, tc.k1, tc.fpSize)
		if got != tc.want {
			t.Errorf("case %d: extractFingerprint(0x%X, 0x%X, %d) = 0x%X, want 0x%X",
				i, tc.k0, tc.k1, tc.fpSize, got, tc.want)
		}
	}
}

// TestExtractFingerprintBijectivity verifies that for a fixed k0,
// 1000 distinct k1 values produce 1000 distinct fingerprints (high 32 bits).
//
// Since fingerprintMixer is odd, k1*fingerprintMixer is bijective on uint64,
// so k0^(k1*C) produces distinct 64-bit values for distinct k1. The high 32
// bits being distinct for 1000 inputs is expected (birthday bound ~2^16).
func TestExtractFingerprintBijectivity(t *testing.T) {
	k0 := uint64(0xDEADBEEFCAFEBABE)
	seen := make(map[uint32]bool)

	for k1 := range uint64(1000) {
		// Verify extractFingerprint matches manual computation
		fp := extractFingerprint(k0, k1, 4)
		h := k0 ^ (k1 * fingerprintMixer)
		expected := uint32(h >> 32)
		if fp != expected {
			t.Fatalf("k1=%d: extractFingerprint=0x%X, manual=0x%X", k1, fp, expected)
		}
		if seen[fp] {
			t.Fatalf("collision at k1=%d: fp=0x%X already seen", k1, fp)
		}
		seen[fp] = true
	}
}

// TestExtractFingerprintK0Sensitivity verifies that varying k0 alone
// changes the fingerprint. Since the formula is k0 ^ (k1 * C), different
// k0 values with the same k1 must produce different fingerprints.
func TestExtractFingerprintK0Sensitivity(t *testing.T) {
	k1 := uint64(0xFEDCBA9876543210)
	fpSize := 4
	seen := make(map[uint32]bool)

	rng := newTestRNG(t)
	for range uint64(1000) {
		k0 := rng.Uint64()
		fp := extractFingerprint(k0, k1, fpSize)
		if seen[fp] {
			t.Fatalf("collision at k0=0x%X: fp=0x%X already seen", k0, fp)
		}
		seen[fp] = true
	}
}

// TestFingerprintMixerOddness verifies that fingerprintMixer is odd
// (required for bijective multiplication on uint64).
func TestFingerprintMixerOddness(t *testing.T) {
	if fingerprintMixer&1 != 1 {
		t.Fatalf("fingerprintMixer = 0x%X is even, must be odd", fingerprintMixer)
	}
}

// =============================================================================
// Stats tests
// =============================================================================

// TestStatsFields builds an index with known params and asserts ALL Stats fields.
func TestStatsFields(t *testing.T) {
	t.Run("WithFingerprint", func(t *testing.T) {
		numKeys := 500
		payloadSize := 4
		fpSize := 1

		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, numKeys, 24)
		payloads := make([]uint64, numKeys)
		for i := range payloads {
			payloads[i] = uint64(i)
		}
		sortKeysAndPayloads(keys, payloads)

		idx := buildAndOpen(t, keys, payloads, WithPayload(payloadSize), WithFingerprint(fpSize))
		defer idx.Close()

		expectedBlocks, err := numBlocksForAlgo(AlgoBijection, uint64(numKeys), payloadSize, fpSize)
		if err != nil {
			t.Fatalf("numBlocksForAlgo failed: %v", err)
		}

		stats := idx.Stats()

		if stats.NumKeys != uint64(numKeys) {
			t.Errorf("NumKeys: got %d, want %d", stats.NumKeys, numKeys)
		}
		if stats.NumBlocks != expectedBlocks {
			t.Errorf("NumBlocks: got %d, want %d", stats.NumBlocks, expectedBlocks)
		}
		if stats.PayloadSize != payloadSize {
			t.Errorf("PayloadSize: got %d, want %d", stats.PayloadSize, payloadSize)
		}
		if stats.FingerprintSize == 0 {
			t.Error("FingerprintSize: got 0, want > 0")
		}
		if stats.FileSize <= 0 {
			t.Errorf("FileSize: got %d, want > 0", stats.FileSize)
		}
		if stats.BitsPerKey <= 0 {
			t.Errorf("BitsPerKey: got %f, want > 0", stats.BitsPerKey)
		}
	})

	t.Run("NoFingerprint", func(t *testing.T) {
		numKeys := 500
		payloadSize := 4

		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, numKeys, 24)
		payloads := make([]uint64, numKeys)
		for i := range payloads {
			payloads[i] = uint64(i)
		}
		sortKeysAndPayloads(keys, payloads)

		idx := buildAndOpen(t, keys, payloads, WithPayload(payloadSize))
		defer idx.Close()

		expectedBlocks, err := numBlocksForAlgo(AlgoBijection, uint64(numKeys), payloadSize, 0)
		if err != nil {
			t.Fatalf("numBlocksForAlgo failed: %v", err)
		}

		stats := idx.Stats()

		if stats.NumKeys != uint64(numKeys) {
			t.Errorf("NumKeys: got %d, want %d", stats.NumKeys, numKeys)
		}
		if stats.NumBlocks != expectedBlocks {
			t.Errorf("NumBlocks: got %d, want %d", stats.NumBlocks, expectedBlocks)
		}
		if stats.PayloadSize != payloadSize {
			t.Errorf("PayloadSize: got %d, want %d", stats.PayloadSize, payloadSize)
		}
		if stats.FingerprintSize > 0 {
			t.Errorf("FingerprintSize: got %d, want 0", stats.FingerprintSize)
		}
		if stats.FileSize <= 0 {
			t.Errorf("FileSize: got %d, want > 0", stats.FileSize)
		}
		if stats.BitsPerKey <= 0 {
			t.Errorf("BitsPerKey: got %f, want > 0", stats.BitsPerKey)
		}
	})
}

// =============================================================================
// Footer round-trip tests
// =============================================================================

// TestFooterRoundtrip creates a footer with known values, calls encodeTo,
// then decodeFooter, and verifies all fields match including reserved bytes.
func TestFooterRoundtrip(t *testing.T) {
	rng := newTestRNG(t)

	for i := range 100 {
		original := footer{
			PayloadRegionHash:  rng.Uint64(),
			MetadataRegionHash: rng.Uint64(),
		}
		// Fill reserved bytes with non-zero data to verify round-trip
		for j := range original.Reserved {
			original.Reserved[j] = byte(rng.IntN(256))
		}

		buf := make([]byte, footerSize)
		original.encodeTo(buf)

		decoded, err := decodeFooter(buf)
		if err != nil {
			t.Fatalf("iter %d: decodeFooter failed: %v", i, err)
		}

		if decoded.PayloadRegionHash != original.PayloadRegionHash {
			t.Fatalf("iter %d: PayloadRegionHash: got 0x%X, want 0x%X",
				i, decoded.PayloadRegionHash, original.PayloadRegionHash)
		}
		if decoded.MetadataRegionHash != original.MetadataRegionHash {
			t.Fatalf("iter %d: MetadataRegionHash: got 0x%X, want 0x%X",
				i, decoded.MetadataRegionHash, original.MetadataRegionHash)
		}
		if decoded.Reserved != original.Reserved {
			t.Fatalf("iter %d: Reserved: got %v, want %v",
				i, decoded.Reserved, original.Reserved)
		}
	}

	// Edge case: zero footer
	zeroFooter := footer{}
	buf := make([]byte, footerSize)
	zeroFooter.encodeTo(buf)
	decoded, err := decodeFooter(buf)
	if err != nil {
		t.Fatalf("zero footer: decodeFooter failed: %v", err)
	}
	if decoded.PayloadRegionHash != 0 || decoded.MetadataRegionHash != 0 {
		t.Fatalf("zero footer: hashes not zero: payload=0x%X metadata=0x%X",
			decoded.PayloadRegionHash, decoded.MetadataRegionHash)
	}
	for i, b := range decoded.Reserved {
		if b != 0 {
			t.Fatalf("zero footer: reserved[%d] = %d, want 0", i, b)
		}
	}

	// Edge case: max values
	maxFooter := footer{
		PayloadRegionHash:  ^uint64(0),
		MetadataRegionHash: ^uint64(0),
	}
	for j := range maxFooter.Reserved {
		maxFooter.Reserved[j] = 0xFF
	}
	maxFooter.encodeTo(buf)
	decoded, err = decodeFooter(buf)
	if err != nil {
		t.Fatalf("max footer: decodeFooter failed: %v", err)
	}
	if decoded.PayloadRegionHash != ^uint64(0) {
		t.Fatalf("max footer: PayloadRegionHash: got 0x%X, want 0x%X",
			decoded.PayloadRegionHash, ^uint64(0))
	}
	if decoded.MetadataRegionHash != ^uint64(0) {
		t.Fatalf("max footer: MetadataRegionHash: got 0x%X, want 0x%X",
			decoded.MetadataRegionHash, ^uint64(0))
	}
	if decoded.Reserved != maxFooter.Reserved {
		t.Fatalf("max footer: Reserved: got %v, want %v",
			decoded.Reserved, maxFooter.Reserved)
	}
}
