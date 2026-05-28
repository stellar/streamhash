package streamhash

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	intbits "github.com/tamirms/streamhash/internal/bits"
	"github.com/tamirms/streamhash/internal/sherr"
)

// entry represents a key-payload pair for test building helpers.
type entry struct {
	Key     []byte
	Payload uint64
}

// extractPrefix extracts the key prefix (first 8 bytes as big-endian uint64).
func extractPrefix(key []byte) uint64 {
	_ = key[7]
	return binary.BigEndian.Uint64(key[0:8])
}

// fillFromRNG fills buf with pseudo-random bytes from rng.
func fillFromRNG(rng *rand.Rand, buf []byte) {
	for i := 0; i+8 <= len(buf); i += 8 {
		binary.LittleEndian.PutUint64(buf[i:], rng.Uint64())
	}
	if tail := len(buf) % 8; tail > 0 {
		v := rng.Uint64()
		start := len(buf) - tail
		for j := range tail {
			buf[start+j] = byte(v >> (j * 8))
		}
	}
}

// generateRandomKeys creates n deterministic pseudo-random keys of the specified size.
func generateRandomKeys(rng *rand.Rand, n, keySize int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = make([]byte, keySize)
		fillFromRNG(rng, keys[i])
	}
	return keys
}

// entriesToSlice converts a key slice to entry slice.
func entriesToSlice(keys [][]byte) []entry {
	entries := make([]entry, len(keys))
	for i, key := range keys {
		entries[i] = entry{Key: key}
	}
	return entries
}

// payloadToUint64 converts a byte slice payload to uint64.
func payloadToUint64(payload []byte) uint64 {
	if len(payload) == 0 {
		return 0
	}
	if len(payload) >= 8 {
		return binary.LittleEndian.Uint64(payload)
	}
	var result uint64
	for i := range payload {
		result |= uint64(payload[i]) << (i * 8)
	}
	return result
}

// numBlocksForAlgo returns the number of blocks for the given algorithm and key count.
func numBlocksForAlgo(algo Algorithm, n uint64, payloadSize, fingerprintSize int) (uint32, error) {
	bldr, err := newBlockBuilder(algo, n, 0, payloadSize, fingerprintSize)
	if err != nil {
		return 0, err
	}
	return bldr.NumBlocks(), nil
}

// blockIndexFromPrefix computes block index directly from prefix using FastRange.
func blockIndexFromPrefix(prefix uint64, numBlocks uint32) uint32 {
	return intbits.FastRange32(prefix, numBlocks)
}

// sortKeysByBlock sorts keys by block index for proper Builder input.
func sortKeysByBlock(keys [][]byte, totalKeys uint64, opts []BuildOption) {
	if len(keys) == 0 {
		return
	}

	cfg := defaultBuildConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	numBlocks, err := numBlocksForAlgo(cfg.algorithm, totalKeys, cfg.payloadSize, cfg.fingerprintSize)
	if err != nil {
		panic(fmt.Sprintf("numBlocksForAlgo failed: %v", err))
	}

	slices.SortFunc(keys, func(a, b []byte) int {
		if c := cmp.Compare(blockIndexFromPrefix(extractPrefix(a), numBlocks),
			blockIndexFromPrefix(extractPrefix(b), numBlocks)); c != 0 {
			return c
		}
		return bytes.Compare(a, b)
	})
}

// sortEntriesByBlock sorts entries by block index for proper Builder input.
func sortEntriesByBlock(entries []entry, opts []BuildOption) {
	if len(entries) == 0 {
		return
	}

	cfg := defaultBuildConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	numBlocks, err := numBlocksForAlgo(cfg.algorithm, uint64(len(entries)), cfg.payloadSize, cfg.fingerprintSize)
	if err != nil {
		panic(fmt.Sprintf("numBlocksForAlgo failed: %v", err))
	}

	slices.SortFunc(entries, func(a, b entry) int {
		if c := cmp.Compare(blockIndexFromPrefix(extractPrefix(a.Key), numBlocks),
			blockIndexFromPrefix(extractPrefix(b.Key), numBlocks)); c != 0 {
			return c
		}
		return bytes.Compare(a.Key, b.Key)
	})
}

// buildFromSlice builds an index from a slice of entries.
// Entries are sorted by block index before building.
func buildFromSlice(ctx context.Context, output string, entries []entry, opts ...BuildOption) error {
	if len(entries) == 0 {
		return sherr.ErrEmptyIndex
	}

	cfg := defaultBuildConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	numBlocks, err := numBlocksForAlgo(cfg.algorithm, uint64(len(entries)), cfg.payloadSize, cfg.fingerprintSize)
	if err != nil {
		return err
	}

	slices.SortFunc(entries, func(a, b entry) int {
		return cmp.Compare(blockIndexFromPrefix(extractPrefix(a.Key), numBlocks),
			blockIndexFromPrefix(extractPrefix(b.Key), numBlocks))
	})

	builder, err := NewSortedBuilder(ctx, output, uint64(len(entries)), opts...)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := builder.AddKey(e.Key, e.Payload); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// buildSorted builds an index from a key iterator with []byte payloads.
func buildSorted(ctx context.Context, output string, totalKeys uint64, keys func(yield func([]byte, []byte) bool), opts ...BuildOption) error {
	if totalKeys == 0 {
		return sherr.ErrEmptyIndex
	}

	builder, err := NewSortedBuilder(ctx, output, totalKeys, opts...)
	if err != nil {
		return err
	}

	for key, payload := range keys {
		if err := builder.AddKey(key, payloadToUint64(payload)); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// buildFromEntries builds an index from entries, sorting them first.
func buildFromEntries(ctx context.Context, output string, entries []entry, opts ...BuildOption) error {
	if len(entries) == 0 {
		return sherr.ErrEmptyIndex
	}

	sorted := make([]entry, len(entries))
	copy(sorted, entries)
	sortEntriesByBlock(sorted, opts)

	builder, err := NewSortedBuilder(ctx, output, uint64(len(sorted)), opts...)
	if err != nil {
		return err
	}

	for _, e := range sorted {
		if err := builder.AddKey(e.Key, e.Payload); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// quickBuild builds from keys (no payloads), sorting by block.
// It copies the input slice to avoid mutating the caller's data.
func quickBuild(ctx context.Context, output string, keys [][]byte, opts ...BuildOption) error {
	if len(keys) == 0 {
		return sherr.ErrEmptyIndex
	}

	sorted := make([][]byte, len(keys))
	copy(sorted, keys)
	keys = sorted

	sortKeysByBlock(keys, uint64(len(keys)), opts)

	builder, err := NewSortedBuilder(ctx, output, uint64(len(keys)), opts...)
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// quickBuildNoPreHash builds an index from keys without pre-hashing.
func quickBuildNoPreHash(ctx context.Context, output string, keys [][]byte) error {
	entries := make([]entry, len(keys))
	for i, k := range keys {
		entries[i] = entry{Key: k}
	}
	return buildFromSlice(ctx, output, entries)
}

// buildUnsortedFromIter collects all keys from iterator, sorts, and builds.
func buildUnsortedFromIter(ctx context.Context, output string, iter func(yield func([]byte, uint64) bool), opts ...BuildOption) error {
	var entries []entry
	iter(func(key []byte, payload uint64) bool {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		entries = append(entries, entry{Key: keyCopy, Payload: payload})
		return true
	})

	if len(entries) == 0 {
		return sherr.ErrEmptyIndex
	}

	sortEntriesByBlock(entries, opts)

	builder, err := NewSortedBuilder(ctx, output, uint64(len(entries)), opts...)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := builder.AddKey(e.Key, e.Payload); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// buildParallelBytes builds with parallel workers from []byte payload iterator.
func buildParallelBytes(ctx context.Context, output string, iter func(yield func([]byte, []byte) bool), opts ...BuildOption) error {
	var entries []entry
	iter(func(key []byte, payload []byte) bool {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		entries = append(entries, entry{Key: keyCopy, Payload: payloadToUint64(payload)})
		return true
	})

	if len(entries) == 0 {
		return sherr.ErrEmptyIndex
	}

	sortEntriesByBlock(entries, opts)

	opts = append(opts, WithWorkers(4))

	builder, err := NewSortedBuilder(ctx, output, uint64(len(entries)), opts...)
	if err != nil {
		return err
	}

	for _, e := range entries {
		if err := builder.AddKey(e.Key, e.Payload); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// createSmallValidIndex creates a small valid index for error tests.
func createSmallValidIndex(path string) error {
	ctx := context.Background()
	numKeys := 100

	keys := make([][]byte, numKeys)
	for i := range numKeys {
		src := make([]byte, 20)
		binary.BigEndian.PutUint64(src[0:8], uint64(i))
		binary.BigEndian.PutUint64(src[8:16], uint64(i*7919))
		for j := 16; j < 20; j++ {
			src[j] = byte(i + j)
		}
		keys[i] = PreHash(src)
	}

	sortKeysByBlock(keys, uint64(numKeys), nil)

	builder, err := NewSortedBuilder(ctx, path, uint64(numKeys))
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := builder.AddKey(key, 0); err != nil {
			builder.Close()
			return err
		}
	}

	return builder.Finish()
}

// buildAndOpen builds a sorted index from the given keys and opens it for querying.
// Keys must already be sorted by block index. The caller must call idx.Close() when done.
func buildAndOpen(t *testing.T, keys [][]byte, payloads []uint64, opts ...BuildOption) *Index {
	t.Helper()
	indexPath := filepath.Join(t.TempDir(), "test.idx")
	ctx := context.Background()
	n := uint64(len(keys))

	builder, err := NewSortedBuilder(ctx, indexPath, n, opts...)
	if err != nil {
		t.Fatalf("NewSortedBuilder: %v", err)
	}

	for i, key := range keys {
		var payload uint64
		if payloads != nil {
			payload = payloads[i]
		}
		if err := builder.AddKey(key, payload); err != nil {
			builder.Close()
			t.Fatalf("AddKey error at %d: %v", i, err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish error: %v", err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return idx
}

// buildAndOpenUnsorted builds an unsorted index from the given keys and opens it for querying.
// Keys can be in any order. The caller must call idx.Close() when done.
func buildAndOpenUnsorted(t *testing.T, keys [][]byte, payloads []uint64, tempDir string, opts ...BuildOption) *Index {
	t.Helper()
	indexPath := filepath.Join(t.TempDir(), "test.idx")
	ctx := context.Background()
	n := uint64(len(keys))

	builder, err := NewUnsortedBuilder(ctx, indexPath, n, tempDir, opts...)
	if err != nil {
		t.Fatalf("NewUnsortedBuilder: %v", err)
	}

	for i, key := range keys {
		var payload uint64
		if payloads != nil {
			payload = payloads[i]
		}
		if err := builder.AddKey(key, payload); err != nil {
			builder.Close()
			t.Fatalf("AddKey error at %d: %v", i, err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatalf("Finish error: %v", err)
	}

	idx, err := Open(indexPath)
	if err != nil {
		t.Fatalf("Open error: %v", err)
	}
	return idx
}

// verifyMPHF checks that all keys map to unique ranks in [0, N).
func verifyMPHF(t *testing.T, idx *Index, keys [][]byte) {
	t.Helper()
	n := uint64(len(keys))
	ranks := make(map[uint64]bool, len(keys))

	for i, key := range keys {
		rank, err := idx.QueryRank(key)
		if err != nil {
			t.Errorf("Query error for key %d: %v", i, err)
			continue
		}
		if rank >= n {
			t.Errorf("key %d: rank %d >= N %d", i, rank, n)
		}
		if ranks[rank] {
			t.Errorf("key %d: duplicate rank %d", i, rank)
		}
		ranks[rank] = true
	}

	if uint64(len(ranks)) != n {
		t.Errorf("expected %d unique ranks, got %d", n, len(ranks))
	}
}

// verifyPayloads checks payload round-trip for all keys.
func verifyPayloads(t *testing.T, idx *Index, keys [][]byte, payloads []uint64, payloadSize int) {
	t.Helper()
	pi, err := idx.WithPayload()
	if err != nil {
		t.Fatalf("WithPayload: %v", err)
	}

	mask := uint64(0)
	for i := 0; i < payloadSize && i < 8; i++ {
		mask |= 0xFF << (i * 8)
	}

	for i, key := range keys {
		_, got, err := pi.QueryPayload(key)
		if err != nil {
			t.Errorf("QueryPayload error for key %d: %v", i, err)
			continue
		}
		want := payloads[i] & mask
		if got != want {
			t.Errorf("key %d: payload got %d, want %d", i, got, want)
		}
	}
}

// verifyNonMemberRejection queries non-member keys and verifies they are rejected.
// With fingerprints, most non-members should get ErrNotFound.
func verifyNonMemberRejection(t *testing.T, rng *rand.Rand, idx *Index, numProbes int) {
	t.Helper()
	rejected := 0
	for i := range numProbes {
		nonMember := make([]byte, 24)
		binary.BigEndian.PutUint64(nonMember[0:8], uint64(0xDEAD000000000000)|uint64(i))
		fillFromRNG(rng, nonMember[8:])

		_, err := idx.QueryRank(nonMember)
		if err != nil {
			rejected++
		}
	}
	// With fp>=1 byte, FPR = 1/256 so >99% should be rejected.
	// Use 90% threshold for safety margin.
	if rejected < numProbes*9/10 {
		t.Errorf("non-member rejection too low: %d/%d (expected >90%%)", rejected, numProbes)
	}
}

// sortKeysAndPayloads sorts keys by bytes.Compare and reorders payloads to match.
func sortKeysAndPayloads(keys [][]byte, payloads []uint64) {
	type kp struct {
		key     []byte
		payload uint64
	}
	pairs := make([]kp, len(keys))
	for i := range keys {
		var p uint64
		if payloads != nil {
			p = payloads[i]
		}
		pairs[i] = kp{keys[i], p}
	}
	slices.SortFunc(pairs, func(a, b kp) int {
		return bytes.Compare(a.key, b.key)
	})
	for i, p := range pairs {
		keys[i] = p.key
		if payloads != nil {
			payloads[i] = p.payload
		}
	}
}
