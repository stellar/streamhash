package streamhash

import (
	"context"
	"math/bits"
	randv2 "math/rand/v2"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	intbits "github.com/stellar/streamhash/internal/bits"
)

type testEntry struct {
	k0, k1  uint64
	payload uint64
	blockID uint32
}

func generateTestEntry(rng *randv2.Rand, payload uint64, numBlocks uint32) testEntry {
	k0 := rng.Uint64()
	k1 := rng.Uint64()
	prefix := bits.ReverseBytes64(k0)
	blockID := intbits.FastRange32(prefix, numBlocks)
	return testEntry{k0: k0, k1: k1, payload: payload, blockID: blockID}
}

func TestUnsortedBuffer_AddKeyAndFlush(t *testing.T) {
	numBlocks, _ := numBlocksForAlgo(AlgoBijection, 10000, 4, 0)
	cfg := &buildConfig{totalKeys: 10000, payloadSize: 4, unsortedTempDir: t.TempDir()}
	u, err := newUnsortedBuffer(cfg, numBlocks, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer u.cleanup()

	ws, err := u.newWriterState()
	if err != nil {
		t.Fatal(err)
	}
	rng := newTestRNG(t)
	for range 5000 {
		e := generateTestEntry(rng, uint64(rng.Uint32()), numBlocks)
		if err := ws.addKey(e.k0, e.k1, e.payload, e.blockID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestUnsortedBuffer_ConcurrentAddKey(t *testing.T) {
	numKeys := uint64(100000)
	numBlocks, _ := numBlocksForAlgo(AlgoBijection, numKeys, 4, 0)
	numWriters := 8
	cfg := &buildConfig{totalKeys: numKeys, payloadSize: 4, unsortedTempDir: t.TempDir()}
	u, err := newUnsortedBuffer(cfg, numBlocks, numWriters)
	if err != nil {
		t.Fatal(err)
	}
	defer u.cleanup()

	entriesPerWriter := 5000
	var wg sync.WaitGroup
	var errCount atomic.Int32

	for w := range numWriters {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			ws, err := u.newWriterState()
			if err != nil {
				errCount.Add(1)
				return
			}
			rng := randv2.New(randv2.NewPCG(seed, seed+1))
			for range entriesPerWriter {
				e := generateTestEntry(rng, uint64(rng.Uint32()), numBlocks)
				if err := ws.addKey(e.k0, e.k1, e.payload, e.blockID); err != nil {
					errCount.Add(1)
					return
				}
			}
			ws.flushAll()
			ws.free()
		}(uint64(w) * 1000)
	}
	wg.Wait()
	if n := errCount.Load(); n > 0 {
		t.Fatalf("%d addKey errors", n)
	}
}

func TestUnsortedBuffer_FastPath(t *testing.T) {
	numKeys := uint64(50)
	numBlocks, _ := numBlocksForAlgo(AlgoBijection, numKeys, 4, 0)
	cfg := &buildConfig{totalKeys: numKeys, payloadSize: 4, unsortedTempDir: t.TempDir()}
	u, err := newUnsortedBuffer(cfg, numBlocks, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer u.cleanup()

	ws, err := u.newWriterState()
	if err != nil {
		t.Fatal(err)
	}
	rng := newTestRNG(t)
	for i := range int(numKeys) {
		e := generateTestEntry(rng, uint64(i), numBlocks)
		if err := ws.addKey(e.k0, e.k1, e.payload, e.blockID); err != nil {
			t.Fatal(err)
		}
	}
	if u.flushed.Load() {
		t.Error("expected fast path (no flush)")
	}
}

func TestUnsortedBuffer_CleanupIdempotent(t *testing.T) {
	cfg := &buildConfig{totalKeys: 100, payloadSize: 4, unsortedTempDir: t.TempDir()}
	numBlocks, _ := numBlocksForAlgo(AlgoBijection, 100, 4, 0)
	u, err := newUnsortedBuffer(cfg, numBlocks, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := u.cleanup(); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}
	if err := u.cleanup(); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}
}

func TestUnsortedBuffer_TempDirRemovedAfterCreate(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &buildConfig{totalKeys: 100, payloadSize: 4, unsortedTempDir: tmpDir}
	numBlocks, _ := numBlocksForAlgo(AlgoBijection, 100, 4, 0)
	u, err := newUnsortedBuffer(cfg, numBlocks, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer u.cleanup()
	// The temp directory is removed after files are unlinked, so no
	// subdirectories should remain in tmpDir.
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("expected empty tmpDir, got %d entries", len(entries))
	}
}

func TestConcurrentWriterEquivalence(t *testing.T) {
	numKeys := 5000
	tmpDir := t.TempDir()
	ctx := context.Background()
	rng := newTestRNG(t)
	keys := generateRandomKeys(rng, numKeys, 16)

	keyToPayload := make(map[string]uint64)
	for i, key := range keys {
		keyToPayload[string(key)] = uint64(i) & 0xFFFFFFFF
	}

	// Single-threaded unsorted.
	singlePath := tmpDir + "/single.idx"
	bs, err := NewUnsortedBuilder(ctx, singlePath, uint64(numKeys),
		tmpDir, WithPayload(4), WithWorkers(4))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range keys {
		if err := bs.AddKey(key, keyToPayload[string(key)]); err != nil {
			bs.Close()
			t.Fatal(err)
		}
	}
	if err := bs.Finish(); err != nil {
		t.Fatal(err)
	}

	// Concurrent writers via AddKeys.
	cwPath := tmpDir + "/cw.idx"
	numWriters := 4
	keysPerWriter := numKeys / numWriters
	bcw, err := NewUnsortedBuilder(ctx, cwPath, uint64(numKeys),
		tmpDir, WithPayload(4), WithWorkers(4))
	if err != nil {
		t.Fatal(err)
	}
	err = bcw.AddKeys(numWriters, func(writerID int, addKey func([]byte, uint64) error) error {
		start := writerID * keysPerWriter
		end := start + keysPerWriter
		if writerID == numWriters-1 {
			end = numKeys
		}
		for i := start; i < end; i++ {
			if err := addKey(keys[i], keyToPayload[string(keys[i])]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	idx1, _ := Open(singlePath)
	idx2, _ := Open(cwPath)
	if idx1.Stats().NumKeys != idx2.Stats().NumKeys {
		t.Errorf("NumKeys mismatch: single=%d, cw=%d", idx1.Stats().NumKeys, idx2.Stats().NumKeys)
	}
}
