package streamhash

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// TestBytesPerSyncLeavesIndexUnchanged checks that writeback changes nothing
// in the built index, sequential or parallel, even when it starts on nearly
// every write.
func TestBytesPerSyncLeavesIndexUnchanged(t *testing.T) {
	const numKeys = 20000
	keys := generateRandomKeys(newTestRNG(t), numKeys, 16)
	numBlocks, err := numBlocksForAlgo(AlgoBijection, numKeys, 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(keys, func(a, b []byte) int {
		return cmp.Compare(blockIndexFromPrefix(extractPrefix(a), numBlocks), blockIndexFromPrefix(extractPrefix(b), numBlocks))
	})

	build := func(path string, opts ...BuildOption) []byte {
		t.Helper()
		opts = append([]BuildOption{WithPayload(4), WithFingerprint(1)}, opts...)
		b, err := NewSortedBuilder(context.Background(), path, numKeys, opts...)
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range keys {
			if err := b.AddKey(key, uint64(i)); err != nil {
				b.Close()
				t.Fatal(err)
			}
		}
		if err := b.Finish(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	dir := t.TempDir()
	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			plain := build(filepath.Join(dir, fmt.Sprintf("plain-%d.idx", workers)), WithWorkers(workers))
			synced := build(filepath.Join(dir, fmt.Sprintf("synced-%d.idx", workers)), WithWorkers(workers), WithBytesPerSync(1024))
			if !bytes.Equal(plain, synced) {
				t.Fatal("the index built with WithBytesPerSync differs from the one built without it")
			}
		})
	}
}
