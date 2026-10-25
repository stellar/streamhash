package streamhash

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

// TestRAMIndexAcrossBufferFlushes checks that the writer writes the RAM index
// as blocks commit, buffering at most ramIndexBufEntries entries of it, for
// block counts around the buffer's flushes. Builds that large need 20M+ keys,
// so it drives the writer with synthetic blocks.
func TestRAMIndexAcrossBufferFlushes(t *testing.T) {
	for _, numBlocks := range []uint32{ramIndexBufEntries - 1, ramIndexBufEntries, ramIndexBufEntries + 1, 2 * ramIndexBufEntries} {
		t.Run(fmt.Sprintf("blocks=%d", numBlocks), func(t *testing.T) {
			cfg := defaultBuildConfig()
			algo, err := newBlockBuilder(AlgoBijection, 0, cfg.globalSeed, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "index.idx")
			iw, err := newIndexWriter(path, cfg, numBlocks, algo)
			if err != nil {
				t.Fatal(err)
			}

			want := make([]byte, (numBlocks+1)*ramIndexEntrySize)
			var entry ramIndexEntry
			for b := range numBlocks {
				encodeRAMIndexEntryTo(entry, want[b*ramIndexEntrySize:])
				numKeys := int(b % 5)
				metadata := bytes.Repeat([]byte{byte(b)}, int(b%3))
				if err := iw.commitBlockWithData(metadata, nil, numKeys); err != nil {
					t.Fatal(err)
				}
				entry.KeysBefore += uint64(numKeys)
				entry.MetadataOffset += uint64(len(metadata))
			}
			encodeRAMIndexEntryTo(entry, want[numBlocks*ramIndexEntrySize:])
			if cap(iw.ramIndexBuf) > ramIndexBufEntries*ramIndexEntrySize {
				t.Fatalf("the writer buffered %d bytes of RAM index", cap(iw.ramIndexBuf))
			}
			if err := iw.finalize(); err != nil {
				t.Fatal(err)
			}

			idx, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer idx.Close()
			if !bytes.Equal(idx.ramIndexBytes, want) {
				t.Fatal("the RAM index differs from the blocks' entries")
			}
			if err := idx.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
