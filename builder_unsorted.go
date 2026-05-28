package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/unix"

	intbits "github.com/tamirms/streamhash/internal/bits"
	"github.com/tamirms/streamhash/internal/sherr"
)

// Unsorted Build Architecture
//
// The unsorted builder constructs an index from keys arriving in arbitrary order.
// It operates in two phases: a write phase that partitions keys to temp files,
// and a read phase that reads them back in partition order to build blocks.
//
// WRITE PHASE (AddKey / AddKeys)
//
// Each key's prefix (first 8 bytes, big-endian) determines its block ID,
// which maps to a partition (partition = blockID / blocksPerPart). The key
// is appended to the writer's in-memory buffer — a single flat array holding
// entries for all partitions, indexed by partition offset. Each writer has
// two such arrays and alternates between them: one accumulates entries while
// the other is flushed to disk in the background via pwrite. Each partition
// has a pre-allocated region in the writer's temp file at offset P * regionSize.
//
// With concurrent writers (AddKeys), each writer has its own temp file and
// its own buffers. Writers operate independently — zero synchronization
// during ingestion. Block counts are accumulated per-writer during flush for
// use in the read phase.
//
// READ PHASE (Finish)
//
// Three code paths depending on dataset size and worker count:
//
//  1. Fast path (finishUnsortedFastPath): If no data was flushed to disk,
//     all entries are still in memory. Counting-sort by blockID and build
//     blocks directly. No file I/O.
//
//  2. Single-threaded (finishUnsortedSingleThreaded, workers=1): Reads
//     partitions sequentially with double-buffered read-ahead (read partition
//     P+1 in background while building blocks from partition P). Builds
//     blocks inline without worker goroutines.
//
//  3. Parallel (finishUnsortedParallel, workers>1): Multiple reader
//     goroutines read partitions from temp files into pre-allocated read
//     buffers. The main thread receives results and dispatches blocks to
//     parallel workers (workChan → workers → resultChan → writer). Each
//     reader owns a pair of dedicated read buffers and alternates between
//     them, so disk reads can overlap with workers processing the previous
//     partition's entries.
//
// PARALLEL READ PIPELINE (finishUnsortedParallel)
//
//     Reader goroutines          Main thread              Workers         Writer
//     ─────────────────          ───────────              ───────         ──────
//     wait for buffer ready      receive result           take work       take result
//     read partition into buf    dispatch blocks           copy entries    write in order
//     send result                mark buffer pending       signal done
//                                signal buffer ready       build block
//                                  (after workers done)    send result
//
// Each reader handles every Nth partition (reader r gets r, r+N, r+2N, ...).
// The main thread consumes results in round-robin order (p % numReaders),
// which matches the partition assignment, so results arrive in partition order.
//
// Buffer reuse protocol:
//
// Entries are dispatched to workers directly from the read buffer (no copy).
// A WaitGroup ("fence") tracks how many workers are still reading from a
// buffer. When a partition's blocks are dispatched, the main thread increments
// the fence for each block. Workers decrement it after copying entries into
// their block builder. A background goroutine waits for the fence to reach
// zero, then signals the reader that the buffer is safe to reuse.
//
// This is deadlock-free because the dependency chain is acyclic: the reader
// waits for a buffer to be ready, which requires workers to finish with
// a previous partition's entries, which requires work that the main thread
// already dispatched before reaching the current partition.

const (
	// overflowSigmas controls the safety margin for per-partition region sizing.
	overflowSigmas = 8

	// writerSkewMargin accounts for non-uniform key distribution across writers.
	writerSkewMargin = 1.2

	// maxFlushBufferBytes is the target memory cap for each writer's flat buffer.
	// Each bufferedEntry is 24 bytes (3×uint64), so 12 MB ≈ 524K entries.
	maxFlushBufferBytes = 12 << 20
)

// bufferedEntry is the in-memory representation during the write phase.
type bufferedEntry struct {
	k0, k1  uint64
	payload uint64
}

// unsortedBuffer holds the shared partition layout and per-writer files for unsorted mode.
// Each writer has its own file for lock-free pwrite. Partitions are logical regions
// within each writer file.
type unsortedBuffer struct {
	cfg       *buildConfig
	numBlocks uint32
	entrySize int // on-disk bytes per entry: MinKeySize + payloadSize

	// Partition layout
	numPartitions    int
	blocksPerPart    int
	blocksPerPartU32 uint32

	// Per-writer file state. Each writer has a single file containing all
	// partition regions. Partition P is at offset P * regionSize.
	numWriters    int        // total writer count (from config)
	regionSize    int64      // bytes per partition region per writer file
	writerFiles   []*os.File // [numWriters]
	writerFds     []int      // [numWriters] fd cache for pwrite/pread
	writerCursors [][]int64  // [numWriters][numPartitions] byte offset within region
	nextWriterID  int        // next writerID to assign
	writerMu      sync.Mutex // protects nextWriterID

	// Read-phase pipeline slots (allocated in prepareForRead)
	readSlots []readSlot

	// Merged block counts from all writers, computed in finishUnsorted.
	mergedBlockCounts [][]int // [numPartitions][blocksPerPart]

	flushed atomic.Bool
}

// writerState holds per-writer flat double-buffered entries. Each writer owns
// its buffers exclusively — zero synchronization during addKey. One buffer
// accumulates entries while the other is flushed to disk in the background.
type writerState struct {
	u             *unsortedBuffer
	writerID      int                // index into u.writerFiles/writerCursors
	flatBufs      [2][]bufferedEntry // double buffer: [activeBuf][partition*regionCap+cursor]
	cursorSets    [2][]int           // [activeBuf][numPartitions]
	activeBuf     int                // index into flatBufs/cursorSets (0 or 1)
	totalBuffered int                // total entries in the active buffer
	bufferCap     int                // flush threshold (total entries)
	regionCap     int                // entries per partition in each flat buffer
	encodeBuf     []byte             // reused encode buffer
	flushWg       sync.WaitGroup     // waits for background flush goroutine
	flushErr      error              // error from background flush
	blockCounts   [][]uint16         // [numPartitions][blocksPerPart] accumulated during flush
}

// newUnsortedBuffer creates an unsortedBuffer with per-writer files.
func newUnsortedBuffer(cfg *buildConfig, numBlocks uint32, numWriters int) (*unsortedBuffer, error) {
	entrySize := MinKeySize + cfg.payloadSize

	numWriters = max(numWriters, 1)

	// Auto-compute partition count to minimize build time.
	// Fewer partitions = larger flush batches = fewer pwrite/pread calls = faster.
	// Optimal P is the smallest that keeps read-phase memory bounded.
	//
	// Two constraints determine P:
	//   1. Flush efficiency floor: P ≤ maxFlushFloorPartitions (2,048).
	//   2. Read memory cap: P ≥ numReadSlots×N×24/maxReadMem keeps read-phase flatBuf
	//      memory bounded at 2 GB — reasonable for machines at scale.
	//
	// P = max(flushFloor, readBound). Below ~46B keys the flush floor binds
	// (P=2,048, constant). Above, the read cap binds and P grows linearly.
	const (
		maxReadMem              = 2 << 30 // 2 GB cap for read-phase flatBuf memory
		maxFlushFloorPartitions = 2048
	)
	numReadSlots := readPipelineDepth * slotsPerReader
	readBound := int(math.Ceil(
		float64(numReadSlots) * float64(cfg.totalKeys) * float64(readEntrySize) / float64(maxReadMem)))
	numPartitions := max(max(maxFlushFloorPartitions, readBound), 1)
	if numPartitions > int(numBlocks) {
		numPartitions = int(numBlocks)
	}

	blocksPerPart := max(int(math.Ceil(float64(numBlocks)/float64(numPartitions))), 1)

	// Region sizing: 1.2× mean + 8σ per partition per writer.
	meanEntries := float64(cfg.totalKeys) * float64(blocksPerPart) / float64(numBlocks) / float64(numWriters)
	sigma := math.Sqrt(meanEntries)
	maxRegionEntries := int64(math.Ceil(meanEntries*writerSkewMargin + overflowSigmas*sigma))
	if maxRegionEntries < 1 {
		maxRegionEntries = 1
	}
	regionSize := maxRegionEntries * int64(entrySize)

	u := &unsortedBuffer{
		cfg:              cfg,
		numBlocks:        numBlocks,
		entrySize:        entrySize,
		numPartitions:    numPartitions,
		blocksPerPart:    blocksPerPart,
		blocksPerPartU32: uint32(blocksPerPart),
		numWriters:       numWriters,
		regionSize:       regionSize,
		writerFiles:      make([]*os.File, numWriters),
		writerFds:        make([]int, numWriters),
		writerCursors:    make([][]int64, numWriters),
	}
	for w := range numWriters {
		u.writerCursors[w] = make([]int64, numPartitions)
	}

	if err := u.createWriterFiles(cfg.unsortedTempDir); err != nil {
		return nil, fmt.Errorf("create writer files: %w", err)
	}

	return u, nil
}

// createWriterFiles creates the temp directory and per-writer files.
// Each writer gets a single file pre-allocated via fallocate.
func (u *unsortedBuffer) createWriterFiles(tempDir string) error {
	if tempDir == "" {
		tempDir = os.TempDir()
	}
	dir, err := os.MkdirTemp(tempDir, "streamhash-parts-*")
	if err != nil {
		return err
	}

	fileSize := u.regionSize * int64(u.numPartitions)
	for w := range u.numWriters {
		path := fmt.Sprintf("%s/writer-%03d", dir, w)
		f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			u.closeWriterFiles()
			os.RemoveAll(dir)
			return fmt.Errorf("create writer %d: %w", w, err)
		}
		if err := preallocFile(int(f.Fd()), fileSize); err != nil {
			f.Close()
			u.closeWriterFiles()
			os.RemoveAll(dir)
			return fmt.Errorf("fallocate writer %d (%d bytes): %w", w, fileSize, err)
		}
		if err := os.Remove(path); err != nil {
			f.Close()
			u.closeWriterFiles()
			os.RemoveAll(dir)
			return fmt.Errorf("unlink writer %d: %w", w, err)
		}
		u.writerFiles[w] = f
		u.writerFds[w] = int(f.Fd())
	}

	// All files are unlinked — remove the now-empty temp directory.
	os.Remove(dir)
	return nil
}

// closeWriterFiles closes all open writer fds.
func (u *unsortedBuffer) closeWriterFiles() {
	for _, f := range u.writerFiles {
		if f != nil {
			f.Close()
		}
	}
}

// newWriterState creates per-writer flat double-buffered regions and assigns a writer file.
func (u *unsortedBuffer) newWriterState() (*writerState, error) {
	u.writerMu.Lock()
	if u.nextWriterID >= u.numWriters {
		u.writerMu.Unlock()
		return nil, fmt.Errorf("all %d writer slots are assigned", u.numWriters)
	}
	id := u.nextWriterID
	u.nextWriterID++
	u.writerMu.Unlock()

	bufferCap := maxFlushBufferBytes / int(unsafe.Sizeof(bufferedEntry{}))
	regionCap := max(bufferCap/u.numPartitions+bufferCap/(u.numPartitions*8), 1)
	flatSize := regionCap * u.numPartitions

	blockCounts := make([][]uint16, u.numPartitions)
	for i := range blockCounts {
		blockCounts[i] = make([]uint16, u.blocksPerPart)
	}
	return &writerState{
		u:           u,
		writerID:    id,
		flatBufs:    [2][]bufferedEntry{make([]bufferedEntry, flatSize), make([]bufferedEntry, flatSize)},
		cursorSets:  [2][]int{make([]int, u.numPartitions), make([]int, u.numPartitions)},
		bufferCap:   bufferCap,
		regionCap:   regionCap,
		blockCounts: blockCounts,
	}, nil
}

// addKey routes an entry to this writer's flat buffer.
// Zero synchronization — each writer owns its buffers exclusively.
func (ws *writerState) addKey(k0, k1 uint64, payload uint64, blockID uint32) error {
	p := int(blockID / ws.u.blocksPerPartU32)
	a := ws.activeBuf
	c := ws.cursorSets[a][p]
	if c >= ws.regionCap {
		if err := ws.flush(); err != nil {
			return err
		}
		a = ws.activeBuf
		c = ws.cursorSets[a][p]
	}
	ws.flatBufs[a][p*ws.regionCap+c] = bufferedEntry{k0: k0, k1: k1, payload: payload}
	ws.cursorSets[a][p] = c + 1
	ws.totalBuffered++
	if ws.totalBuffered >= ws.bufferCap {
		return ws.flush()
	}
	return nil
}

// flush swaps the active buffer and launches a background goroutine to encode
// and pwrite the filled buffer to disk. Waits for any prior background flush first.
func (ws *writerState) flush() error {
	ws.flushWg.Wait()
	if ws.flushErr != nil {
		return ws.flushErr
	}

	if ws.totalBuffered == 0 {
		return nil
	}

	ws.u.flushed.Store(true)

	// Swap buffers: the current active becomes the flush target.
	flushBuf := ws.activeBuf
	ws.activeBuf ^= 1
	ws.totalBuffered = 0
	// Clear cursors for the new active buffer.
	clear(ws.cursorSets[ws.activeBuf])

	ws.flushWg.Add(1)
	go func() {
		defer ws.flushWg.Done()
		ws.flushErr = ws.doFlush(flushBuf)
	}()
	return nil
}

// doFlush encodes and pwrites all partitions from flatBufs[bufIdx] to the
// writer's file. Also counts entries per block for read-phase scatter.
func (ws *writerState) doFlush(bufIdx int) error {
	u := ws.u
	entrySize := u.entrySize
	payloadSize := u.cfg.payloadSize
	regionCap := ws.regionCap
	cursors := ws.cursorSets[bufIdx]
	flat := ws.flatBufs[bufIdx]
	numBlocks := u.numBlocks
	blocksPerPartU32 := u.blocksPerPartU32

	for p := range u.numPartitions {
		n := cursors[p]
		if n == 0 {
			continue
		}

		needed := n * entrySize
		if len(ws.encodeBuf) < needed {
			ws.encodeBuf = make([]byte, needed)
		}
		buf := ws.encodeBuf

		// Encode entries and count per-block in a single pass.
		base := p * regionCap
		entries := flat[base : base+n]
		bc := ws.blockCounts[p]
		for i := range entries {
			e := &entries[i]
			pos := i * entrySize
			binary.LittleEndian.PutUint64(buf[pos:], e.k0)
			binary.LittleEndian.PutUint64(buf[pos+8:], e.k1)
			if payloadSize > 0 {
				packPayloadToBytes(buf[pos+MinKeySize:], e.payload, payloadSize)
			}
			prefix := bits.ReverseBytes64(e.k0)
			localBlockIdx := intbits.FastRange32(prefix, numBlocks) - uint32(p)*blocksPerPartU32
			bc[localBlockIdx]++
		}

		cursor := u.writerCursors[ws.writerID][p]
		regionStart := int64(p) * u.regionSize
		off := regionStart + cursor
		if off+int64(needed) > regionStart+u.regionSize {
			return fmt.Errorf("partition %d overflow in writer %d: region capacity exceeded", p, ws.writerID)
		}

		_, err := unix.Pwrite(u.writerFds[ws.writerID], buf[:needed], off)
		if err != nil {
			return fmt.Errorf("pwrite partition %d writer %d: %w", p, ws.writerID, err)
		}

		u.writerCursors[ws.writerID][p] = cursor + int64(needed)
	}
	return nil
}

// flushAll waits for any background flush, then synchronously flushes the
// remaining active buffer.
func (ws *writerState) flushAll() error {
	// Wait for any in-progress background flush.
	ws.flushWg.Wait()
	if ws.flushErr != nil {
		return ws.flushErr
	}

	if ws.totalBuffered == 0 {
		return nil
	}

	ws.u.flushed.Store(true)

	// Synchronously flush the active buffer (no swap needed).
	return ws.doFlush(ws.activeBuf)
}

// free releases the writer's flat buffer memory.
// blockCounts are preserved for merging in finishUnsorted.
func (ws *writerState) free() {
	ws.flatBufs = [2][]bufferedEntry{}
	ws.cursorSets = [2][]int{}
	ws.encodeBuf = nil
}

// freeBlockCounts releases the block count memory after merging.
func (ws *writerState) freeBlockCounts() {
	ws.blockCounts = nil
}

// unsortedWriter is a concurrent writer for unsorted mode.
// Each writer has private per-partition regions — zero synchronization during addKey.
type unsortedWriter struct {
	b          *builder
	ws         *writerState
	keysAdded  uint64
	keyCounter int
	closed     bool
}

// addKey adds a key-payload pair via this writer.
func (w *unsortedWriter) addKey(key []byte, payload uint64) error {
	if err := w.b.validateKey(key, payload); err != nil {
		return err
	}

	w.keyCounter++
	if w.keyCounter >= contextCheckInterval {
		w.keyCounter = 0
		select {
		case <-w.b.ctx.Done():
			return w.b.ctx.Err()
		default:
		}
	}

	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	prefix := bits.ReverseBytes64(k0)
	blockIdx := intbits.FastRange32(prefix, w.b.numBlocks)

	if err := w.ws.addKey(k0, k1, payload, blockIdx); err != nil {
		return err
	}
	w.keysAdded++
	return nil
}

// close flushes remaining entries and releases writer memory.
func (w *unsortedWriter) close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	if err := w.ws.flushAll(); err != nil {
		return err
	}
	w.ws.free()
	return nil
}

// UnsortedBuilder builds an index from keys in arbitrary order.
// Keys are buffered to per-writer temp files, partitioned, and built during Finish.
//
// Two ingestion modes (mutually exclusive):
//   - AddKey: single-threaded sequential addition, then call Finish
//   - AddKeys: concurrent multi-writer callback, calls Finish internally
//
// Usage (single-threaded):
//
//	builder, _ := streamhash.NewUnsortedBuilder(ctx, path, totalKeys, tempDir, opts...)
//	defer builder.Close()
//	for key, payload := range data {
//	    builder.AddKey(key, payload)
//	}
//	return builder.Finish()
//
// Usage (concurrent):
//
//	builder, _ := streamhash.NewUnsortedBuilder(ctx, path, totalKeys, tempDir, opts...)
//	defer builder.Close()
//	return builder.AddKeys(8, func(writerID int, addKey func([]byte, uint64) error) error {
//	    for key, payload := range myPartition(writerID) {
//	        if err := addKey(key, payload); err != nil { return err }
//	    }
//	    return nil
//	})
type UnsortedBuilder struct {
	b           *builder
	addKeyUsed  bool
	addKeysUsed bool
	initialized bool // true after unsortedBuffer is created

	// Unsorted-specific state (owned by UnsortedBuilder, not shared builder core)
	unsortedBuf     *unsortedBuffer
	defaultWriterWS *writerState // default writer for single-threaded AddKey

	// Concurrent writers
	writersMu sync.Mutex
	writers   []*unsortedWriter
}

// NewUnsortedBuilder creates a builder for unsorted input.
// Keys can be added in any order via AddKey or AddKeys.
//
// tempDir specifies where partition files are created. Pass "" to use the
// system default (os.TempDir). The directory must exist and be on a local
// filesystem (ext4, xfs, btrfs). NFS is not supported. tmpfs works but is
// not recommended at scale since it stores data in RAM/swap.
//
// Use WithWorkers(N) to parallelize block building during Finish.
func NewUnsortedBuilder(ctx context.Context, output string, totalKeys uint64, tempDir string, opts ...BuildOption) (*UnsortedBuilder, error) {
	b, err := newBuilder(ctx, output, totalKeys, opts...)
	if err != nil {
		return nil, err
	}
	b.cfg.unsortedTempDir = tempDir
	// Validate temp dir eagerly so invalid configs fail at constructor time.
	dir := tempDir
	if dir == "" {
		dir = os.TempDir()
	}
	if info, err := os.Stat(dir); err != nil {
		return nil, errors.Join(fmt.Errorf("temp dir: %w", err), b.iw.close(), os.Remove(output))
	} else if !info.IsDir() {
		return nil, errors.Join(fmt.Errorf("temp dir %q is not a directory", dir), b.iw.close(), os.Remove(output))
	}
	return &UnsortedBuilder{b: b}, nil
}

// initBuffer lazily creates the unsorted buffer with the given writer count.
func (ub *UnsortedBuilder) initBuffer(numWriters int) error {
	if ub.initialized {
		return nil
	}

	buf, err := newUnsortedBuffer(ub.b.cfg, ub.b.numBlocks, numWriters)
	if err != nil {
		return fmt.Errorf("init unsorted mode: %w", err)
	}
	ub.unsortedBuf = buf

	if numWriters <= 1 {
		ws, err := buf.newWriterState()
		if err != nil {
			return errors.Join(fmt.Errorf("init default writer: %w", err), buf.cleanup())
		}
		ub.defaultWriterWS = ws
	}
	ub.initialized = true
	return nil
}

// AddKey adds a key-payload pair. Keys can be in any order.
// Cannot be used after AddKeys has been called.
func (ub *UnsortedBuilder) AddKey(key []byte, payload uint64) error {
	b := ub.b

	if !ub.initialized {
		return ub.addKeyFirstCall(key, payload)
	}

	if b.closed {
		return sherr.ErrBuilderClosed
	}

	// Inline validation for hot-path performance.
	if len(key) > maxKeyLength {
		return sherr.ErrKeyTooLong
	}
	if len(key) < MinKeySize {
		return sherr.ErrKeyTooShort
	}
	if ps := b.cfg.payloadSize; ps > 0 && ps < 8 {
		if payload > uint64(1)<<(ps*8)-1 {
			return sherr.ErrPayloadOverflow
		}
	}

	b.keyCounter++
	if b.keyCounter >= contextCheckInterval {
		b.keyCounter = 0
		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		default:
		}
	}

	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	prefix := bits.ReverseBytes64(k0)
	blockIdx := intbits.FastRange32(prefix, b.numBlocks)

	// Inline writerState.addKey for hot-path performance.
	ws := ub.defaultWriterWS
	p := int(blockIdx / ws.u.blocksPerPartU32)
	a := ws.activeBuf
	c := ws.cursorSets[a][p]
	if c >= ws.regionCap {
		if err := ws.flush(); err != nil {
			return err
		}
		a = ws.activeBuf
		c = ws.cursorSets[a][p]
	}
	ws.flatBufs[a][p*ws.regionCap+c] = bufferedEntry{k0: k0, k1: k1, payload: payload}
	ws.cursorSets[a][p] = c + 1
	ws.totalBuffered++
	if ws.totalBuffered >= ws.bufferCap {
		if err := ws.flush(); err != nil {
			return err
		}
	}
	b.totalKeysAdded++
	return nil
}

// addKeyFirstCall handles the first AddKey call — validates mode, initializes buffer,
// then delegates to the fast path. Kept out of AddKey to keep the hot path function small.
func (ub *UnsortedBuilder) addKeyFirstCall(key []byte, payload uint64) error {
	b := ub.b
	if b.closed {
		return sherr.ErrBuilderClosed
	}
	if ub.addKeysUsed {
		return fmt.Errorf("AddKey and AddKeys are mutually exclusive; AddKeys was already called")
	}
	ub.addKeyUsed = true

	if err := ub.initBuffer(1); err != nil {
		return errors.Join(err, b.cleanup())
	}

	return ub.AddKey(key, payload)
}

// AddKeys ingests keys in parallel using numWriters concurrent writers.
// The callback fn is invoked once per writer in its own goroutine.
// Each invocation receives a writerID (0-based) and an addKey function.
// AddKeys calls Finish internally — do not call Finish separately.
//
// Cannot be used after AddKey has been called.
//
// Example:
//
//	err := builder.AddKeys(8, func(writerID int, addKey func([]byte, uint64) error) error {
//	    for key, payload := range myPartition(writerID) {
//	        if err := addKey(key, payload); err != nil { return err }
//	    }
//	    return nil
//	})
func (ub *UnsortedBuilder) AddKeys(numWriters int, fn func(writerID int, addKey func(key []byte, payload uint64) error) error) error {
	if ub.b.closed {
		return sherr.ErrBuilderClosed
	}
	if ub.addKeyUsed {
		return fmt.Errorf("AddKey and AddKeys are mutually exclusive; AddKey was already called")
	}
	ub.addKeysUsed = true

	if numWriters < 1 {
		return fmt.Errorf("AddKeys: numWriters must be >= 1, got %d", numWriters)
	}

	if err := ub.initBuffer(numWriters); err != nil {
		ub.b.cleanup()
		return err
	}

	// Create all writers before launching goroutines.
	writers := make([]*unsortedWriter, numWriters)
	for i := range numWriters {
		ws, err := ub.unsortedBuf.newWriterState()
		if err != nil {
			ub.Close()
			return fmt.Errorf("new writer: %w", err)
		}
		w := &unsortedWriter{b: ub.b, ws: ws}
		ub.writersMu.Lock()
		ub.writers = append(ub.writers, w)
		ub.writersMu.Unlock()
		writers[i] = w
	}

	// Run writers concurrently.
	var wg sync.WaitGroup
	errs := make([]error, numWriters)
	for i := range numWriters {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			w := writers[writerID]
			errs[writerID] = fn(writerID, w.addKey)
			if closeErr := w.close(); closeErr != nil && errs[writerID] == nil {
				errs[writerID] = closeErr
			}
		}(i)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		ub.Close()
		return err
	}

	return ub.Finish()
}

// Finish completes the index. Called automatically by AddKeys.
// Only call this directly when using AddKey (not AddKeys).
func (ub *UnsortedBuilder) Finish() error {
	b := ub.b
	if b.closed {
		return sherr.ErrBuilderClosed
	}
	b.closed = true

	// Sum key counts from all writers.
	totalKeys := b.totalKeysAdded
	for _, w := range ub.writers {
		totalKeys += w.keysAdded
	}

	if totalKeys != b.cfg.totalKeys {
		primaryErr := fmt.Errorf("%w: expected %d, got %d", sherr.ErrKeyCountMismatch, b.cfg.totalKeys, totalKeys)
		return errors.Join(primaryErr, ub.cleanupAll())
	}

	return ub.finishUnsorted()
}

// Close aborts the build and cleans up resources. Safe to call after Finish.
func (ub *UnsortedBuilder) Close() error {
	b := ub.b
	if b.closed {
		b.shutdownWorkers()
		return nil
	}
	b.closed = true
	return ub.cleanupAll()
}

// cleanupAll cleans up both unsorted buffer and core builder resources.
func (ub *UnsortedBuilder) cleanupAll() error {
	var errs []error
	if ub.unsortedBuf != nil {
		if err := ub.unsortedBuf.cleanup(); err != nil {
			errs = append(errs, err)
		}
		ub.unsortedBuf = nil
	}
	if err := ub.b.cleanup(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
