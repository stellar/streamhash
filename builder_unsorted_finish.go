package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"

	intbits "github.com/tamirms/streamhash/internal/bits"
	"github.com/tamirms/streamhash/internal/sherr"
)

const (
	// readBufSize is the streaming read buffer size for partition files.
	readBufSize = 4 << 20

	// readEntrySize is the per-entry memory during the read phase (routedEntry).
	// Used to compute partition count so read-phase flatBuf memory stays bounded.
	readEntrySize = int(unsafe.Sizeof(routedEntry{}))

	// blockContextCheckInterval is how often to check for context cancellation
	// during block-level iteration in Finish.
	blockContextCheckInterval = 64
)

// readPipelineDepth is the number of reader goroutines for partition reading.
const readPipelineDepth = 3

// slotsPerReader is the number of readSlots each reader alternates between.
// Two slots allow a reader to start the next partition's pread while workers
// finish reading entries from the previous partition's flatBuf.
const slotsPerReader = 2

// readSlot holds all per-partition-read buffers. Each reader goroutine owns
// a pair of dedicated slots and alternates between them across partitions.
type readSlot struct {
	flatBuf      []routedEntry
	blockEntries [][]routedEntry
	blockCursors []int
	readBuf      []byte
}

// readPartition reads partition p's data from all writer files and returns
// entries grouped by block. Single-pass: block counts were pre-computed during
// addKey, so we only need one scatter pass. Uses pread for concurrent reader safety.
func (u *unsortedBuffer) readPartition(p int, slot *readSlot) ([][]routedEntry, error) {
	partStartBlock := p * u.blocksPerPart
	partEndBlock := min(partStartBlock+u.blocksPerPart, int(u.numBlocks))
	localBlocks := partEndBlock - partStartBlock
	if localBlocks <= 0 {
		return nil, nil
	}

	hasData := false
	for w := range u.numWriters {
		if u.writerCursors[w][p] > 0 {
			hasData = true
			break
		}
	}
	if !hasData {
		return make([][]routedEntry, localBlocks), nil
	}

	entrySize := u.entrySize
	payloadSize := u.cfg.payloadSize
	readBuf := slot.readBuf
	regionStart := int64(p) * u.regionSize

	// Use pre-computed block counts from addKey-time counting, then free.
	blockCounts := u.mergedBlockCounts[p][:localBlocks]
	u.mergedBlockCounts[p] = nil

	// Prefix sum → block offsets and result slices.
	flatBuf := slot.flatBuf
	blockCursors := slot.blockCursors[:localBlocks]
	blockEntries := slot.blockEntries[:localBlocks]
	offset := 0
	for b := range localBlocks {
		blockCursors[b] = offset
		blockEntries[b] = flatBuf[offset : offset+blockCounts[b]]
		offset += blockCounts[b]
	}

	// Single pass: scatter entries from all writer files.
	for w := range u.numWriters {
		regionBytes := u.writerCursors[w][p]
		if regionBytes == 0 {
			continue
		}
		if err := u.readRegion(u.writerFds[w], regionStart, regionBytes, readBuf, entrySize,
			func(data []byte) {
				k0 := binary.LittleEndian.Uint64(data[0:8])
				k1 := binary.LittleEndian.Uint64(data[8:16])
				var payload uint64
				if payloadSize > 0 {
					payload = unpackPayloadFromBytes(data[MinKeySize:], payloadSize)
				}
				prefix := bits.ReverseBytes64(k0)
				localIdx := int(intbits.FastRange32(prefix, u.numBlocks)) - partStartBlock
				flatBuf[blockCursors[localIdx]] = routedEntry{k0: k0, k1: k1, payload: payload}
				blockCursors[localIdx]++
			}); err != nil {
			return nil, fmt.Errorf("read partition %d writer %d: %w", p, w, err)
		}
	}

	return blockEntries, nil
}

// readRegion reads regionBytes from fd at offset using pread, calling fn for
// each entry. Uses pread (not seek+read) for concurrent reader safety.
func (u *unsortedBuffer) readRegion(fd int, offset, regionBytes int64, readBuf []byte, entrySize int, fn func([]byte)) error {
	pos := offset
	remaining := regionBytes
	leftoverN := 0

	for remaining > 0 {
		toRead := min(int64(len(readBuf)-leftoverN), remaining)
		nr, err := unix.Pread(fd, readBuf[leftoverN:leftoverN+int(toRead)], pos)
		if nr == 0 {
			if err != nil {
				return err
			}
			return fmt.Errorf("pread: unexpected zero read at offset %d with %d remaining", pos, remaining)
		}
		pos += int64(nr)
		remaining -= int64(nr)

		data := readBuf[:leftoverN+nr]
		leftoverN = 0

		for len(data) >= entrySize {
			fn(data[:entrySize])
			data = data[entrySize:]
		}

		if len(data) > 0 {
			leftoverN = copy(readBuf, data)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// mergeBlockCounts sums block counts from all writers into mergedBlockCounts.
//
// The per-writer counts are uint16. A single block can only exceed 65535 keys
// (and thus wrap) under catastrophic hash skew. Such a block is unbuildable
// anyway (it far exceeds maxKeysPerBlock), but that guard fires later during
// block construction — this check runs first, here in the flush/merge phase,
// and is the only thing standing between a uint16 wrap and silent mis-bucketing
// in the read phase. We verify the merged counts sum to the expected key total
// (already validated against the user-declared count in Finish); a mismatch
// means a count wrapped, so we abort rather than emit a corrupt index. The
// total is accumulated inside the existing merge loop (no extra traversal).
func (u *unsortedBuffer) mergeBlockCounts(allWS []*writerState) error {
	u.mergedBlockCounts = make([][]int, u.numPartitions)
	var total uint64
	for p := range u.numPartitions {
		merged := make([]int, u.blocksPerPart)
		for _, ws := range allWS {
			bc := ws.blockCounts[p]
			for b := range merged {
				v := int(bc[b])
				merged[b] += v
				total += uint64(v)
			}
		}
		u.mergedBlockCounts[p] = merged
	}
	if total != u.cfg.totalKeys {
		return fmt.Errorf("%w: per-block counts sum to %d, expected %d (a block likely exceeded the uint16 count limit)",
			sherr.ErrBlockOverflow, total, u.cfg.totalKeys)
	}
	return nil
}

// maxPartitionEntries returns the maximum entry count across all partitions.
func (u *unsortedBuffer) maxPartitionEntries() int {
	maxEntries := 0
	for p := range u.numPartitions {
		var totalBytes int64
		for w := range u.numWriters {
			totalBytes += u.writerCursors[w][p]
		}
		n := int(totalBytes) / u.entrySize
		if n > maxEntries {
			maxEntries = n
		}
	}
	return maxEntries
}

// prepareForRead pre-allocates read-phase slots.
// All writers must be closed (and their regions flushed) before calling this.
// numSlots controls how many readSlots to allocate.
func (u *unsortedBuffer) prepareForRead(numSlots int) error {
	maxEntries := u.maxPartitionEntries()
	if maxEntries == 0 {
		return nil
	}

	maxBlocksPerPart := u.blocksPerPart

	numSlots = min(numSlots, u.numPartitions)
	u.readSlots = make([]readSlot, numSlots)
	for i := range numSlots {
		u.readSlots[i] = readSlot{
			flatBuf:      make([]routedEntry, maxEntries),
			blockEntries: make([][]routedEntry, maxBlocksPerPart),
			blockCursors: make([]int, maxBlocksPerPart),
			readBuf:      make([]byte, readBufSize),
		}
	}

	return nil
}

// cleanup closes all writer fds. The temp directory is removed during
// createWriterFiles after all files are unlinked. Idempotent.
func (u *unsortedBuffer) cleanup() error {
	for w, f := range u.writerFiles {
		if f != nil {
			f.Close()
			u.writerFiles[w] = nil
		}
	}
	u.writerFiles = nil
	return nil
}

// finishUnsorted builds blocks from partition files.
// Correctness is validated by TestBuildModeEquivalence.
func (ub *UnsortedBuilder) finishUnsorted() error {
	u := ub.unsortedBuf

	// Fast path: if nothing was ever flushed to disk, all entries are still
	// in memory. Sort in-memory and build blocks directly without file I/O.
	// Check BEFORE flushAll — flushAll sets flushed=true, making this unreachable.
	if !u.flushed.Load() {
		return ub.finishUnsortedFastPath()
	}

	// Flush the default writer's remaining entries to disk.
	if ub.defaultWriterWS != nil {
		if err := ub.defaultWriterWS.flushAll(); err != nil {
			return errors.Join(err, ub.cleanupAll())
		}
	}

	// Merge block counts from all writers before freeing write-phase buffers.
	var allWS []*writerState
	if ub.defaultWriterWS != nil {
		allWS = append(allWS, ub.defaultWriterWS)
	}
	for _, w := range ub.writers {
		if w.ws.blockCounts != nil {
			allWS = append(allWS, w.ws)
		}
	}
	if err := u.mergeBlockCounts(allWS); err != nil {
		return errors.Join(err, ub.cleanupAll())
	}
	for _, ws := range allWS {
		ws.freeBlockCounts()
	}

	// Free write-phase buffers before the read phase.
	if ub.defaultWriterWS != nil {
		ub.defaultWriterWS.free()
		ub.defaultWriterWS = nil
	}
	numSlots := 2 // single-threaded: 2-buffer pipelining
	if ub.b.workers > 1 {
		numSlots = readPipelineDepth * slotsPerReader
	}
	if err := u.prepareForRead(numSlots); err != nil {
		return errors.Join(err, ub.cleanupAll())
	}

	if ub.b.workers > 1 {
		return ub.finishUnsortedParallel()
	}
	return ub.finishUnsortedSingleThreaded()
}

// finishUnsortedFastPath handles small datasets that fit entirely in memory.
// No writer ever flushed, so all entries are still in writer regions.
func (ub *UnsortedBuilder) finishUnsortedFastPath() error {
	b := ub.b
	u := ub.unsortedBuf

	// Collect all writerStates (default + concurrent writers).
	var allWS []*writerState
	if ub.defaultWriterWS != nil {
		allWS = append(allWS, ub.defaultWriterWS)
	}
	for _, w := range ub.writers {
		if w.ws.cursorSets[0] != nil {
			allWS = append(allWS, w.ws)
		}
	}

	// Collect entries from all writers' flat buffers.
	var entries []routedEntry
	var entryBlockIDs []uint32
	for _, ws := range allWS {
		a := ws.activeBuf
		for p := range u.numPartitions {
			n := ws.cursorSets[a][p]
			base := p * ws.regionCap
			for i := range n {
				e := ws.flatBufs[a][base+i]
				entries = append(entries, routedEntry(e))
				prefix := bits.ReverseBytes64(e.k0)
				entryBlockIDs = append(entryBlockIDs, intbits.FastRange32(prefix, u.numBlocks))
			}
		}
	}

	// Free partition regions before allocating sort buffers to reduce peak heap.
	for _, ws := range allWS {
		ws.free()
	}

	// Check context before doing any work
	select {
	case <-b.ctx.Done():
		return errors.Join(b.ctx.Err(), ub.cleanupAll())
	default:
	}

	// Counting sort by blockID
	numBlocks := int(u.numBlocks)
	counts := make([]int, numBlocks)
	for _, bid := range entryBlockIDs {
		counts[bid]++
	}
	offsets := make([]int, numBlocks)
	for i := 1; i < numBlocks; i++ {
		offsets[i] = offsets[i-1] + counts[i-1]
	}
	sorted := make([]routedEntry, len(entries))
	cursors := make([]int, numBlocks)
	copy(cursors, offsets)
	for i, e := range entries {
		bid := entryBlockIDs[i]
		sorted[cursors[bid]] = e
		cursors[bid]++
	}

	if b.workers > 1 {
		b.initParallelWorkers()
	}

	fpSize := b.cfg.fingerprintSize
	for blockID := uint32(0); blockID < u.numBlocks; blockID++ {
		// Periodic context check
		if blockID%blockContextCheckInterval == 0 {
			select {
			case <-b.ctx.Done():
				return errors.Join(b.ctx.Err(), ub.cleanupAll())
			default:
			}
		}

		start := offsets[blockID]
		n := counts[blockID]

		if n == 0 {
			if b.workers > 1 {
				if err := b.dispatchEmptyBlock(blockID); err != nil {
					return errors.Join(err, ub.cleanupAll())
				}
			} else {
				if err := b.commitEmptyBlock(); err != nil {
					return errors.Join(err, ub.cleanupAll())
				}
			}
			continue
		}

		blockEntries := sorted[start : start+n]
		if b.workers > 1 {
			// Fresh allocation, not from pool or flatBuf
			dst := make([]routedEntry, n)
			copy(dst, blockEntries)
			if err := b.dispatchBlockWork(blockID, dst, false); err != nil {
				return errors.Join(err, ub.cleanupAll())
			}
		} else {
			b.builder.Reset()
			for _, e := range blockEntries {
				b.builder.AddKey(e.k0, e.k1, e.payload, extractFingerprint(e.k0, e.k1, fpSize))
			}
			if err := b.buildBlock(); err != nil {
				return errors.Join(err, ub.cleanupAll())
			}
		}
	}

	if err := u.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	ub.unsortedBuf = nil

	if b.workers > 1 {
		return b.drainParallelPipeline()
	}
	return b.finalizeIndex()
}

// finishUnsortedSingleThreaded reads partitions sequentially and builds blocks
// inline without worker goroutines. Used when workers=1 to avoid channel overhead.
func (ub *UnsortedBuilder) finishUnsortedSingleThreaded() error {
	b := ub.b
	u := ub.unsortedBuf
	numParts := u.numPartitions
	fpSize := b.cfg.fingerprintSize

	type readResult struct {
		entries [][]routedEntry
		err     error
	}

	var nextResult *readResult

	// Per-slot fences not needed for single-threaded (no workers sharing flatBuf).
	// Use simple 2-buffer pipelining: read next partition while building current.

	// Read partition 0 inline into slot 0.
	if numParts > 0 {
		entries, err := u.readPartition(0, &u.readSlots[0])
		nextResult = &readResult{entries, err}
	}

	for p := range numParts {
		currentResult := nextResult
		if currentResult.err != nil {
			return errors.Join(currentResult.err, ub.cleanupAll())
		}

		// Start reading next partition in background.
		var wg sync.WaitGroup
		if p+1 < numParts {
			nextResult = &readResult{}
			wg.Add(1)
			result := nextResult
			nextPart := p + 1
			nextPIdx := nextPart % 2
			go func() {
				defer wg.Done()
				result.entries, result.err = u.readPartition(nextPart, &u.readSlots[nextPIdx])
			}()
		}

		// Build blocks inline — no channel dispatch, no worker goroutines.
		partStartBlock := uint32(p * u.blocksPerPart)
		for localIdx, entries := range currentResult.entries {
			blockID := partStartBlock + uint32(localIdx)

			if blockID%blockContextCheckInterval == 0 {
				select {
				case <-b.ctx.Done():
					wg.Wait()
					return errors.Join(b.ctx.Err(), ub.cleanupAll())
				default:
				}
			}

			if len(entries) == 0 {
				if err := b.commitEmptyBlock(); err != nil {
					wg.Wait()
					return errors.Join(err, ub.cleanupAll())
				}
				continue
			}

			b.builder.Reset()
			for _, e := range entries {
				b.builder.AddKey(e.k0, e.k1, e.payload, extractFingerprint(e.k0, e.k1, fpSize))
			}
			if err := b.buildBlock(); err != nil {
				wg.Wait()
				return errors.Join(err, ub.cleanupAll())
			}
		}

		wg.Wait()
	}

	if err := u.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	ub.unsortedBuf = nil

	return b.finalizeIndex()
}

// finishUnsortedParallel reads partitions and dispatches blocks to workers.
// Each reader goroutine owns a pair of dedicated readSlots and alternates
// between them, allowing pread to overlap with workers finishing previous
// entries. Readers are stride-assigned (reader r handles partitions r, r+N,
// r+2N, ...) so main consumes results in round-robin order without
// per-partition channels.
func (ub *UnsortedBuilder) finishUnsortedParallel() error {
	b := ub.b
	b.initParallelWorkers()

	u := ub.unsortedBuf
	numParts := u.numPartitions
	numSlots := len(u.readSlots)
	numReaders := numSlots / slotsPerReader
	if numReaders < 1 {
		numReaders = 1
	}

	// Derived context for reader goroutines. Cancelled on any error to unblock
	// readers waiting on slotReady, preventing hang in readerWg.Wait().
	readerCtx, readerCancel := context.WithCancel(b.ctx)
	defer readerCancel()

	type readResult struct {
		slotIdx int
		entries [][]routedEntry
		err     error
	}

	// Per-slot fences: workers signal Done() after reading entries from a slot's flatBuf.
	slotFences := make([]sync.WaitGroup, numSlots)

	// Per-slot ready channels: signaled when a slot's flatBuf is safe to reuse.
	slotReady := make([]chan struct{}, numSlots)
	for i := range numSlots {
		slotReady[i] = make(chan struct{}, 1)
		slotReady[i] <- struct{}{} // initially ready
	}

	// Per-reader result channels. Main reads round-robin to consume in partition order.
	resultCh := make([]chan readResult, numReaders)
	for r := range numReaders {
		resultCh[r] = make(chan readResult, 1)
	}

	// Stride-assigned reader goroutines: reader r handles partitions r, r+N, r+2N, ...
	var readerWg sync.WaitGroup
	for r := range numReaders {
		readerWg.Add(1)
		go func(r int) {
			defer readerWg.Done()
			defer close(resultCh[r])
			baseSlot := r * slotsPerReader
			slotCycle := 0
			for p := r; p < numParts; p += numReaders {
				slotIdx := baseSlot + slotCycle%slotsPerReader
				slotCycle++
				select {
				case <-slotReady[slotIdx]:
				case <-readerCtx.Done():
					return
				}
				entries, err := u.readPartition(p, &u.readSlots[slotIdx])
				// Guard the send: on any error/cancellation the main consumer
				// stops reading and cancels readerCtx, so a plain blocking send
				// would hang readerWg.Wait() forever. The deferred
				// close(resultCh[r]) tells the consumer this reader exited.
				select {
				case resultCh[r] <- readResult{slotIdx, entries, err}:
				case <-readerCtx.Done():
					return
				}
				if err != nil {
					return
				}
			}
		}(r)
	}

	// fail tears down the reader pool and returns err joined with cleanup. Every
	// error/cancellation exit below funnels through here so they share one shape.
	fail := func(err error) error {
		readerCancel()
		readerWg.Wait()
		return errors.Join(err, ub.cleanupAll())
	}

	// Main thread consumes results in partition order via round-robin.
	for p := range numParts {
		r := p % numReaders
		var result readResult
		select {
		case res, ok := <-resultCh[r]:
			if !ok {
				// Reader r exited early (cancelled) without producing this
				// result. Tear down and surface the cancellation cause.
				return fail(b.ctx.Err())
			}
			result = res
		case <-b.ctx.Done():
			// Parent cancellation while waiting for the next partition. Without
			// this escape main could block here forever if a reader is parked.
			return fail(b.ctx.Err())
		case err := <-b.writerDone:
			// Pool failure (e.g. a worker hit ErrDuplicateKey): the writer exits
			// and the in-flight blocks' fences are orphaned, which parks the
			// reader goroutines so resultCh[r] may never deliver. b.workerCtx is
			// cancelled but it is a child of b.ctx, so b.ctx.Done above never
			// fires. Observe the failure here — mirroring the inner dispatch
			// select — and surface the real error instead of deadlocking. (The
			// readers-parked scenario is always driven by a build error that
			// reached writerDone, so this is the right error, not a bare cancel.)
			b.writerErr = err
			return fail(err)
		}
		if result.err != nil {
			return fail(result.err)
		}

		slotIdx := result.slotIdx

		partStartBlock := uint32(p * u.blocksPerPart)
		for localIdx, entries := range result.entries {
			blockID := partStartBlock + uint32(localIdx)

			if blockID%blockContextCheckInterval == 0 {
				select {
				case <-b.ctx.Done():
					return fail(b.ctx.Err())
				default:
				}
				select {
				case err := <-b.writerDone:
					b.writerErr = err
					return fail(err)
				default:
				}
			}

			if len(entries) == 0 {
				if err := b.dispatchEmptyBlock(blockID); err != nil {
					return fail(err)
				}
				continue
			}

			// Zero-copy dispatch from slot's flatBuf.
			slotFences[slotIdx].Add(1)
			work := blockWork{
				blockID:    blockID,
				entries:    entries,
				fenceWg:    &slotFences[slotIdx],
				keysBefore: b.keysBefore,
			}
			b.keysBefore += uint64(len(entries))
			b.nextBlockToWrite = blockID + 1

			select {
			case b.workChan <- work:
			case <-b.workerCtx.Done():
				slotFences[slotIdx].Done()
				return fail(b.workerCtx.Err())
			case err := <-b.writerDone:
				b.writerErr = err
				slotFences[slotIdx].Done()
				return fail(err)
			}
		}

		// Signal reader that this slot is safe to reuse once workers finish.
		go func(s int) {
			slotFences[s].Wait()
			slotReady[s] <- struct{}{}
		}(slotIdx)
	}

	readerWg.Wait()

	if err := u.cleanup(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	ub.unsortedBuf = nil

	return b.drainParallelPipeline()
}
