package streamhash

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"
)

const (
	// workChanBufferMultiplier is the multiplier for work channel buffer size
	workChanBufferMultiplier = 2

	// minPoolCapacity is the minimum entry pool capacity per block.
	// Ensures reasonable allocation even for indexes with many small blocks.
	minPoolCapacity = 1024
)

// routedEntry holds a key with pre-computed routing values for parallel building.
// k0, k1 are the first 16 bytes of the key as little-endian uint64s.
// Fingerprints are computed at index-write time via extractFingerprint(k0, k1, fpSize),
// not stored in the entry (saves 8B per entry due to alignment).
type routedEntry struct {
	k0      uint64 // First 8 bytes of key (little-endian)
	k1      uint64 // Second 8 bytes of key (little-endian)
	payload uint64
}

// blockWork represents work to be done by a worker.
type blockWork struct {
	blockID    uint32
	entries    []routedEntry
	pooled     bool            // true if entries was obtained from entryPool (should be returned)
	fenceWg    *sync.WaitGroup // if non-nil, Done() after reading entries (for flatBuf reuse fencing)
	keysBefore uint64          // Cumulative keys before this block (for payload offset calculation)
}

// blockResult holds the result of building a block (separated layout).
type blockResult struct {
	blockID     uint32
	metadata    []byte // Only metadata (payloads written directly by workers)
	numKeys     int
	payloadHash uint64 // xxHash64 of this block's payloads (for streaming hash)
	err         error
}

// startPipeline creates the parallel pipeline's channels and pools, and
// starts its writer goroutine and b.workers goroutines running worker, which
// send blockResults on resultChan and return once workerCtx is done. Used by
// sorted parallel mode, unsorted parallel mode and BuildFromSortedFiles.
func (b *builder) startPipeline(worker func() error) {
	b.workChan = make(chan blockWork, b.workers*workChanBufferMultiplier)
	b.resultChan = make(chan blockResult, b.workers*workChanBufferMultiplier)
	b.writerDone = make(chan error, 1)

	// Estimate max keys per block for entry pool (2x average with 1024 minimum)
	avgKeysPerBlock := int(b.cfg.totalKeys / uint64(b.numBlocks))
	maxKeysPerBlock := max(avgKeysPerBlock*2, minPoolCapacity)
	b.entryPool.New = func() any {
		return make([]routedEntry, 0, maxKeysPerBlock)
	}

	// Initialize metadata buffer pool with max metadata size
	metadataBufSize := b.builder.MaxIndexMetadataSize()
	b.metadataPool.New = func() any {
		return make([]byte, metadataBufSize)
	}

	// Wrap in explicit cancel so shutdownWorkers can unblock workers stuck on resultChan.
	ctx, cancel := context.WithCancel(b.ctx)
	b.workerCancel = cancel
	b.workerGroup, b.workerCtx = errgroup.WithContext(ctx)
	for range b.workers {
		b.workerGroup.Go(worker)
	}

	// Start writer goroutine
	go b.runWriter()
}

// addKeyParallel handles AddKey in parallel sorted mode.
// Parameters are pre-parsed in AddKey for efficiency.
//
// Writer errors are detected at block boundaries (dispatchBlock/dispatchEmptyBlock
// check writerDone in their blocking select) and periodically via AddKey's context
// check interval. This avoids per-entry channel overhead (~3-5ns × 10M keys).
func (b *builder) addKeyParallel(k0, k1 uint64, payload uint64, blockIdx uint32) error {
	if b.firstKey {
		// Dispatch empty blocks for indices 0 to blockIdx-1
		for b.nextBlockToWrite < blockIdx {
			if err := b.dispatchEmptyBlock(b.nextBlockToWrite); err != nil {
				return err
			}
		}
		b.currentBlockIdx = blockIdx
		b.firstKey = false
	} else if blockIdx != b.currentBlockIdx {
		// Dispatch current block
		if len(b.pendingEntries) > 0 {
			if err := b.dispatchBlock(); err != nil {
				return err
			}
		}

		// Dispatch empty blocks for gaps
		for b.nextBlockToWrite < blockIdx {
			if err := b.dispatchEmptyBlock(b.nextBlockToWrite); err != nil {
				return err
			}
		}

		b.currentBlockIdx = blockIdx
	}

	// Accumulate entry in pending block (values already parsed in AddKey)
	b.pendingEntries = append(b.pendingEntries, routedEntry{
		k0:      k0,
		k1:      k1,
		payload: payload,
	})

	return nil
}

// dispatchBlock sends the pending block to workers for building (sorted mode).
// Wraps dispatchBlockWork with pendingEntries management.
func (b *builder) dispatchBlock() error {
	entries := b.pendingEntries
	b.pendingEntries = b.getEntrySlice()
	return b.dispatchBlockWork(b.currentBlockIdx, entries, true)
}

// dispatchBlockWork sends a block with explicit parameters to workers for building.
// Maintains keysBefore as a running accumulator for payload offset calculation.
// Used by both sorted mode (via dispatchBlock) and unsorted mode directly.
// The pooled flag indicates whether entries should be returned to the pool after use.
func (b *builder) dispatchBlockWork(blockID uint32, entries []routedEntry, pooled bool) error {
	work := blockWork{
		blockID:    blockID,
		entries:    entries,
		pooled:     pooled,
		keysBefore: b.keysBefore,
	}
	b.keysBefore += uint64(len(entries))
	b.nextBlockToWrite = blockID + 1

	select {
	case b.workChan <- work:
		return nil
	case <-b.workerCtx.Done():
		return b.workerCtx.Err()
	case err := <-b.writerDone:
		// A nil read here means writerDone was closed (pipeline torn down); do
		// not drop this block — consumePipelineError surfaces the real error.
		return b.consumePipelineError(err)
	}
}

// dispatchEmptyBlock sends an empty block to workers. An empty block has no
// entries, so keysBefore is left unchanged (no payload offset to advance).
func (b *builder) dispatchEmptyBlock(blockID uint32) error {
	return b.dispatchBlockWork(blockID, nil, false)
}

// blockSolver is a worker's block builder, with the payload buffer it reuses
// across blocks.
type blockSolver struct {
	b           *builder
	blk         blockBuilder
	payloadsBuf []byte
}

func (b *builder) newBlockSolver() (*blockSolver, error) {
	blk, err := newBlockBuilder(b.cfg.algorithm, b.cfg.totalKeys, b.cfg.globalSeed, b.cfg.payloadSize, b.cfg.fingerprintSize)
	if err != nil {
		return nil, err
	}
	return &blockSolver{b: b, blk: blk}, nil
}

// loadBlock resets blk and adds entries to it, computing their fingerprints
// from k0 and k1.
func (b *builder) loadBlock(blk blockBuilder, entries []routedEntry) {
	fpSize := b.cfg.fingerprintSize
	blk.Reset()
	for _, e := range entries {
		blk.AddKey(e.k0, e.k1, e.payload, extractFingerprint(e.k0, e.k1, fpSize))
	}
}

// build solves the loaded block as blockID, writes its payloads after the
// keysBefore keys of the blocks before it, and returns the result for the
// writer. The metadata buffer comes from the pool; the writer returns it.
func (s *blockSolver) build(blockID uint32, keysBefore uint64) blockResult {
	b := s.b
	entrySize := b.cfg.payloadSize + b.cfg.fingerprintSize
	payloadsNeeded := s.blk.KeysAdded() * entrySize
	if cap(s.payloadsBuf) < payloadsNeeded {
		s.payloadsBuf = make([]byte, payloadsNeeded)
	}
	payloads := s.payloadsBuf[:payloadsNeeded]
	metadataBuf := b.metadataPool.Get().([]byte)
	metadataLen, _, numKeys, err := s.blk.BuildSeparatedInto(metadataBuf, payloads)
	result := blockResult{blockID: blockID, metadata: metadataBuf[:metadataLen], numKeys: numKeys, err: err}
	if err == nil {
		// Hash and write the payloads while they are hot in CPU cache.
		result.payloadHash = xxhash.Sum64(payloads)
		result.err = b.iw.writePayloadsDirect(payloads, keysBefore*uint64(entrySize))
	}
	return result
}

// sendResult hands a block's result to the writer and returns the block's
// error, if any, for the worker to return. A block build error must
// propagate to the errgroup, not just ride along in blockResult.err:
// returning it cancels workerCtx, which releases any peer workers blocked
// sending on resultChan after the writer exits on the first error --
// otherwise they (and drainParallelPipeline's workerGroup.Wait) deadlock.
// Wait then surfaces this error to the build.
func (b *builder) sendResult(result blockResult) error {
	select {
	case b.resultChan <- result:
		return result.err
	case <-b.workerCtx.Done():
		return b.workerCtx.Err()
	}
}

// runWorker is the worker goroutine that builds blocks in parallel.
func (b *builder) runWorker() error {
	solver, err := b.newBlockSolver()
	if err != nil {
		return err
	}

	for work := range b.workChan {
		select {
		case <-b.workerCtx.Done():
			// Release fence before returning so background readers don't deadlock.
			if work.fenceWg != nil {
				work.fenceWg.Done()
			}
			return b.workerCtx.Err()
		default:
		}

		b.loadBlock(solver.blk, work.entries)

		// Signal that entries are no longer being read. This allows the
		// flatBuf backing the entries to be safely reused for the next
		// partition read (fencing for unsorted parallel mode).
		if work.fenceWg != nil {
			work.fenceWg.Done()
		}

		result := solver.build(work.blockID, work.keysBefore)

		// Return entry slice to pool only if it was pool-allocated.
		// Unsorted mode dispatches flatBuf-backed slices with pooled=false
		// and uses fenceWg for lifetime management instead.
		if work.pooled {
			b.putEntrySlice(work.entries)
		}

		if err := b.sendResult(result); err != nil {
			return err
		}
	}

	return nil
}

// runWriter is the writer goroutine that writes metadata in order.
//
// Error handling flow:
//   - On error, sends to writerDone (buffered, size 1) and returns
//   - Writer errors are detected at block boundaries (dispatchBlock/dispatchEmptyBlock
//     check writerDone in their blocking select) and periodically via AddKey's context
//     check interval
//   - finishParallel checks writerErr first, then waits on writerDone
//   - The channel is closed by defer, so finishParallel receives nil if no error was sent
//
// Streaming hash: The writer folds payload hashes in block order into the streaming
// hasher. This produces a deterministic hash-of-hashes that can be verified at read time.
// failWriter records the writer's terminating error on writerDone and cancels
// the worker context. The cancellation is the single uniform funnel that closes
// the whole teardown-deadlock class: a pure writer-side error (e.g. an ENOSPC
// metadata write) produces no worker buildErr, so without it the errgroup
// context is never cancelled and workers parked on the resultChan send — plus
// drainParallelPipeline's workerGroup.Wait — would hang forever. Combined with
// runWorker returning buildErr (which cancels via the errgroup), this guarantees
// the invariant: ANY internal failure cancels b.workerCtx exactly once.
// workerCancel is idempotent (also called by shutdownWorkers).
func (b *builder) failWriter(err error) {
	b.writerDone <- err
	b.workerCancel()
}

func (b *builder) runWriter() {
	defer close(b.writerDone)

	pending := make(map[uint32]blockResult)
	nextBlockID := uint32(0)

	for result := range b.resultChan {
		if result.err != nil {
			b.failWriter(result.err)
			return
		}
		pending[result.blockID] = result

		// Emit all consecutive ready blocks IN ORDER
		// Critical: payload hashes must be folded in block order for deterministic results
		for r, ok := pending[nextBlockID]; ok; r, ok = pending[nextBlockID] {
			delete(pending, nextBlockID)

			// Fold payload hash (order enforced by loop)
			b.iw.foldPayloadHash(r.payloadHash)

			// Test-only fault injection for the writer-error teardown path.
			if b.writerFaultHook != nil {
				if ferr := b.writerFaultHook(nextBlockID); ferr != nil {
					b.failWriter(ferr)
					return
				}
			}

			// Write only metadata (payloads already written directly by workers)
			// writeMetadata also updates the streaming metadata hasher
			if err := b.iw.writeMetadata(r.metadata, r.numKeys); err != nil {
				b.failWriter(err)
				return
			}

			// Return metadata buffer to pool for reuse
			//lint:ignore SA6002 slice value boxing is acceptable; pointer-to-slice adds complexity
			b.metadataPool.Put(r.metadata[:cap(r.metadata)]) //nolint:staticcheck

			nextBlockID++
		}
	}
}

// finishParallel completes the build in parallel sorted mode.
//
// Shutdown sequence:
//  1. Dispatch remaining work (final block + trailing empty blocks)
//  2. Drain pipeline (close workChan, wait workers, close resultChan, wait writer)
//  3. Finalize index
//
// Note: If writer panics, the defer in runWriter closes writerDone,
// so this wait receives nil (no deadlock). A stuck writer would block
// forever, but that indicates a bug that a timeout would only mask.
func (b *builder) finishParallel() error {
	// Check for writer errors first
	if b.writerErr != nil {
		return errors.Join(b.writerErr, b.cleanup())
	}

	// Dispatch final block if it has entries
	if len(b.pendingEntries) > 0 {
		if err := b.dispatchBlock(); err != nil {
			return errors.Join(err, b.cleanup())
		}
	} else {
		// Return unused slice to pool
		b.putEntrySlice(b.pendingEntries)
		b.pendingEntries = nil
	}

	// Dispatch trailing empty blocks
	for b.nextBlockToWrite < b.numBlocks {
		if err := b.dispatchEmptyBlock(b.nextBlockToWrite); err != nil {
			return errors.Join(err, b.cleanup())
		}
	}

	return b.drainParallelPipeline()
}

// drainParallelPipeline closes the work channel, waits for the workers and
// the writer to finish, then finalizes the index, unless one of them failed
// or the pipeline was canceled.
func (b *builder) drainParallelPipeline() error {
	// Close work channel to signal workers we're done
	close(b.workChan)
	b.workersShutDown.Store(true) // Prevents double-close in Close()/shutdownWorkers()

	// Read the cancellation before Wait, which cancels workerCtx itself.
	canceled := b.workerCtx.Err()
	werr := b.workerGroup.Wait()
	close(b.resultChan)
	cause := <-b.writerDone
	// A block build error reaches writerDone through failWriter and is the
	// worker's error too; the writer's copy is the cause as it is, and werr
	// is then a bare context.Canceled. The prefixes name where an error of
	// the pipeline's own arose.
	switch {
	case cause != nil && werr == nil:
		cause = fmt.Errorf("writer error: %w", cause)
	case cause == nil && werr != nil:
		cause = fmt.Errorf("worker error: %w", werr)
	case cause == nil:
		// A canceled pipeline never finalizes: its producer may have stopped
		// before dispatching every block.
		cause = canceled
	}
	if cause != nil {
		return errors.Join(cause, b.cleanup())
	}
	return b.finalizeIndex()
}

// shutdownWorkers closes the work channel and waits for worker and writer
// goroutines to exit. Safe to call multiple times (no-op after first call).
//
// Cancel is called first to unblock workers that may be stuck waiting to
// send results (e.g., when the writer has already exited due to an error).
func (b *builder) shutdownWorkers() {
	if b.workersShutDown.Load() || b.workChan == nil {
		return
	}
	b.workersShutDown.Store(true)
	if b.workerCancel != nil {
		b.workerCancel()
	}
	close(b.workChan)
	_ = b.workerGroup.Wait()
	// Cancelled workers return on workerCtx.Done() without draining the rest of
	// workChan, so release the fences on any work items they left behind. The
	// unsorted-parallel finish parks a slot-reuse goroutine on each fence; an
	// un-Done fence would leak that goroutine forever. (On the success path
	// drainParallelPipeline closes workChan without cancelling, so workers drain
	// every item themselves and this loop sees nothing.)
	for work := range b.workChan {
		if work.fenceWg != nil {
			work.fenceWg.Done()
		}
	}
	close(b.resultChan)
	<-b.writerDone
}

// getEntrySlice gets a []routedEntry slice from the pool.
func (b *builder) getEntrySlice() []routedEntry {
	return b.entryPool.Get().([]routedEntry)[:0]
}

// putEntrySlice returns a []routedEntry slice to the pool.
func (b *builder) putEntrySlice(s []routedEntry) {
	//lint:ignore SA6002 slice value boxing is acceptable; pointer-to-slice adds complexity
	b.entryPool.Put(s[:0]) //nolint:staticcheck
}
