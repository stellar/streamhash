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

// initParallelWorkers initializes channels, pools, and starts worker/writer goroutines
// for parallel building. Used by both sorted parallel mode and unsorted parallel mode.
func (b *builder) initParallelWorkers() {
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

	// Start worker goroutines.
	// Wrap in explicit cancel so shutdownWorkers can unblock workers stuck on resultChan.
	ctx, cancel := context.WithCancel(b.ctx)
	b.workerCancel = cancel
	b.workerGroup, b.workerCtx = errgroup.WithContext(ctx)
	for range b.workers {
		b.workerGroup.Go(b.runWorker)
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
		b.writerErr = err
		return err
	}
}

// dispatchEmptyBlock sends an empty block to workers. An empty block has no
// entries, so keysBefore is left unchanged (no payload offset to advance).
func (b *builder) dispatchEmptyBlock(blockID uint32) error {
	return b.dispatchBlockWork(blockID, nil, false)
}

// runWorker is the worker goroutine that builds blocks in parallel.
func (b *builder) runWorker() error {
	// Create block builder for this worker
	blkBuilder, err := newBlockBuilder(b.cfg.algorithm, b.cfg.totalKeys, b.cfg.globalSeed, b.cfg.payloadSize, b.cfg.fingerprintSize)
	if err != nil {
		return err
	}

	fpSize := b.cfg.fingerprintSize
	entrySize := b.cfg.payloadSize + fpSize

	// Pre-allocate reusable payload buffer (grows as needed, reused across blocks)
	// Metadata buffer is NOT reused because it's sent through channel
	var payloadsBuf []byte

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

		var metadataBuf []byte
		var numKeys int
		var payloadHash uint64
		var buildErr error

		if len(work.entries) == 0 {
			// Empty block: reset builder and build empty metadata
			blkBuilder.Reset()
			metadataBuf = b.metadataPool.Get().([]byte)
			var metadataLen int
			metadataLen, _, _, buildErr = blkBuilder.BuildSeparatedInto(metadataBuf, nil)
			metadataBuf = metadataBuf[:metadataLen]
			numKeys = 0
			// Empty block contributes hash of empty slice (deterministic)
			payloadHash = xxhash.Sum64(nil)
		} else {
			// Reset and populate the builder, computing fingerprints from k0/k1
			blkBuilder.Reset()
			for _, e := range work.entries {
				blkBuilder.AddKey(e.k0, e.k1, e.payload, extractFingerprint(e.k0, e.k1, fpSize))
			}

			// Signal that entries are no longer being read. This allows the
			// flatBuf backing the entries to be safely reused for the next
			// partition read (fencing for unsorted parallel mode).
			if work.fenceWg != nil {
				work.fenceWg.Done()
			}

			numKeysInBlock := len(work.entries)
			payloadsNeeded := numKeysInBlock * entrySize

			// Reuse payload buffer (grows if needed)
			if cap(payloadsBuf) < payloadsNeeded {
				payloadsBuf = make([]byte, payloadsNeeded)
			} else {
				payloadsBuf = payloadsBuf[:payloadsNeeded]
			}

			// Get metadata buffer from pool (returned to pool by writer after use)
			metadataBuf = b.metadataPool.Get().([]byte)

			var metadataLen int
			metadataLen, _, numKeys, buildErr = blkBuilder.BuildSeparatedInto(metadataBuf, payloadsBuf)
			if buildErr == nil {
				metadataBuf = metadataBuf[:metadataLen]

				// Compute payload hash and write payloads while data is hot in CPU cache
				if payloadsNeeded > 0 {
					// Hash payload buffer before writing via pwrite
					payloadHash = xxhash.Sum64(payloadsBuf[:payloadsNeeded])
					// Write payloads via pwrite, then buffer can be reused
					payloadOffset := work.keysBefore * uint64(entrySize)
					if werr := b.iw.writePayloadsDirect(payloadsBuf, payloadOffset); werr != nil {
						buildErr = werr
					}
				} else {
					// MPHF-only mode: no payloads, hash empty slice
					payloadHash = xxhash.Sum64(nil)
				}
			}

			// Return entry slice to pool only if it was pool-allocated.
			// Unsorted mode dispatches flatBuf-backed slices with pooled=false
			// and uses fenceWg for lifetime management instead.
			if work.pooled {
				b.putEntrySlice(work.entries)
			}
		}

		// Send result to writer (metadata + payload hash)
		select {
		case b.resultChan <- blockResult{
			blockID:     work.blockID,
			metadata:    metadataBuf,
			numKeys:     numKeys,
			payloadHash: payloadHash,
			err:         buildErr,
		}:
		case <-b.workerCtx.Done():
			return b.workerCtx.Err()
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
func (b *builder) runWriter() {
	defer close(b.writerDone)

	pending := make(map[uint32]blockResult)
	nextBlockID := uint32(0)

	for result := range b.resultChan {
		if result.err != nil {
			b.writerDone <- result.err
			return
		}
		pending[result.blockID] = result

		// Emit all consecutive ready blocks IN ORDER
		// Critical: payload hashes must be folded in block order for deterministic results
		for r, ok := pending[nextBlockID]; ok; r, ok = pending[nextBlockID] {
			delete(pending, nextBlockID)

			// Fold payload hash (order enforced by loop)
			b.iw.foldPayloadHash(r.payloadHash)

			// Write only metadata (payloads already written directly by workers)
			// writeMetadata also updates the streaming metadata hasher
			if err := b.iw.writeMetadata(r.metadata, r.numKeys); err != nil {
				b.writerDone <- err
				return
			}

			// Return metadata buffer to pool for reuse
			b.metadataPool.Put(r.metadata[:cap(r.metadata)])

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

// drainParallelPipeline closes the work channel, waits for workers and writer
// to finish, then finalizes the index.
func (b *builder) drainParallelPipeline() error {
	// Close work channel to signal workers we're done
	close(b.workChan)
	b.workersShutDown = true // Prevents double-close in Close()/shutdownWorkers()

	// Wait for all workers to finish
	if err := b.workerGroup.Wait(); err != nil {
		close(b.resultChan)
		<-b.writerDone // Wait for writer to exit before cleanup
		primaryErr := fmt.Errorf("worker error: %w", err)
		return errors.Join(primaryErr, b.cleanup())
	}

	// Close result channel to signal writer we're done
	close(b.resultChan)

	// Wait for writer to finish
	if err := <-b.writerDone; err != nil {
		primaryErr := fmt.Errorf("writer error: %w", err)
		return errors.Join(primaryErr, b.cleanup())
	}

	return b.finalizeIndex()
}

// shutdownWorkers closes the work channel and waits for worker and writer
// goroutines to exit. Safe to call multiple times (no-op after first call).
//
// Cancel is called first to unblock workers that may be stuck waiting to
// send results (e.g., when the writer has already exited due to an error).
func (b *builder) shutdownWorkers() {
	if b.workersShutDown || b.workChan == nil {
		return
	}
	b.workersShutDown = true
	if b.workerCancel != nil {
		b.workerCancel()
	}
	close(b.workChan)
	_ = b.workerGroup.Wait()
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
