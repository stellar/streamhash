package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sync"
	"sync/atomic"

	intbits "github.com/tamirms/streamhash/internal/bits"
	"github.com/tamirms/streamhash/internal/sherr"
	"golang.org/x/sync/errgroup"
)

const (
	// contextCheckInterval is how often to check for context cancellation during AddKey.
	contextCheckInterval = 10000

	// maxKeyLength is the maximum key length accepted by AddKey.
	maxKeyLength = 65535

	// maxKeys is the maximum number of keys (~1.1 trillion). The limit comes
	// from the 40-bit cumulative key counter in the RAM index (see header.go):
	// the sentinel entry stores KeysBefore == totalKeys, so the largest
	// representable total is 2^40-1, not 2^40 (which would wrap to 0).
	maxKeys = uint64(1)<<40 - 1
)

// builder is the internal shared core for SortedBuilder and UnsortedBuilder.
type builder struct {
	ctx              context.Context
	cfg              *buildConfig
	numBlocks        uint32
	iw               *indexWriter
	builder          blockBuilder // Block builder (also used directly in single-threaded mode)
	output           string
	nextBlockToWrite uint32
	currentBlockIdx  uint32
	firstKey         bool
	keyCounter       int
	closed           bool

	// Parallel mode fields (when workers > 1)
	workers         int
	workChan        chan blockWork
	resultChan      chan blockResult
	workerGroup     *errgroup.Group
	workerCtx       context.Context
	workerCancel    context.CancelFunc // Cancels workerCtx to unblock stuck workers
	writerDone      chan error
	writerErr       error
	workersShutDown atomic.Bool // True once the worker pipeline has been torn down

	// writerFaultHook, when non-nil, is invoked by runWriter before each
	// metadata write with the block ID; a non-nil return injects a writer-side
	// failure. Test-only (always nil in production) — used to exercise the
	// pure-writer-error teardown path, which a real ENOSPC/EIO metadata write
	// would otherwise be the only way to reach.
	writerFaultHook func(blockID uint32) error

	// Block accumulation for parallel mode
	pendingEntries []routedEntry // Current block being accumulated
	keysBefore     uint64        // Cumulative keys before current block
	entryPool      sync.Pool     // Pool of []routedEntry slices for reuse
	metadataPool   sync.Pool     // Pool of []byte slices for metadata buffers

	// Reusable buffers for single-threaded buildBlock/commitEmptyBlock
	metadataBuf []byte
	payloadsBuf []byte

	// Sorted mode validation
	lastBlockID int64 // Last seen block ID (-1 initially), for detecting out-of-order keys

	// Key count tracking
	totalKeysAdded uint64 // Number of keys added via AddKey
}

// newBuilder creates the shared builder core used by NewSortedBuilder and NewUnsortedBuilder.
//
// totalKeys must be the exact number of keys that will be added.
//
// Use WithWorkers(N) to enable parallel building with N workers.
// With workers > 1, blocks are built in parallel while maintaining
// the streaming API and O(W × block_size) memory.
func newBuilder(ctx context.Context, output string, totalKeys uint64, opts ...BuildOption) (*builder, error) {
	if totalKeys == 0 {
		return nil, sherr.ErrEmptyIndex
	}

	if totalKeys > maxKeys {
		return nil, sherr.ErrTooManyKeys
	}

	cfg := defaultBuildConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	cfg.totalKeys = totalKeys

	if cfg.payloadSize < 0 || cfg.payloadSize > maxPayloadSize {
		return nil, sherr.ErrPayloadTooLarge
	}
	if cfg.fingerprintSize < 0 || cfg.fingerprintSize > maxFingerprintSize {
		return nil, sherr.ErrFingerprintTooLarge
	}

	// Get the block builder for building
	bldr, err := newBlockBuilder(cfg.algorithm, totalKeys, cfg.globalSeed, cfg.payloadSize, cfg.fingerprintSize)
	if err != nil {
		return nil, err
	}

	numBlocks := bldr.NumBlocks()

	iw, err := newIndexWriter(output, cfg, numBlocks, bldr)
	if err != nil {
		return nil, fmt.Errorf("create index writer: %w", err)
	}

	// Determine worker count
	workers := cfg.workers
	if workers <= 0 {
		workers = 1 // Default to single-threaded
	}
	if workers > int(numBlocks) {
		workers = int(numBlocks)
	}

	b := &builder{
		ctx:         ctx,
		cfg:         cfg,
		numBlocks:   numBlocks,
		iw:          iw,
		builder:     bldr,
		output:      output,
		firstKey:    true,
		workers:     workers,
		lastBlockID: -1,
	}

	return b, nil
}

// validateKey checks key length and payload overflow. Shared by all builder types.
func (b *builder) validateKey(key []byte, payload uint64) error {
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
	return nil
}

// checkContextSlow is the slow path for context checking, called every 10K keys.
func (b *builder) checkContextSlow() error {
	b.keyCounter = 0
	select {
	case <-b.ctx.Done():
		return b.ctx.Err()
	default:
	}
	if b.workers > 1 && b.writerDone != nil {
		select {
		case err := <-b.writerDone:
			return b.consumePipelineError(err)
		default:
		}
	}
	return nil
}

// consumePipelineError interprets a value read from writerDone. A non-nil err is
// the writer's failure (recorded and returned). A nil err means writerDone was
// CLOSED — the pipeline was already torn down and any real error consumed by an
// earlier read — so surface the stored error (or ErrBuilderClosed) rather than
// returning nil, which would let the caller silently drop the block it was about
// to dispatch (e.g. when a prior AddKey error was ignored).
func (b *builder) consumePipelineError(err error) error {
	if err != nil {
		b.writerErr = err
		return err
	}
	if b.writerErr != nil {
		return b.writerErr
	}
	return sherr.ErrBuilderClosed
}

// addKeySingleThreaded handles AddKey in single-threaded sorted mode.
// Parameters are pre-parsed in AddKey for efficiency.
func (b *builder) addKeySingleThreaded(k0, k1 uint64, payload uint64, fingerprint uint32, blockIdx uint32) error {
	if b.firstKey {
		// Emit empty blocks for indices 0 to blockIdx-1
		for b.nextBlockToWrite < blockIdx {
			if err := b.commitEmptyBlock(); err != nil {
				return err
			}
			b.nextBlockToWrite++
		}
		// Reset builder after gap emission leaves it in post-build state
		b.resetCurrentBuilder()
		b.currentBlockIdx = blockIdx
		b.firstKey = false
	} else if blockIdx != b.currentBlockIdx {
		// Emit current block
		if b.currentBlockKeysAdded() > 0 {
			if err := b.buildBlock(); err != nil {
				return err
			}
			b.nextBlockToWrite++
		}

		// Emit empty blocks for gaps
		for b.nextBlockToWrite < blockIdx {
			if err := b.commitEmptyBlock(); err != nil {
				return err
			}
			b.nextBlockToWrite++
		}
		// Reset builder after gap emission leaves it in post-build state
		b.resetCurrentBuilder()

		b.currentBlockIdx = blockIdx
	}

	// Add key to block builder (values already parsed in AddKey)
	b.builder.AddKey(k0, k1, payload, fingerprint)
	return nil
}

// currentBlockKeysAdded returns the number of keys added to the current block builder.
func (b *builder) currentBlockKeysAdded() int {
	return b.builder.KeysAdded()
}

// resetCurrentBuilder resets the current block builder for reuse.
func (b *builder) resetCurrentBuilder() {
	b.builder.Reset()
}

// commitEmptyBlock writes an empty block's metadata directly to the index.
// Resets the builder, builds empty metadata, and commits via pwrite.
func (b *builder) commitEmptyBlock() error {
	b.builder.Reset()
	metadataNeeded := b.estimateMetadataSize()
	if cap(b.metadataBuf) < metadataNeeded {
		b.metadataBuf = make([]byte, metadataNeeded)
	} else {
		b.metadataBuf = b.metadataBuf[:metadataNeeded]
	}
	metadataLen, _, _, _ := b.builder.BuildSeparatedInto(b.metadataBuf, nil)
	return b.iw.commitBlockWithData(b.metadataBuf[:metadataLen], nil, 0)
}

// estimateMetadataSize returns max metadata size for the current block.
func (b *builder) estimateMetadataSize() int {
	return b.builder.MaxMetadataSizeForCurrentBlock()
}

// finishSorted completes the index in sorted mode.
func (b *builder) finishSorted() error {
	if b.closed {
		return sherr.ErrBuilderClosed
	}
	b.closed = true

	if b.totalKeysAdded != b.cfg.totalKeys {
		primaryErr := fmt.Errorf("%w: expected %d, got %d", sherr.ErrKeyCountMismatch, b.cfg.totalKeys, b.totalKeysAdded)
		return errors.Join(primaryErr, b.cleanup())
	}

	if b.workers > 1 {
		return b.finishParallel()
	}
	return b.finishSingleThreaded()
}

// finishSingleThreaded completes the build in single-threaded sorted mode.
func (b *builder) finishSingleThreaded() error {
	// Emit final block
	if b.currentBlockKeysAdded() > 0 {
		if err := b.buildBlock(); err != nil {
			return errors.Join(err, b.cleanup())
		}
	}

	// Emit trailing empty blocks
	for b.iw.blockCount < b.numBlocks {
		if err := b.commitEmptyBlock(); err != nil {
			return errors.Join(err, b.cleanup())
		}
	}

	return b.finalizeIndex()
}

// buildBlock builds the current block into reusable buffers and writes via pwrite.
func (b *builder) buildBlock() error {
	numKeys := b.currentBlockKeysAdded()
	entrySize := b.cfg.payloadSize + b.cfg.fingerprintSize

	// Reuse buffers (grow if needed, avoids per-block allocation)
	payloadsNeeded := numKeys * entrySize
	if cap(b.payloadsBuf) < payloadsNeeded {
		b.payloadsBuf = make([]byte, payloadsNeeded)
	} else {
		b.payloadsBuf = b.payloadsBuf[:payloadsNeeded]
	}
	metadataNeeded := b.estimateMetadataSize()
	if cap(b.metadataBuf) < metadataNeeded {
		b.metadataBuf = make([]byte, metadataNeeded)
	} else {
		b.metadataBuf = b.metadataBuf[:metadataNeeded]
	}

	metadataLen, _, builtNumKeys, err := b.builder.BuildSeparatedInto(b.metadataBuf, b.payloadsBuf)
	if err != nil {
		return err
	}

	return b.iw.commitBlockWithData(b.metadataBuf[:metadataLen], b.payloadsBuf, builtNumKeys)
}

// close aborts the sorted build and cleans up resources.
func (b *builder) close() error {
	if b.closed {
		b.shutdownWorkers()
		return nil
	}
	b.closed = true
	return b.cleanup()
}

// cleanup shuts down any running worker goroutines and removes the output file.
// Workers must be stopped before closing the file, which they may still be
// writing to via writePayloadsDirect.
func (b *builder) cleanup() error {
	b.shutdownWorkers()
	return errors.Join(b.iw.close(), os.Remove(b.output))
}

// finalizeIndex finalizes the index writer, deleting the partial output file if
// finalization fails — otherwise a failed Finish would leave a corrupt index on
// disk, since Close() after Finish is a no-op.
func (b *builder) finalizeIndex() error {
	if err := b.iw.finalize(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	return nil
}

// SortedBuilder builds an index from keys that arrive in block-sorted order.
//
// Usage:
//
//	builder, err := streamhash.NewSortedBuilder(ctx, "index.idx", totalKeys, opts...)
//	if err != nil { return err }
//	defer builder.Close()
//
//	for key, payload := range sortedData {
//	    if err := builder.AddKey(key, payload); err != nil { return err }
//	}
//	return builder.Finish()
type SortedBuilder struct {
	b *builder
}

// NewSortedBuilder creates a builder for sorted input.
// Keys must be added in block-sorted order via AddKey.
// Use WithWorkers(N) to parallelize block building during Finish.
func NewSortedBuilder(ctx context.Context, output string, totalKeys uint64, opts ...BuildOption) (*SortedBuilder, error) {
	b, err := newBuilder(ctx, output, totalKeys, opts...)
	if err != nil {
		return nil, err
	}
	if b.workers > 1 {
		b.initParallelWorkers()
		b.pendingEntries = b.getEntrySlice()
	}
	return &SortedBuilder{b: b}, nil
}

// AddKey adds a key-payload pair. Keys must be in block-sorted order.
func (sb *SortedBuilder) AddKey(key []byte, payload uint64) error {
	b := sb.b

	if b.closed {
		return sherr.ErrBuilderClosed
	}

	// Inline validation, context check, and routing for hot-path performance.
	// Delegating to helper methods adds +23 instructions per key from wrapper
	// overhead, lost bounds-check elimination, and error plumbing — measurable
	// at 4B+ keys.
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
		if err := b.checkContextSlow(); err != nil {
			return err
		}
	}

	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	prefix := bits.ReverseBytes64(k0)
	blockIdx := intbits.FastRange32(prefix, b.numBlocks)

	if int64(blockIdx) < b.lastBlockID {
		return sherr.ErrUnsortedInput
	}
	b.lastBlockID = int64(blockIdx)

	var err error
	if b.workers > 1 {
		err = b.addKeyParallel(k0, k1, payload, blockIdx)
	} else {
		fingerprint := extractFingerprint(k0, k1, b.cfg.fingerprintSize)
		err = b.addKeySingleThreaded(k0, k1, payload, fingerprint, blockIdx)
	}
	if err != nil {
		return err
	}
	b.totalKeysAdded++
	return nil
}

// Finish completes the index and writes it to disk.
func (sb *SortedBuilder) Finish() error {
	return sb.b.finishSorted()
}

// Close aborts the build and cleans up resources.
// Safe to call after Finish.
func (sb *SortedBuilder) Close() error {
	return sb.b.close()
}
