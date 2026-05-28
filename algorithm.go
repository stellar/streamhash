package streamhash

import (
	"fmt"

	"github.com/tamirms/streamhash/internal/bijection"
	"github.com/tamirms/streamhash/internal/ptrhash"
	"github.com/tamirms/streamhash/internal/sherr"
)

// Algorithm identifies the MPHF algorithm used for block construction.
// This is stored in the file header.
type Algorithm uint16

const (
	// AlgoBijection uses EF/GR encoding with O(128) query.
	AlgoBijection Algorithm = 0

	// AlgoPTRHash uses PTRHash-style Cuckoo with 8-bit pilots.
	AlgoPTRHash Algorithm = 1
)

// String returns the algorithm name.
func (a Algorithm) String() string {
	switch a {
	case AlgoBijection:
		return "bijection"
	case AlgoPTRHash:
		return "ptrhash"
	default:
		return "unknown"
	}
}

// blockBuilder defines the interface for building MPHF index blocks.
//
// This interface abstracts algorithm-specific details (bucket sizes, encoding
// schemes, solver strategies) from the main framework. Each algorithm (bijection,
// ptrhash) implements this interface with its own internal parameters.
//
// # Lifecycle
//
// A blockBuilder is created once per index build via newBlockBuilder(). It is
// then used to build multiple blocks sequentially:
//
//  1. Create: newBlockBuilder(algo, totalKeys, seed, payloadSize, fpSize)
//  2. For each block:
//     a. Add keys: AddKey(k0, k1, payload, fingerprint) for each key in the block
//     b. Get buffer: at least MaxMetadataSizeForCurrentBlock() bytes
//     c. Build: BuildSeparatedInto(metadataDst, payloadsDst)
//     d. Reset: Reset() to prepare for next block
//
// # Thread Safety
//
// A blockBuilder is NOT safe for concurrent use. In parallel builds, each
// worker goroutine creates its own builder instance.
//
// # Memory
//
// Builders are designed for reuse across blocks. Call Reset() between blocks
// rather than creating new builders. Internal buffers are retained and reused
// to minimize allocations.
type blockBuilder interface {
	// NumBlocks returns the total number of blocks for the index.
	// This is computed at construction time based on totalKeys and the
	// algorithm's lambda (keys per bucket) and buckets per block parameters.
	// The value is cached and does not change.
	NumBlocks() uint32

	// GlobalConfigSize returns the size in bytes of algorithm-specific
	// configuration that will be written to the file header.
	// Currently returns 0 for both algorithms (they use compile-time constants),
	// but future algorithms may store tunable parameters here.
	GlobalConfigSize() int

	// EncodeGlobalConfigInto writes algorithm-specific configuration to dst.
	// Returns the number of bytes written (same as GlobalConfigSize()).
	// The dst slice must have at least GlobalConfigSize() bytes available.
	EncodeGlobalConfigInto(dst []byte) int

	// AddKey adds a key to the current block being built.
	//
	// Parameters:
	//   - k0: first 8 bytes of the key as little-endian uint64
	//   - k1: second 8 bytes of the key as little-endian uint64
	//   - payload: user data to store with this key (up to 8 bytes, packed)
	//   - fingerprint: pre-extracted from key end for verification (up to 4 bytes)
	//
	// Keys are accumulated internally until BuildSeparatedInto is called.
	// The order of AddKey calls within a block does not matter.
	AddKey(k0, k1 uint64, payload uint64, fingerprint uint32)

	// KeysAdded returns the number of keys added to the current block
	// since the last Reset() call (or since construction if never reset).
	KeysAdded() int

	// Reset clears the builder state for reuse with the next block.
	// All accumulated keys are discarded. Internal buffers are retained
	// for reuse to minimize allocations.
	Reset()

	// MaxIndexMetadataSize returns the worst-case maximum metadata size
	// for any block, regardless of key count or distribution.
	//
	// This is used for file size estimation before any keys are added.
	// The actual metadata size after building is typically much smaller.
	//
	// Algorithm-specific values:
	//   - Bijection: ~8220 bytes (checkpoints + EF + seeds + fallback list)
	//   - PTRHash: ~3500 bytes (pilots + remap table)
	MaxIndexMetadataSize() int

	// MaxMetadataSizeForCurrentBlock returns the maximum metadata size
	// for the current block based on the keys added so far.
	//
	// Call this after adding all keys but before BuildSeparatedInto to
	// determine the buffer size needed. The actual size after building
	// will be <= this value.
	//
	// For empty blocks (KeysAdded() == 0), returns the size needed for
	// empty block metadata.
	MaxMetadataSizeForCurrentBlock() int

	// BuildSeparatedInto encodes the current block into the provided buffers.
	//
	// This is the main build operation that:
	//  1. Solves the MPHF (finds seeds/pilots that create a perfect hash)
	//  2. Encodes metadata (seeds, bucket info) into metadataDst
	//  3. Writes payloads (fingerprint + payload) into payloadsDst at
	//     computed slot positions
	//
	// Parameters:
	//   - metadataDst: buffer for encoded metadata, must be at least
	//     MaxMetadataSizeForCurrentBlock() bytes
	//   - payloadsDst: buffer for payloads, must be at least
	//     KeysAdded() * (fingerprintSize + payloadSize) bytes
	//
	// Returns:
	//   - metadataLen: actual bytes written to metadataDst
	//   - payloadsLen: actual bytes written to payloadsDst
	//   - numKeys: number of keys in the block (same as KeysAdded())
	//   - error: non-nil if solving fails (extremely rare with good hash)
	//
	// After this call, the builder state is undefined. Call Reset() before
	// building the next block.
	BuildSeparatedInto(metadataDst, payloadsDst []byte) (int, int, int, error)
}

// blockDecoder handles query-time slot computation for MPHF lookups.
//
// Unlike blockBuilder, a decoder is stateless and safe for concurrent use.
// It is created once when opening an index and reused for all queries.
//
// The decoder reads algorithm-specific metadata encoded by the builder
// and computes the slot (rank) for a given key within a block.
type blockDecoder interface {
	// QuerySlot computes the local slot index for a key within a block.
	//
	// Parameters:
	//   - k0: first 8 bytes of the key as little-endian uint64
	//   - k1: second 8 bytes of the key as little-endian uint64
	//   - metadata: the block's encoded metadata from the index file
	//   - keysInBlock: number of keys in this block (from RAM index)
	//
	// Returns:
	//   - slot: local slot index in [0, keysInBlock), add to block's
	//     keysBefore to get global rank
	//   - error: ErrNotFound if key doesn't belong to this block,
	//     or other errors for corrupted metadata
	//
	// The slot is deterministic: the same key always returns the same slot.
	// However, different keys may return the same slot (MPHF property),
	// so fingerprint verification is needed to confirm membership.
	QuerySlot(k0, k1 uint64, metadata []byte, keysInBlock int) (int, error)
}

// newBlockBuilder creates a block builder for building MPHF indexes.
//
// Parameters:
//   - id: algorithm to use (AlgoBijection or AlgoPTRHash)
//   - totalKeys: total number of keys that will be indexed (for geometry)
//   - globalSeed: random seed for hash functions (stored in file header)
//   - payloadSize: bytes per payload (0-8), 0 for MPHF-only mode
//   - fingerprintSize: bytes per fingerprint (0-4), 0 to disable verification
//
// Returns an error if the algorithm ID is unknown.
func newBlockBuilder(id Algorithm, totalKeys uint64, globalSeed uint64,
	payloadSize, fingerprintSize int) (blockBuilder, error) {
	switch id {
	case AlgoBijection:
		return bijection.NewBuilder(totalKeys, globalSeed, payloadSize, fingerprintSize), nil
	case AlgoPTRHash:
		return ptrhash.NewBuilder(totalKeys, globalSeed, payloadSize, fingerprintSize), nil
	}
	return nil, fmt.Errorf("%w: unknown algorithm ID %d", sherr.ErrInvalidGeometry, id)
}

// newBlockDecoder creates a decoder for query-time slot computation.
//
// Parameters:
//   - id: algorithm used to build the index (from file header)
//   - globalConfig: algorithm-specific configuration bytes (from file header)
//   - globalSeed: random seed for hash functions (from file header)
//
// Returns an error if the algorithm ID is unknown or globalConfig is invalid.
// The returned decoder is safe for concurrent use.
func newBlockDecoder(id Algorithm, globalConfig []byte, globalSeed uint64) (blockDecoder, error) {
	switch id {
	case AlgoBijection:
		return bijection.NewDecoder(globalConfig, globalSeed)
	case AlgoPTRHash:
		return ptrhash.NewDecoder(globalConfig, globalSeed)
	}
	return nil, fmt.Errorf("%w: unknown algorithm ID %d", sherr.ErrInvalidGeometry, id)
}
