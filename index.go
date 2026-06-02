package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/edsrzf/mmap-go"
	intbits "github.com/tamirms/streamhash/internal/bits"
	"github.com/tamirms/streamhash/internal/sherr"
)

const (
	// minFileSize is a conservative lower bound for valid index files.
	// Derived from: headerSize(64) + variableSections(8 min) + ramIndex(30 min for 2 blocks + sentinel)
	// + footerSize(32) = 134 bytes minimum structure. 304 adds margin for metadata region.
	minFileSize = 304
)

// Index is a read-only StreamHash index for querying.
//
// Thread Safety:
// - QueryRank, PayloadIndex.QueryPayload, and other read methods are safe for concurrent use
// - Close is NOT safe to call concurrently with queries
// - Close must only be called after all queries have completed
// - After Close returns, no methods may be called on the Index
type Index struct {
	// Memory map (no file handle needed after mmap)
	mmap mmap.MMap
	data []byte

	// Parsed header
	header *header

	// Variable-length data from file
	userMetadata []byte

	// RAM index — entries are decoded on demand from this mmap-backed
	// byte view via ramEntry(). Keeping it as []byte avoids the
	// per-Open allocation + decode loop a Go-heap []ramIndexEntry
	// would require, which scales linearly with NumBlocks.
	ramIndexBytes []byte
	numRAMEntries uint32

	// Separated layout region offsets (computed from header)
	payloadRegionOffset  uint64
	metadataRegionOffset uint64

	// Computed values
	entrySize int // FingerprintSize + PayloadSize

	// Algorithm decoder (created at Open time, reused for all queries)
	decoder blockDecoder

	closed atomic.Bool // Atomic for lock-free close check
}

// Stats holds index statistics.
type Stats struct {
	NumKeys         uint64
	NumBlocks       uint32
	BitsPerKey      float64
	PayloadSize     int
	FingerprintSize int // bytes per key (0 if disabled)
	FileSize        int64
	OverheadBPK     float64 // MPHF overhead bits per key (excludes payload + fingerprint)
	Algorithm       Algorithm
}

// Open opens a StreamHash index file for querying.
// It opens the file, memory-maps it, and closes the file descriptor.
func Open(path string) (*Index, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open index file: %w", err)
	}
	defer file.Close()
	return OpenFile(file)
}

// OpenFile opens a StreamHash index by memory-mapping the given file.
// The caller is responsible for closing f. Per POSIX mmap(2), f may be
// closed immediately after OpenFile returns.
func OpenFile(f *os.File) (*Index, error) {
	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat index file: %w", err)
	}
	fileSize := stat.Size()

	if fileSize < int64(minFileSize) {
		return nil, sherr.ErrTruncatedFile
	}

	mm, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("mmap index file: %w", err)
	}

	idx := &Index{
		mmap: mm,
		data: []byte(mm),
	}
	if err := idx.initFromData(); err != nil {
		return nil, errors.Join(err, idx.Close())
	}
	return idx, nil
}

// OpenBytes creates a StreamHash index from an in-memory byte slice.
// No file is opened or memory-mapped; Close is a no-op.
// The caller must ensure data is not modified while the Index is in use.
func OpenBytes(data []byte) (*Index, error) {
	if len(data) < minFileSize {
		return nil, sherr.ErrTruncatedFile
	}
	idx := &Index{
		data: data,
	}
	if err := idx.initFromData(); err != nil {
		return nil, err
	}
	return idx, nil
}

// initFromData parses the header and locates the variable-length
// sections and RAM index inside idx.data. Open() only touches the
// contiguous prefix (header + variable sections + RAM-index span);
// individual RAM index entries are decoded on demand via ramEntry(),
// and footer decoding is deferred to Verify().
func (idx *Index) initFromData() error {
	fileSize := uint64(len(idx.data))

	// Parse header
	hdr, err := decodeHeader(idx.data[:headerSize])
	if err != nil {
		return err
	}
	idx.header = hdr

	// Read variable-length sections after header
	// Layout: [Header 64B][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index]...
	offset := uint64(headerSize)

	// Read userMetadata
	if offset+4 > fileSize {
		return sherr.ErrTruncatedFile
	}
	userMetadataLen := binary.LittleEndian.Uint32(idx.data[offset:])
	offset += 4
	if offset+uint64(userMetadataLen) > fileSize {
		return sherr.ErrTruncatedFile
	}
	idx.userMetadata = idx.data[offset : offset+uint64(userMetadataLen)]
	offset += uint64(userMetadataLen)

	// Read algoConfig
	if offset+4 > fileSize {
		return sherr.ErrTruncatedFile
	}
	algoConfigLen := binary.LittleEndian.Uint32(idx.data[offset:])
	offset += 4
	if offset+uint64(algoConfigLen) > fileSize {
		return sherr.ErrTruncatedFile
	}
	algoConfig := idx.data[offset : offset+uint64(algoConfigLen)]
	offset += uint64(algoConfigLen)

	// Calculate RAM index position (after variable sections)
	idx.numRAMEntries = hdr.NumBlocks + 1 // includes sentinel
	ramIndexStart := offset
	ramIndexSize := uint64(idx.numRAMEntries) * uint64(ramIndexEntrySize)
	ramIndexEnd := ramIndexStart + ramIndexSize

	// fileSize >= minFileSize > footerSize, so no underflow.
	if ramIndexEnd > fileSize-uint64(footerSize) {
		return sherr.ErrCorruptedIndex
	}

	// RAM index — keep the mmap'd region as a byte view; ramEntry()
	// decodes individual entries on demand. See the field doc on Index.
	idx.ramIndexBytes = idx.data[ramIndexStart:ramIndexEnd]

	idx.entrySize = hdr.entrySize()

	// Compute separated layout region offsets
	idx.payloadRegionOffset = ramIndexEnd
	payloadRegionSize := hdr.TotalKeys * uint64(idx.entrySize)
	idx.metadataRegionOffset = idx.payloadRegionOffset + payloadRegionSize

	// Create algorithm decoder for query-time slot computation
	idx.decoder, err = newBlockDecoder(hdr.BlockAlgorithm, algoConfig, hdr.Seed)
	if err != nil {
		return err
	}

	return nil
}

// ramEntry decodes RAM index entry i from the mmap'd byte view.
// decodeRAMIndexEntry reads exactly ramIndexEntrySize bytes from the
// start of the slice and does a per-byte little-endian unpack, so the
// open-ended slice expression is safe and incurs only one bounds
// check (avoiding the 3-index form keeps this method small enough to
// inline alongside the inlined decoder).
func (idx *Index) ramEntry(i uint32) ramIndexEntry {
	return decodeRAMIndexEntry(idx.ramIndexBytes[uint64(i)*ramIndexEntrySize:])
}

// Close closes the index and releases resources.
func (idx *Index) Close() error {
	if idx.closed.Swap(true) {
		return nil // Already closed
	}

	if idx.mmap != nil {
		return idx.mmap.Unmap()
	}
	return nil
}

// QueryRank returns the rank (0-based index) for a key.
// This is the core MPHF operation.
// Returns ErrKeyTooShort if key is less than 16 bytes.
func (idx *Index) QueryRank(key []byte) (uint64, error) {
	if idx.closed.Load() {
		return 0, sherr.ErrIndexClosed
	}

	// Validate key length (must be at least 16 bytes for routing)
	if len(key) < MinKeySize {
		return 0, sherr.ErrKeyTooShort
	}

	return idx.queryInternal(key)
}

// verifyFingerprintSeparated checks if the fingerprint matches.
// Reads from the separated payload region.
// Always uses extractFingerprint(k0, k1, fpSize) for both build and query.
// Precondition: idx.header.hasFingerprint() (callers must check).
func (idx *Index) verifyFingerprintSeparated(k0, k1 uint64, globalRank uint64) (bool, error) {
	fpSize := idx.header.fingerprintSizeInt()

	// Get stored fingerprint from payload region
	payloadOffset := idx.payloadRegionOffset + globalRank*uint64(idx.entrySize)

	if payloadOffset+uint64(fpSize) > uint64(len(idx.data)) {
		return false, sherr.ErrCorruptedIndex
	}

	storedFP := unpackFingerprintFromBytes(idx.data[payloadOffset:], fpSize)
	expectedFP := extractFingerprint(k0, k1, fpSize)

	return storedFP == expectedFP, nil
}

// queryInternal is the internal query implementation.
// Uses the algorithm-specific decoder to compute slots.
func (idx *Index) queryInternal(key []byte) (uint64, error) {
	// Step 1: Parse key and route to block
	// k0, k1 are little-endian for hash computation
	// prefix is big-endian for monotonic block routing
	k0 := binary.LittleEndian.Uint64(key[0:8])
	k1 := binary.LittleEndian.Uint64(key[8:16])
	prefix := bits.ReverseBytes64(k0) // Big-endian for monotonic routing
	blockIdx := intbits.FastRange32(prefix, idx.header.NumBlocks)

	if blockIdx+1 >= idx.numRAMEntries {
		return 0, sherr.ErrNotFound
	}

	// Step 2: Get block info from RAM index (on-demand decode from
	// mmap'd bytes — see ramEntry).
	entry := idx.ramEntry(blockIdx)
	nextEntry := idx.ramEntry(blockIdx + 1)
	baseRank := entry.KeysBefore

	// KeysBefore is read raw from the (untrusted) RAM index and must be
	// monotonic. Reject a non-monotonic pair before the subtraction: otherwise
	// the uint64 underflow yields a huge value that int() turns negative,
	// driving the decoder to a slice-bounds panic / wrong rank on corrupt input.
	if nextEntry.KeysBefore < entry.KeysBefore {
		return 0, sherr.ErrCorruptedIndex
	}
	keysInBlock := int(nextEntry.KeysBefore - entry.KeysBefore)

	if keysInBlock == 0 {
		return 0, sherr.ErrNotFound
	}

	// Step 3: Get metadata for block
	metaStart := idx.metadataRegionOffset + entry.MetadataOffset
	metaEnd := idx.metadataRegionOffset + nextEntry.MetadataOffset

	if metaStart >= metaEnd || metaEnd > uint64(len(idx.data)) {
		return 0, sherr.ErrCorruptedIndex
	}

	metadataData := idx.data[metaStart:metaEnd]

	// Step 4: Use decoder to compute local slot
	localSlot, err := idx.decoder.QuerySlot(k0, k1, metadataData, keysInBlock)
	if err != nil {
		return 0, err
	}

	// Step 5: Compute global rank and verify fingerprint
	globalRank := baseRank + uint64(localSlot)

	if idx.header.hasFingerprint() {
		ok, err := idx.verifyFingerprintSeparated(k0, k1, globalRank)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, sherr.ErrNotFound
		}
	}

	return globalRank, nil
}

// PayloadIndex extends Index with payload query capability.
// Obtained via OpenPayload, OpenPayloadFile, OpenPayloadBytes,
// or Index.WithPayload().
type PayloadIndex struct {
	*Index
}

// OpenPayload opens a StreamHash index that has payload data.
// Returns an error if the index was built without WithPayload.
func OpenPayload(path string) (*PayloadIndex, error) {
	idx, err := Open(path)
	if err != nil {
		return nil, err
	}
	pi, err := idx.WithPayload()
	if err != nil {
		_ = idx.Close() // best-effort cleanup; surface the original error
		return nil, err
	}
	return pi, nil
}

// OpenPayloadFile opens a payload-capable index from an open file.
// Returns an error if the index was built without WithPayload.
func OpenPayloadFile(f *os.File) (*PayloadIndex, error) {
	idx, err := OpenFile(f)
	if err != nil {
		return nil, err
	}
	pi, err := idx.WithPayload()
	if err != nil {
		_ = idx.Close() // best-effort cleanup; surface the original error
		return nil, err
	}
	return pi, nil
}

// OpenPayloadBytes creates a payload-capable index from an in-memory byte slice.
// Returns an error if the index was built without WithPayload.
func OpenPayloadBytes(data []byte) (*PayloadIndex, error) {
	idx, err := OpenBytes(data)
	if err != nil {
		return nil, err
	}
	pi, err := idx.WithPayload()
	if err != nil {
		_ = idx.Close() // best-effort cleanup; surface the original error
		return nil, err
	}
	return pi, nil
}

// WithPayload returns a PayloadIndex if the index has payload data.
// Returns an error if the index was built without WithPayload.
func (idx *Index) WithPayload() (*PayloadIndex, error) {
	if idx.header.PayloadSize == 0 {
		return nil, sherr.ErrNoPayload
	}
	return &PayloadIndex{Index: idx}, nil
}

// QueryPayload returns the rank and payload for a key.
// Returns ErrKeyTooShort if key is less than 16 bytes.
func (pi *PayloadIndex) QueryPayload(key []byte) (rank uint64, payload uint64, err error) {
	if pi.closed.Load() {
		return 0, 0, sherr.ErrIndexClosed
	}

	if len(key) < MinKeySize {
		return 0, 0, sherr.ErrKeyTooShort
	}

	rank, err = pi.queryInternal(key)
	if err != nil {
		return 0, 0, err
	}

	// Read payload from payload region
	fpSize := pi.header.fingerprintSizeInt()
	payloadSize := pi.header.payloadSizeInt()

	payloadOffset := pi.payloadRegionOffset + rank*uint64(pi.entrySize) + uint64(fpSize)

	if payloadOffset+uint64(payloadSize) > uint64(len(pi.data)) {
		return 0, 0, sherr.ErrCorruptedIndex
	}

	payload = unpackPayloadFromBytes(pi.data[payloadOffset:], payloadSize)
	return rank, payload, nil
}

// NumKeys returns the total number of keys in the index.
func (idx *Index) NumKeys() uint64 {
	return idx.header.TotalKeys
}

// NumBlocks returns the number of blocks in the index.
func (idx *Index) NumBlocks() uint32 {
	return idx.header.NumBlocks
}

// PayloadSize returns the payload size per key.
func (idx *Index) PayloadSize() int {
	return idx.header.payloadSizeInt()
}

// UserMetadata returns the variable-length user-defined metadata.
// The returned slice is backed by the memory-mapped file data.
func (idx *Index) UserMetadata() []byte {
	return idx.userMetadata
}

// GetStats returns statistics for an index file.
func GetStats(path string) (*Stats, error) {
	idx, err := Open(path)
	if err != nil {
		return nil, err
	}
	stats := idx.Stats()
	if err := idx.Close(); err != nil {
		return nil, err
	}
	return stats, nil
}

// Stats returns statistics for the index.
func (idx *Index) Stats() *Stats {
	totalSize := int64(len(idx.data))

	bitsPerKey := float64(0)
	if idx.header.TotalKeys > 0 {
		bitsPerKey = float64(totalSize*8) / float64(idx.header.TotalKeys)
	}

	payloadBits := float64(idx.header.payloadSizeInt() * 8)
	fpBits := float64(idx.header.fingerprintSizeInt() * 8)

	return &Stats{
		NumKeys:         idx.header.TotalKeys,
		NumBlocks:       idx.header.NumBlocks,
		BitsPerKey:      bitsPerKey,
		PayloadSize:     idx.header.payloadSizeInt(),
		FingerprintSize: idx.header.fingerprintSizeInt(),
		FileSize:        totalSize,
		OverheadBPK:     bitsPerKey - payloadBits - fpBits,
		Algorithm:       idx.header.BlockAlgorithm,
	}
}

// Verify checks the integrity of the entire index.
// For separated layout, this verifies:
// 1. PayloadRegionHash (hash-of-hashes: H(H(b0) || H(b1) || ...))
// 2. MetadataRegionHash (streaming hash of metadata region)
//
// The footer (last 32 bytes) is decoded on each Verify call rather than at Open time,
// so Open() only touches the contiguous prefix and avoids a scattered page fault.
//
// The hash-of-hashes approach matches the streaming hash computation during build,
// where workers compute per-block payload hashes that are folded in order.
func (idx *Index) Verify() error {
	if idx.closed.Load() {
		return sherr.ErrIndexClosed
	}

	// Lazy footer decode — only touched by Verify, not Open.
	fileSize := uint64(len(idx.data))
	ft, err := decodeFooter(idx.data[fileSize-footerSize:])
	if err != nil {
		return err
	}

	numBlocks := idx.header.NumBlocks

	// Verify payload region hash using hash-of-hashes
	// This matches the build-time computation: fold per-block hashes in order
	payloadHasher := xxhash.New()
	for blockID := range numBlocks {
		// Get this block's payload range from RAM index
		startKey := idx.ramEntry(blockID).KeysBefore
		endKey := idx.ramEntry(blockID + 1).KeysBefore
		if startKey > endKey {
			return sherr.ErrCorruptedIndex
		}
		numKeys := endKey - startKey

		// Compute hash of this block's payloads
		var blockHash uint64
		if numKeys > 0 && idx.entrySize > 0 {
			payloadStart := idx.payloadRegionOffset + startKey*uint64(idx.entrySize)
			payloadEnd := idx.payloadRegionOffset + endKey*uint64(idx.entrySize)
			if payloadEnd > uint64(len(idx.data)) {
				return sherr.ErrCorruptedIndex
			}
			blockPayloads := idx.data[payloadStart:payloadEnd]
			blockHash = xxhash.Sum64(blockPayloads)
		} else {
			// Empty block or MPHF-only: hash of empty slice
			blockHash = xxhash.Sum64(nil)
		}

		// Fold block hash into streaming hasher (little-endian)
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], blockHash)
		if _, err := payloadHasher.Write(buf[:]); err != nil {
			panic("hash.Hash.Write returned unexpected error: " + err.Error())
		}
	}

	if payloadHasher.Sum64() != ft.PayloadRegionHash {
		return sherr.ErrChecksumFailed
	}

	// Verify metadata region hash (streaming hash of entire region)
	footerOffset := fileSize - footerSize
	if footerOffset < idx.metadataRegionOffset {
		return sherr.ErrCorruptedIndex
	}
	metadataRegionSize := footerOffset - idx.metadataRegionOffset
	if metadataRegionSize > 0 {
		metadataRegion := idx.data[idx.metadataRegionOffset:footerOffset]
		actualMetaHash := xxhash.Sum64(metadataRegion)
		if actualMetaHash != ft.MetadataRegionHash {
			return sherr.ErrChecksumFailed
		}
	}

	return nil
}
