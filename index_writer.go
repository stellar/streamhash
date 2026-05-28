package streamhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sys/unix"
)

// indexWriter handles writing index data to disk using pwrite-based writes.
// File layout: [Header 64B][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index (N+1)×10B][Payload Region][Metadata Region][Footer 32B]
type indexWriter struct {
	file *os.File
	fd   int // cached file descriptor for pwrite syscalls

	// Region offsets (computed upfront for separated layout)
	payloadRegionOffset  uint64
	metadataRegionOffset uint64

	// Variable section offsets
	userMetadataOffset uint64
	algoConfigOffset   uint64
	ramIndexOffset     uint64

	// Write tracking
	metadataWriteOffset uint64 // Current write position in metadata region

	// RAM index (built incrementally)
	ramIndex []ramIndexEntry

	// Streaming hashers for incremental hash computation
	// These hash data while it's hot in CPU cache during writes
	payloadHasher  *xxhash.Digest // Hash-of-hashes: folds per-block payload hashes in order
	metadataHasher *xxhash.Digest // Streaming hash of metadata as it's written

	// Config
	header        header
	cfg           *buildConfig
	numBlocks     uint32
	estimatedSize uint64 // Pre-allocated file size
	algo          blockBuilder
	userMetadata  []byte

	// State
	keyCount   uint64
	blockCount uint32
}

// newIndexWriter creates a new pwrite-based index writer using separated layout.
// The file is pre-allocated for space reservation.
func newIndexWriter(path string, cfg *buildConfig, numBlocks uint32, algo blockBuilder) (*indexWriter, error) {
	// Compute variable section sizes
	userMetadataLen := len(cfg.userMetadata)
	algoConfigLen := algo.GlobalConfigSize()

	// Estimate total file size for pre-allocation
	estimatedSize := estimateFileSizeForAlgo(cfg, numBlocks, algo, userMetadataLen, algoConfigLen)

	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create index file: %w", err)
	}

	entrySize := cfg.payloadSize + cfg.fingerprintSize

	// Compute region offsets for separated layout with variable sections
	// Layout: [Header][UserMetaLen 4B][UserMeta][AlgoConfigLen 4B][AlgoConfig][RAM Index]...
	userMetadataOffset := uint64(headerSize)
	algoConfigOffset := userMetadataOffset + 4 + uint64(userMetadataLen)
	ramIndexOffset := algoConfigOffset + 4 + uint64(algoConfigLen)
	ramIndexSize := uint64(numBlocks+1) * uint64(ramIndexEntrySize)
	payloadRegionOffset := ramIndexOffset + ramIndexSize
	payloadRegionSize := cfg.totalKeys * uint64(entrySize)
	metadataRegionOffset := payloadRegionOffset + payloadRegionSize

	// Pre-allocate disk blocks for space reservation
	if err := preallocFile(int(file.Fd()), int64(estimatedSize)); err != nil {
		primaryErr := fmt.Errorf("failed to allocate disk space: %w", err)
		return nil, errors.Join(primaryErr, file.Close())
	}

	iw := &indexWriter{
		file:                 file,
		fd:                   int(file.Fd()),
		payloadRegionOffset:  payloadRegionOffset,
		metadataRegionOffset: metadataRegionOffset,
		userMetadataOffset:   userMetadataOffset,
		algoConfigOffset:     algoConfigOffset,
		ramIndexOffset:       ramIndexOffset,
		metadataWriteOffset:  metadataRegionOffset,
		ramIndex:             make([]ramIndexEntry, 0, numBlocks+1),
		payloadHasher:        xxhash.New(),
		metadataHasher:       xxhash.New(),
		cfg:                  cfg,
		numBlocks:            numBlocks,
		estimatedSize:        estimatedSize,
		algo:                 algo,
		userMetadata:         cfg.userMetadata,
	}

	// Initialize header (spec §5.2)
	iw.header = header{
		Magic:           magic,
		Version:         version,
		TotalKeys:       cfg.totalKeys,
		NumBlocks:       numBlocks,
		PayloadSize:     uint32(cfg.payloadSize),
		FingerprintSize: uint8(cfg.fingerprintSize),
		Seed:            cfg.globalSeed,
		BlockAlgorithm:  cfg.algorithm,
	}

	return iw, nil
}

// writePayloadsDirect writes payload data to the payload region via pwrite.
// offset is relative to the start of the payload region.
// This is thread-safe for non-overlapping regions, enabling parallel payload writes.
func (iw *indexWriter) writePayloadsDirect(data []byte, offset uint64) error {
	if len(data) == 0 {
		return nil
	}

	absoluteOffset := iw.payloadRegionOffset + offset
	// Defensive bounds check to prevent overwriting metadata region
	if absoluteOffset+uint64(len(data)) > iw.metadataRegionOffset {
		return fmt.Errorf("writePayloadsDirect: write exceeds payload region boundary")
	}
	_, err := unix.Pwrite(iw.fd, data, int64(absoluteOffset))
	return err
}

// foldPayloadHash folds a per-block payload hash into the streaming payload hasher.
// This must be called in block order (block 0, 1, 2, ...) to produce deterministic results.
// Workers compute xxhash.Sum64(payloads) for their block, then the writer goroutine
// calls this method in order to fold those hashes into the final payload region hash.
func (iw *indexWriter) foldPayloadHash(blockPayloadHash uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], blockPayloadHash)
	if _, err := iw.payloadHasher.Write(buf[:]); err != nil {
		panic("hash.Hash.Write returned unexpected error: " + err.Error())
	}
}

// recordAndWriteMetadata appends the RAM index entry for the block at the
// current write position and writes its metadata to the metadata region,
// updating the streaming metadata hash. It does not advance keyCount/blockCount;
// callers do that after any payload write so the entry and payload offset both
// see the pre-block keyCount.
func (iw *indexWriter) recordAndWriteMetadata(metadata []byte) error {
	metadataRelOffset := iw.metadataWriteOffset - iw.metadataRegionOffset
	iw.ramIndex = append(iw.ramIndex, ramIndexEntry{
		KeysBefore:     iw.keyCount,
		MetadataOffset: metadataRelOffset,
	})

	if len(metadata) > 0 {
		if _, err := unix.Pwrite(iw.fd, metadata, int64(iw.metadataWriteOffset)); err != nil {
			return err
		}
		if _, err := iw.metadataHasher.Write(metadata); err != nil {
			panic("hash.Hash.Write returned unexpected error: " + err.Error())
		}
		iw.metadataWriteOffset += uint64(len(metadata))
	}
	return nil
}

// writeMetadata writes metadata for a block to the metadata region via pwrite.
// Blocks must be written in order (blockID 0, 1, 2, ...).
// This is used with writePayloadsDirect for parallel builds where payloads
// are written directly by workers and only metadata goes through the queue.
func (iw *indexWriter) writeMetadata(metadata []byte, numKeys int) error {
	if err := iw.recordAndWriteMetadata(metadata); err != nil {
		return err
	}

	iw.keyCount += uint64(numKeys)
	iw.blockCount++
	return nil
}

// commitBlockWithData writes a block's payloads and metadata via pwrite,
// computing and folding hashes from the provided buffers.
// This is used by single-threaded builds where the caller provides data in local buffers.
func (iw *indexWriter) commitBlockWithData(metadata []byte, payloads []byte, numKeys int) error {
	if err := iw.recordAndWriteMetadata(metadata); err != nil {
		return err
	}

	// Write and hash payloads
	entrySize := iw.cfg.payloadSize + iw.cfg.fingerprintSize
	if entrySize > 0 && numKeys > 0 {
		payloadOffset := iw.payloadRegionOffset + iw.keyCount*uint64(entrySize)
		if _, err := unix.Pwrite(iw.fd, payloads, int64(payloadOffset)); err != nil {
			return err
		}
		iw.foldPayloadHash(xxhash.Sum64(payloads))
	} else {
		iw.foldPayloadHash(xxhash.Sum64(nil))
	}

	iw.keyCount += uint64(numKeys)
	iw.blockCount++
	return nil
}

// finalize completes the index file by writing header, variable sections,
// RAM index, and footer via pwrite.
// On error, delegates to close() for idempotent cleanup.
// On success, nils file so that close() is a safe no-op.
func (iw *indexWriter) finalize() error {
	// Validate we didn't exceed estimated size
	actualSize := iw.metadataWriteOffset + footerSize
	if actualSize > iw.estimatedSize {
		primaryErr := fmt.Errorf("actual size %d exceeds estimated size %d", actualSize, iw.estimatedSize)
		return errors.Join(primaryErr, iw.close())
	}

	// Add sentinel entry for RAM index (spec §5.3)
	metadataRelOffset := iw.metadataWriteOffset - iw.metadataRegionOffset
	iw.ramIndex = append(iw.ramIndex, ramIndexEntry{
		KeysBefore:     iw.keyCount,
		MetadataOffset: metadataRelOffset,
	})

	// Get final hashes from streaming hashers (no re-read of regions needed!)
	payloadRegionHash := iw.payloadHasher.Sum64()
	metadataRegionHash := iw.metadataHasher.Sum64()

	// Update header
	iw.header.TotalKeys = iw.keyCount
	iw.header.NumBlocks = iw.blockCount

	// Write header at offset 0
	var headerBuf [headerSize]byte
	iw.header.encodeTo(headerBuf[:])
	if _, err := unix.Pwrite(iw.fd, headerBuf[:], 0); err != nil {
		primaryErr := fmt.Errorf("write header failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// Write variable-length sections
	// UserMetadata: [length 4B][data]
	userMetaBuf := make([]byte, 4+len(iw.userMetadata))
	binary.LittleEndian.PutUint32(userMetaBuf, uint32(len(iw.userMetadata)))
	copy(userMetaBuf[4:], iw.userMetadata)
	if _, err := unix.Pwrite(iw.fd, userMetaBuf, int64(iw.userMetadataOffset)); err != nil {
		primaryErr := fmt.Errorf("write user metadata failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// AlgoConfig: [length 4B][data]
	algoConfigLen := iw.algo.GlobalConfigSize()
	algoConfigBuf := make([]byte, 4+algoConfigLen)
	binary.LittleEndian.PutUint32(algoConfigBuf, uint32(algoConfigLen))
	if algoConfigLen > 0 {
		iw.algo.EncodeGlobalConfigInto(algoConfigBuf[4:])
	}
	if _, err := unix.Pwrite(iw.fd, algoConfigBuf, int64(iw.algoConfigOffset)); err != nil {
		primaryErr := fmt.Errorf("write algo config failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// Write RAM index entries
	ramIndexBuf := make([]byte, len(iw.ramIndex)*int(ramIndexEntrySize))
	for i, entry := range iw.ramIndex {
		offset := i * int(ramIndexEntrySize)
		encodeRAMIndexEntryTo(entry, ramIndexBuf[offset:])
	}
	if _, err := unix.Pwrite(iw.fd, ramIndexBuf, int64(iw.ramIndexOffset)); err != nil {
		primaryErr := fmt.Errorf("write RAM index failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// Write footer
	ftr := footer{
		PayloadRegionHash:  payloadRegionHash,
		MetadataRegionHash: metadataRegionHash,
	}
	var footerBuf [footerSize]byte
	ftr.encodeTo(footerBuf[:])
	if _, err := unix.Pwrite(iw.fd, footerBuf[:], int64(iw.metadataWriteOffset)); err != nil {
		primaryErr := fmt.Errorf("write footer failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// Shrink to actual size
	if err := iw.file.Truncate(int64(actualSize)); err != nil {
		primaryErr := fmt.Errorf("truncate failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	// Flush data to disk for durability
	if err := iw.file.Sync(); err != nil {
		primaryErr := fmt.Errorf("fsync failed: %w", err)
		return errors.Join(primaryErr, iw.close())
	}

	closeErr := iw.file.Close()
	iw.file = nil
	return closeErr
}

// close closes the writer without finalizing (for error cleanup).
// Idempotent: safe to call multiple times.
func (iw *indexWriter) close() error {
	if iw.file != nil {
		closeErr := iw.file.Close()
		iw.file = nil
		return closeErr
	}
	return nil
}

// estimateFileSizeForAlgo estimates file size based on algorithm.
func estimateFileSizeForAlgo(cfg *buildConfig, numBlocks uint32, algo blockBuilder, userMetadataLen, algoConfigLen int) uint64 {
	// Variable section sizes
	variableSectionSize := uint64(4 + userMetadataLen + 4 + algoConfigLen)
	ramIndexSize := uint64(numBlocks+1) * ramIndexEntrySize
	entrySize := cfg.payloadSize + cfg.fingerprintSize
	payloadRegionSize := cfg.totalKeys * uint64(entrySize)

	// Estimate metadata size using algorithm's worst-case max.
	// Add 16-byte safety margin per block for alignment/padding variance.
	metadataPerBlock := uint64(algo.MaxIndexMetadataSize()) + 16
	estimatedMetadataSize := uint64(numBlocks) * metadataPerBlock
	return headerSize + variableSectionSize + ramIndexSize + payloadRegionSize + estimatedMetadataSize + footerSize
}
