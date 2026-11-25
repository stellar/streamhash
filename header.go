package streamhash

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/cespare/xxhash/v2"
	"github.com/stellar/streamhash/internal/sherr"
)

const (
	// magic number for StreamHash index files (spec §3.2)
	// "STMH" in little-endian
	magic = uint32(0x53544D48)

	// version is the current format version
	version = uint16(0x0002)

	// headerSize is the exact size of the serialized header (64 bytes)
	headerSize = 64

	// footerSize is the exact size of the serialized footer (32 bytes)
	footerSize = 32

	// headerHashOffset is where the header's HeaderHash is.
	headerHashOffset = 56

	// ramIndexEntrySize is the size of each RAM index entry.
	// Format: [KeysBefore: 40 bits (5 bytes)][MetadataOffset: 40 bits (5 bytes)][Check: 4 bytes]
	// This supports up to ~1.1 trillion keys and ~1.1TB metadata region.
	ramIndexEntrySize = 14

	// ramIndexEntryDataSize is the entry's bytes before its check.
	ramIndexEntryDataSize = 10
)

// header is the 64-byte file header (spec §3.2).
//
// Layout:
//
//	Offset  Size  Field            Type
//	0       4     Magic            0x53544D48 ("STMH")
//	4       2     Version          0x0002
//	6       8     TotalKeys        uint64_le
//	14      4     NumBlocks        uint32_le
//	18      4     PayloadSize      uint32_le
//	22      1     FingerprintSize  uint8 (bytes)
//	23      8     Seed             uint64_le (globalSeed)
//	31      2     BlockAlgorithm   uint16_le (0=Bijection, 1=PTRHash)
//	33      23    Reserved         [23]byte (zero)
//	56      8     HeaderHash       uint64_le (xxHash64, see headerHash)
//
// Note: TotalBuckets, BucketsPerBlock, and PrefixBits are algorithm-internal.
// Each algorithm derives these from NumBlocks using its own constants.
// UserMetadata and AlgoConfig are stored in variable-length sections after header.
type header struct {
	Magic           uint32    // 4 bytes: magic number 0x53544D48
	Version         uint16    // 2 bytes: format version
	TotalKeys       uint64    // 8 bytes: total number of keys
	NumBlocks       uint32    // 4 bytes: number of blocks
	PayloadSize     uint32    // 4 bytes: payload bytes per key (0 = MPHF only)
	FingerprintSize uint8     // 1 byte: fingerprint bytes (0 = none)
	Seed            uint64    // 8 bytes: global seed for hashing
	BlockAlgorithm  Algorithm // 2 bytes: algorithm (0=Bijection, 1=PTRHash)
	Reserved        [23]byte  // 23 bytes: reserved (zero)
	HeaderHash      uint64    // 8 bytes: xxHash64 of the header and variable sections
}

// encodeTo serializes the header to an existing buffer.
func (h *header) encodeTo(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], h.Magic)
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	binary.LittleEndian.PutUint64(buf[6:14], h.TotalKeys)
	binary.LittleEndian.PutUint32(buf[14:18], h.NumBlocks)
	binary.LittleEndian.PutUint32(buf[18:22], h.PayloadSize)
	buf[22] = h.FingerprintSize
	binary.LittleEndian.PutUint64(buf[23:31], h.Seed)
	binary.LittleEndian.PutUint16(buf[31:33], uint16(h.BlockAlgorithm))
	copy(buf[33:56], h.Reserved[:])
	binary.LittleEndian.PutUint64(buf[56:64], h.HeaderHash)
}

// headerHash is the HeaderHash of an index whose bytes before the RAM index
// are prefix: xxHash64 of them, without the HeaderHash bytes.
func headerHash(prefix []byte) uint64 {
	d := xxhash.New()
	_, _ = d.Write(prefix[:headerHashOffset])
	_, _ = d.Write(prefix[headerSize:])
	return d.Sum64()
}

// decodeHeader parses a 64-byte header.
func decodeHeader(buf []byte) (*header, error) {
	if len(buf) < headerSize {
		return nil, sherr.ErrTruncatedFile
	}

	h := &header{
		Magic:           binary.LittleEndian.Uint32(buf[0:4]),
		Version:         binary.LittleEndian.Uint16(buf[4:6]),
		TotalKeys:       binary.LittleEndian.Uint64(buf[6:14]),
		NumBlocks:       binary.LittleEndian.Uint32(buf[14:18]),
		PayloadSize:     binary.LittleEndian.Uint32(buf[18:22]),
		FingerprintSize: buf[22],
		Seed:            binary.LittleEndian.Uint64(buf[23:31]),
		BlockAlgorithm:  Algorithm(binary.LittleEndian.Uint16(buf[31:33])),
	}
	copy(h.Reserved[:], buf[33:56])
	h.HeaderHash = binary.LittleEndian.Uint64(buf[56:64])

	if h.Magic != magic {
		return nil, sherr.ErrInvalidMagic
	}
	if h.Version != version {
		return nil, sherr.ErrInvalidVersion
	}
	if h.PayloadSize > uint32(maxPayloadSize) {
		return nil, sherr.ErrCorruptedIndex
	}
	if h.FingerprintSize > uint8(maxFingerprintSize) {
		return nil, sherr.ErrCorruptedIndex
	}
	if h.NumBlocks == 0 && h.TotalKeys > 0 {
		return nil, sherr.ErrCorruptedIndex
	}
	// Reject the wrap-around value: initFromData computes numRAMEntries as
	// NumBlocks+1 in uint32, so NumBlocks==0xFFFFFFFF would wrap to 0 and slip
	// past the RAM-index span check, leaving an empty index that later panics
	// (e.g. in Verify). No valid index has this many blocks.
	if h.NumBlocks == ^uint32(0) {
		return nil, sherr.ErrCorruptedIndex
	}

	return h, nil
}

// payloadSizeInt returns PayloadSize as int for arithmetic convenience.
func (h *header) payloadSizeInt() int {
	return int(h.PayloadSize)
}

// hasFingerprint returns true if the index stores fingerprints.
func (h *header) hasFingerprint() bool {
	return h.FingerprintSize > 0
}

// fingerprintSizeInt returns FingerprintSize as int for arithmetic convenience.
func (h *header) fingerprintSizeInt() int {
	return int(h.FingerprintSize)
}

// entrySize returns bytes stored per key (fingerprint + payload).
func (h *header) entrySize() int {
	return int(h.PayloadSize) + int(h.FingerprintSize)
}

// footer is the 32-byte file footer (spec §3.7).
//
// Layout (Separated Layout v3):
//
//	Offset  Size  Field               Type
//	0       8     PayloadRegionHash   uint64_le (xxHash64 of payload region)
//	8       8     MetadataRegionHash  uint64_le (xxHash64 of metadata region)
//	16      16    Reserved            [16]byte (zero)
type footer struct {
	PayloadRegionHash  uint64   // 8 bytes: xxHash64 of entire payload region
	MetadataRegionHash uint64   // 8 bytes: xxHash64 of entire metadata region
	Reserved           [16]byte // 16 bytes: reserved for future use
}

// encodeTo serializes the footer into an existing buffer.
func (f *footer) encodeTo(buf []byte) {
	binary.LittleEndian.PutUint64(buf[0:8], f.PayloadRegionHash)
	binary.LittleEndian.PutUint64(buf[8:16], f.MetadataRegionHash)
	copy(buf[16:32], f.Reserved[:])
}

// decodeFooter parses a 32-byte footer.
func decodeFooter(buf []byte) (*footer, error) {
	if len(buf) < footerSize {
		return nil, sherr.ErrTruncatedFile
	}

	f := &footer{
		PayloadRegionHash:  binary.LittleEndian.Uint64(buf[0:8]),
		MetadataRegionHash: binary.LittleEndian.Uint64(buf[8:16]),
	}
	copy(f.Reserved[:], buf[16:32])

	return f, nil
}

// ramIndexEntry is a single entry in the RAM index.
// Each entry is 14 bytes (Compact Layout with uint40+uint40, and a check).
//
// Wire format (14 bytes packed, little-endian):
//
//	Offset  Size  Field           Type
//	0       5     KeysBefore      uint40_le (cumulative count, max ~1.1 trillion)
//	5       5     MetadataOffset  uint40_le (relative offset, max ~1.1 TB)
//	10      4     Check           uint32_le (see ramIndexEntryCheck)
//
// Note: Payloads are at fixed offsets (globalRank × entrySize), so no BlockOffset needed.
// MetadataOffset is relative to the start of the metadata region.
type ramIndexEntry struct {
	KeysBefore     uint64 // Cumulative keys before this block (stored as 40-bit)
	MetadataOffset uint64 // Relative offset within metadata region (stored as 40-bit)
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ramIndexEntryCheck is the check of block's entry, whose first
// ramIndexEntryDataSize bytes are data: CRC-32C of those bytes, continued
// from the block index as the running CRC. The index makes an entry read at
// another block's position fail too. Passing it as the running CRC, not as
// bytes, keeps a buffer off the heap on every lookup.
func ramIndexEntryCheck(block uint32, data []byte) uint32 {
	return crc32.Update(block, castagnoli, data[:ramIndexEntryDataSize])
}

// encodeRAMIndexEntryTo serializes block's RAM index entry, with its check,
// into an existing buffer. Uses little-endian 40-bit encoding for both fields.
func encodeRAMIndexEntryTo(e ramIndexEntry, block uint32, buf []byte) {
	// Little-endian 40-bit KeysBefore (bytes 0-4)
	buf[0] = byte(e.KeysBefore)
	buf[1] = byte(e.KeysBefore >> 8)
	buf[2] = byte(e.KeysBefore >> 16)
	buf[3] = byte(e.KeysBefore >> 24)
	buf[4] = byte(e.KeysBefore >> 32)
	// Little-endian 40-bit MetadataOffset (bytes 5-9)
	buf[5] = byte(e.MetadataOffset)
	buf[6] = byte(e.MetadataOffset >> 8)
	buf[7] = byte(e.MetadataOffset >> 16)
	buf[8] = byte(e.MetadataOffset >> 24)
	buf[9] = byte(e.MetadataOffset >> 32)
	binary.LittleEndian.PutUint32(buf[10:14], ramIndexEntryCheck(block, buf))
}

// decodeRAMIndexEntry parses a RAM index entry without verifying its check.
// Uses little-endian 40-bit decoding for both fields.
func decodeRAMIndexEntry(buf []byte) ramIndexEntry {
	return ramIndexEntry{
		KeysBefore: uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 |
			uint64(buf[3])<<24 | uint64(buf[4])<<32,
		MetadataOffset: uint64(buf[5]) | uint64(buf[6])<<8 | uint64(buf[7])<<16 |
			uint64(buf[8])<<24 | uint64(buf[9])<<32,
	}
}
