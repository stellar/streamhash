// Package sherr defines error sentinels shared across the streamhash library.
// User-facing sentinels are re-exported by the root streamhash package.
package sherr

import "errors"

// Build errors
var (
	ErrBuilderClosed    = errors.New("streamhash: builder is closed")
	ErrEmptyIndex       = errors.New("streamhash: cannot build index with zero keys")
	ErrTooManyKeys      = errors.New("streamhash: key count exceeds maximum (2^40 - 1)")
	ErrKeyTooShort      = errors.New("streamhash: key is shorter than minimum required length")
	ErrKeyTooLong       = errors.New("streamhash: key exceeds maximum length (65535 bytes)")
	ErrPayloadOverflow  = errors.New("streamhash: payload value exceeds configured PayloadSize capacity")
	ErrDuplicateKey     = errors.New("streamhash: duplicate key detected")
	ErrUnsortedInput    = errors.New("streamhash: input keys are not sorted")
	ErrKeyCountMismatch = errors.New("streamhash: key count mismatch")
)

// Construction errors
var (
	ErrPayloadTooLarge             = errors.New("streamhash: PayloadSize exceeds maximum 8 bytes")
	ErrFingerprintTooLarge         = errors.New("streamhash: FingerprintSize exceeds maximum (4 bytes)")
	ErrSplitBucketSeedSearchFailed = errors.New("streamhash: split bucket seed search failed - retry with different globalSeed")
	ErrIndistinguishableHashes     = errors.New("streamhash: indistinguishable hashes in bucket - retry with different globalSeed")
)

// Index errors
var (
	ErrInvalidMagic   = errors.New("streamhash: invalid magic number")
	ErrInvalidVersion = errors.New("streamhash: unsupported version")
	ErrChecksumFailed = errors.New("streamhash: file checksum verification failed")
	ErrTruncatedFile  = errors.New("streamhash: index file is truncated")
	ErrCorruptedIndex = errors.New("streamhash: index data is corrupted")
)

// Query errors
var (
	ErrIndexClosed = errors.New("streamhash: index is closed")
	ErrNoPayload   = errors.New("streamhash: index has no payload data")
	ErrNotFound    = errors.New("streamhash: key not found")
)

// Internal errors (used by algorithm implementations, not re-exported)
var (
	ErrInvalidGeometry = errors.New("streamhash: invalid geometry parameters")
	ErrBlockOverflow   = errors.New("streamhash: block overflow")
)
