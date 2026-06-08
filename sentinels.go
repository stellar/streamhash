package streamhash

import "github.com/stellar/streamhash/internal/sherr"

// Build errors.
var (
	ErrBuilderClosed    = sherr.ErrBuilderClosed
	ErrEmptyIndex       = sherr.ErrEmptyIndex
	ErrTooManyKeys      = sherr.ErrTooManyKeys
	ErrKeyTooShort      = sherr.ErrKeyTooShort
	ErrKeyTooLong       = sherr.ErrKeyTooLong
	ErrPayloadOverflow  = sherr.ErrPayloadOverflow
	ErrDuplicateKey     = sherr.ErrDuplicateKey
	ErrUnsortedInput    = sherr.ErrUnsortedInput
	ErrKeyCountMismatch = sherr.ErrKeyCountMismatch
)

// Construction errors.
var (
	ErrPayloadTooLarge             = sherr.ErrPayloadTooLarge
	ErrFingerprintTooLarge         = sherr.ErrFingerprintTooLarge
	ErrSplitBucketSeedSearchFailed = sherr.ErrSplitBucketSeedSearchFailed
	ErrIndistinguishableHashes     = sherr.ErrIndistinguishableHashes
)

// Index errors.
var (
	ErrInvalidMagic   = sherr.ErrInvalidMagic
	ErrInvalidVersion = sherr.ErrInvalidVersion
	ErrChecksumFailed = sherr.ErrChecksumFailed
	ErrTruncatedFile  = sherr.ErrTruncatedFile
	ErrCorruptedIndex = sherr.ErrCorruptedIndex
)

// Query errors.
var (
	ErrIndexClosed = sherr.ErrIndexClosed
	ErrNoPayload   = sherr.ErrNoPayload
	ErrNotFound    = sherr.ErrNotFound
)
