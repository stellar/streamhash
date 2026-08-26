package streamhash

import "github.com/stellar/streamhash/internal/sherr"

// Build errors.
var (
	ErrBuilderClosed    = sherr.ErrBuilderClosed
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

	// ErrBlockOverflow signals that a block exceeded its per-block key cap
	// during construction — non-uniform or adversarial keys concentrating into
	// one block (see the security notes in streamhash-spec.md §7.7). Exported so
	// consumers can errors.Is on it, e.g. to rebuild with a different routing
	// transform instead of treating the failure as fatal.
	ErrBlockOverflow = sherr.ErrBlockOverflow
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
