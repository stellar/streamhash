package streamhash

import (
	"encoding/binary"

	"github.com/zeebo/xxh3"
)

// PreHash applies xxHash3-128 to a key, returning 16 bytes.
//
// Use this function when your keys are not uniformly distributed (e.g., strings,
// URLs, sequential integers, JSON). Pre-hashing transforms arbitrary input into
// uniformly random 128-bit values required by the MPHF construction.
//
// # Usage
//
// For unsorted builds, prehash keys before building:
//
//	hashedKeys := make([][]byte, len(keys))
//	for i, key := range keys {
//	    hashedKeys[i] = streamhash.PreHash(key)
//	}
//
// For sorted builds (Builder), you must sort by the HASHED key, not the
// original key. The original keys are discarded; only hashed keys are indexed:
//
//	// 1. Prehash all keys (original keys are no longer needed)
//	hashedKeys := make([][]byte, len(keys))
//	for i, key := range keys {
//	    hashedKeys[i] = streamhash.PreHash(key)
//	}
//
//	// 2. Sort by hashed key bytes
//	sort.Slice(hashedKeys, func(i, j int) bool {
//	    return bytes.Compare(hashedKeys[i], hashedKeys[j]) < 0
//	})
//
//	// 3. Build using Builder
//	builder, _ := streamhash.NewSortedBuilder(ctx, path, uint64(len(hashedKeys)))
//	for _, hk := range hashedKeys {
//	    builder.AddKey(hk, 0)
//	}
//	err := builder.Finish()
//
// # When to use
//
//   - Strings, URLs, file paths: highly non-uniform prefix distribution
//   - Sequential integers: all keys share common high bits
//   - UUIDs with common prefixes: e.g., all start with same version nibble
//   - Any key where the first 16 bytes are not uniformly random
//
// # When NOT to use
//
//   - Keys are already random 128-bit values (e.g., random UUIDs, crypto hashes)
//   - Keys are already uniformly distributed across their prefix bits
//
// Querying: If you prehash keys during build, you must also prehash during query:
//
//	rank, err := idx.QueryRank(streamhash.PreHash(originalKey))
func PreHash(key []byte) []byte {
	result := make([]byte, 16)
	PreHashInPlace(result, key)
	return result
}

// PreHashInPlace applies xxHash3-128 to a key, writing the result to dst.
// dst must be at least 16 bytes. This avoids allocation when processing
// many keys in a loop.
func PreHashInPlace(dst []byte, key []byte) {
	h := xxh3.Hash128(key)
	binary.LittleEndian.PutUint64(dst[0:8], h.Lo)
	binary.LittleEndian.PutUint64(dst[8:16], h.Hi)
}
