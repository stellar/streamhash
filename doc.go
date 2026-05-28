// Package streamhash implements a Minimal Perfect Hash Function (MPHF) library
// with streaming build support and bounded RAM usage.
//
// StreamHash is designed for building large-scale indexes (1B+ keys) efficiently.
// See the README for performance characteristics.
//
// # Basic Usage
//
// Building an index (sorted input):
//
//	builder, err := streamhash.NewSortedBuilder(ctx, "index.idx", totalKeys)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer builder.Close()
//	for key, payload := range sortedKeys {
//	    if err := builder.AddKey(key, payload); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := builder.Finish(); err != nil {
//	    log.Fatal(err)
//	}
//
// Building an index (unsorted input):
//
//	builder, err := streamhash.NewUnsortedBuilder(ctx, "index.idx", totalKeys, "/tmp")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer builder.Close()
//	for key, payload := range keys {
//	    if err := builder.AddKey(key, payload); err != nil {
//	        log.Fatal(err)
//	    }
//	}
//	if err := builder.Finish(); err != nil {
//	    log.Fatal(err)
//	}
//
// Querying an index:
//
//	idx, err := streamhash.Open("index.idx")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer idx.Close()
//
//	rank, err := idx.QueryRank(streamhash.PreHash([]byte("mykey")))
//	if err != nil {
//	    log.Fatal(err)
//	}
//	fmt.Printf("Key rank: %d\n", rank)
//
// # Package Structure
//
// The implementation is organized as follows:
//
//   - Public API: builder.go (NewSortedBuilder), builder_unsorted.go (NewUnsortedBuilder), index.go (Open, QueryRank, PayloadIndex)
//   - Configuration: builder_options.go (BuildOption, With* functions)
//   - Serialization: header.go (header, footer, ramIndexEntry), index_writer.go
//   - Key routing & hashing: key.go (fingerprint extraction, payload packing), prehash.go (PreHash); block routing is FastRange32 over the big-endian prefix, inline in builder.go/index.go
//   - Algorithm dispatch: algorithm.go (blockBuilder/blockDecoder interfaces, factory functions)
//   - Block algorithms: internal/bijection/ (EF/GR), internal/ptrhash/ (Cuckoo)
//   - Platform: platform_*.go (OS-specific: fallocate)
package streamhash
