# StreamHash

A Go library for building and querying **Minimal Perfect Hash Function (MPHF)** indexes over billions of keys, using bounded RAM and streaming construction.

An MPHF maps N keys to N consecutive integers [0, N) with no collisions. This enables compact, read-only lookup tables where every key has a unique position — O(1) lookups without storing the keys themselves.

## Features

- **Streaming construction** — build indexes over 1B+ keys with ~1–75 MB of RAM, regardless of dataset size
- **Sorted and unsorted input** — pre-sorted keys build with no temp disk; unsorted keys use per-writer temp files
- **Parallel builds** — block-independent construction scales near-linearly with worker count
- **Concurrent writers** — unsorted mode supports multiple concurrent writers for parallel key ingestion
- **Two algorithms** — Bijection (compact, low RAM) and PTRHash (fast queries)
- **Optional payloads** — store 1–8 byte fixed-size values alongside keys
- **Optional fingerprints** — detect non-member keys with configurable false-positive rates
- **Single-file output** — one mmap'd file for queries, no external dependencies

## Performance

**Index size**: Bijection produces the most compact indexes (~2.5 bits/key MPHF overhead). PTRHash is slightly larger (~2.7 bits/key) but offers significantly faster queries.

**Query latency**: PTRHash queries are O(1) via direct pilot lookup, roughly 20× faster than Bijection's O(128) checkpoint-based decoding. Query latency is the same regardless of build mode. Each query requires one metadata read from disk; payload mode adds a second read.

**Build throughput**: Both algorithms build at similar speeds. Sorted input is faster than unsorted at higher worker counts because blocks are streamed directly to workers without an intermediate partition-read phase. Unsorted builds match sorted throughput at 1 worker and scale well with more workers but plateau below sorted peak throughput. Adding more workers via `WithWorkers(n)` scales build throughput near-linearly up to the number of blocks.

**Memory**: Sorted builds use very little heap (single-digit MB at 1 worker). Unsorted builds use more due to per-writer flush buffers (~12 MB per writer) and the read-phase pipeline, but remain bounded regardless of dataset size. With `AddKeys(N, fn)`, the AddKey phase parallelizes across N concurrent writers.

Run `go run ./cmd/bench` on your hardware for concrete numbers (see [Benchmarking](#benchmarking)).

## Installation

```
go get github.com/stellar/streamhash
```

Requires Go 1.26+.

## Usage

### Building an index (sorted input)

Keys must be at least 16 bytes and uniformly distributed. Use `PreHash` for non-uniform keys (strings, integers, etc.).

```go
builder, err := streamhash.NewSortedBuilder(ctx, "index.idx", totalKeys,
    streamhash.WithPayload(4),
    streamhash.WithWorkers(4),
)
if err != nil {
    log.Fatal(err)
}
defer builder.Close()

for _, key := range sortedKeys {
    if err := builder.AddKey(key, payload); err != nil {
        log.Fatal(err)
    }
}
if err := builder.Finish(); err != nil {
    log.Fatal(err)
}
```

### Building an index (unsorted input)

```go
builder, err := streamhash.NewUnsortedBuilder(ctx, "index.idx", totalKeys, "/tmp/myindex",
    streamhash.WithWorkers(4),
)
if err != nil {
    log.Fatal(err)
}
defer builder.Close()

for _, key := range keys {
    if err := builder.AddKey(key, payload); err != nil {
        log.Fatal(err)
    }
}
if err := builder.Finish(); err != nil {
    log.Fatal(err)
}
```

### Building with concurrent writers (unsorted input)

```go
builder, err := streamhash.NewUnsortedBuilder(ctx, "index.idx", totalKeys, "/tmp/myindex",
    streamhash.WithWorkers(16),
)
if err != nil {
    log.Fatal(err)
}
defer builder.Close()

// AddKeys calls Finish internally.
if err := builder.AddKeys(8, func(writerID int, addKey func([]byte, uint64) error) error {
    for key, payload := range myPartition(writerID) {
        if err := addKey(key, payload); err != nil {
            return err
        }
    }
    return nil
}); err != nil {
    log.Fatal(err)
}
```

### Querying

```go
idx, err := streamhash.Open("index.idx")
if err != nil {
    log.Fatal(err)
}
defer idx.Close()

// MPHF mode: get the rank (0-based index) for a key
rank, err := idx.QueryRank(key)

// Payload mode: get the stored payload for a key
pi, err := idx.WithPayload()
rank, payload, err := pi.QueryPayload(key)
```

### Pre-hashing non-uniform keys

If your keys are not already uniformly random (e.g., strings, sequential integers, UUIDs), pre-hash them before building and querying:

```go
// Pre-hash and sort
hashedKeys := make([][]byte, len(keys))
for i, key := range keys {
    hashedKeys[i] = streamhash.PreHash(key)
}
sort.Slice(hashedKeys, func(i, j int) bool {
    return bytes.Compare(hashedKeys[i], hashedKeys[j]) < 0
})

// Build
builder, err := streamhash.NewSortedBuilder(ctx, "index.idx", uint64(len(hashedKeys)))
if err != nil {
    log.Fatal(err)
}
defer builder.Close()
for _, hk := range hashedKeys {
    if err := builder.AddKey(hk, 0); err != nil {
        log.Fatal(err)
    }
}
if err := builder.Finish(); err != nil {
    log.Fatal(err)
}

// Query: pre-hash the lookup key
rank, err := idx.QueryRank(streamhash.PreHash(originalKey))
```

## Build Options

| Option | Description | Default |
|---|---|---|
| `WithWorkers(n)` | Parallel build workers | 1 |
| `WithPayload(sizeBytes)` | Payload size in bytes (0-8) | 0 (MPHF only) |
| `WithFingerprint(sizeBytes)` | Fingerprint size in bytes (0-4) | 0 (disabled) |
| `WithAlgorithm(algo)` | `AlgoBijection` or `AlgoPTRHash` | `AlgoBijection` |
| `WithGlobalSeed(seed)` | Hash seed (change on build failure) | fixed default |
| `WithMetadata(data)` | Arbitrary metadata stored in the file | none |

## Choosing an Algorithm

**Bijection** (default) — best for most use cases. Smallest indexes and lowest RAM usage. Query decoding is O(128) via checkpoint-based Elias-Fano/Golomb-Rice decoding.

**PTRHash** — best when query speed is critical. O(1) queries via direct pilot byte lookup, at the cost of slightly larger indexes and more RAM during construction.

## Design

StreamHash partitions keys into fixed-size blocks by prefix, routes each key to its block, and delegates MPHF construction to a pluggable algorithm. This block-partitioning architecture enables:

- **Bounded RAM**: each block is solved independently, using only O(block) memory
- **Parallelism**: workers solve blocks concurrently while a coordinator sequences output
- **Locality**: all metadata for a block is contiguous on disk — one read per query

The framework handles key routing, file layout, parallel coordination, and payload/fingerprint storage. Algorithms only need to implement a build-time solver and a query-time decoder.

See [streamhash-spec.md](streamhash-spec.md) for the full technical specification.

## Benchmarking

```bash
# MPHF-only mode (core hash function performance)
go run ./cmd/bench -keys 10000000 -payload 0 -fp 0 -algo bijection

# With payload and fingerprint
go run ./cmd/bench -keys 10000000 -payload 4 -fp 1 -algo ptrhash -workers 4
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
