# StreamHash

**A streaming, algorithm-agnostic framework for Minimal Perfect Hash construction**

**Technical Specification**

---

## Table of Contents

1. [Overview](#1-overview)
2. [How It Works](#2-how-it-works)
3. [File Format](#3-file-format) *(normative)*
4. [Algorithm: Bijection](#4-algorithm-bijection) *(normative)*
5. [Algorithm: PTRHash](#5-algorithm-ptrhash) *(normative)*
6. [Construction](#6-construction)
7. [Operations](#7-operations)
8. [Appendices](#8-appendices)

**How to read this document**

- **Just want the idea?** §1 (Overview) and §2 (How It Works).
- **Choosing whether to use it?** §1.3 (When to use it) and §1.4 (Choosing an algorithm).
- **Reimplementing the format?** §3, §4, and §5 are the normative reference, together with §2.5 (the fingerprint mixer) and §2.6 (query assembly) — which are equally required for byte-identical, interoperable files in any language.
- **Building the library?** §6 (Construction) describes the reference build pipeline; §7 (Operations) covers limits, errors, and safety.
- Term definitions live in the [Glossary](#83-glossary).

Go-specific names in this document refer to the reference implementation; the on-disk format itself is language-independent.

---

## 1. Overview

### 1.1. What is an MPHF?

A **Minimal Perfect Hash Function (MPHF)** maps a fixed set of N keys to the N consecutive integers `[0, N)` with no collisions and no gaps. This gives you a compact, read-only lookup table where every key has a unique position — we call that position the key's **rank**, in `[0, N)`. The result is O(1) lookups *without storing the keys themselves*.

### 1.2. What is StreamHash?

StreamHash is a framework for building MPHF indexes that is:

- **Streaming and bounded-RAM** — it builds indexes over billions of keys in memory that does not grow with the dataset. A single-worker sorted build uses only a few MB; see §1.5 and §6 for the full picture.
- **Locality-optimized for queries** — each block is a few thousand keys whose entire query state sits in one contiguous on-disk region, so a lookup is a single read.
- **Parallel** — the CPU-bound solving step runs independently per block, so construction scales with worker count while one coordinator sequences output.
- **Algorithm-agnostic** — the perfect-hashing algorithm is pluggable. The framework handles key routing, file layout, parallelism, and payload/fingerprint storage; an algorithm only supplies a build-time *solver* and a query-time *decoder* (see §2.4).

The core idea: partition keys into many small, fixed-size **blocks** by key prefix, and solve each block's MPHF independently. Within a block, an algorithm further groups keys into tiny **buckets** (~3 keys each) that it solves one at a time, and the result is the key's **slot** in the block. So the data hierarchy is **block → bucket → slot**. Small independent blocks are what make bounded RAM, single-read queries, and parallelism all fall out naturally.

Two algorithms ship today — **Bijection** (default; smallest index, lowest build RAM) and **PTRHash** (fastest queries). §1.4 helps you choose between them and §1.5 has the measured numbers.

### 1.3. When to use it

With a configured payload size (1–8 bytes), the index doubles as a **static perfect hash map** — it returns a small fixed-size value per key instead of a rank. Typical uses: content-hash → storage location, primary key → row offset, document ID → posting-list offset, object key → compact metadata. For larger or variable-length values, store the rank and use it as an offset into an external file.

**StreamHash is a good fit when:**

- You have **many keys** (roughly ≥100K). Below that, fixed per-block overhead dominates — use a plain hash table or a monolithic MPHF.
- The set is **static**. There are no inserts or deletes after construction.
- Keys are **uniformly random**, or you pre-hash them (see §8.1). Routing by key prefix assumes a uniform prefix distribution.

**Membership is not guaranteed by default.** Like any MPHF, the index returns *some* rank for any input, including keys that were never indexed. If you need to reject non-members, enable fingerprints (`WithFingerprint`, 1–4 bytes, see §7.2): they detect non-members with a tunable false-positive rate, the same way a Bloom filter does.

### 1.4. Choosing an algorithm

| | Bijection | PTRHash |
|---|---|---|
| **Best for** | Compactness, low build RAM | Query speed |
| Index size | ~2.46 bits/key | ~2.70 bits/key |
| Query latency (CPU only) | ~1.5 µs (decode ≤128 buckets) | ~0.07 µs (one pilot byte) |
| Build throughput (4 workers) | ~65 M keys/sec | ~62 M keys/sec |
| Peak heap (4 workers) | ~8 MB | ~61 MB |

This table is a quick decision aid; §1.5 has the raw measurements, including single-worker numbers.

**Use Bijection** (the default) when index size or build RAM matters most. It compresses better and uses far less RAM during construction.

**Use PTRHash** when query latency is critical: it reads a single pilot byte per query, roughly 20× faster than Bijection's checkpoint-based decode, at the cost of a slightly larger index and more build RAM.

Build throughput is similar for both. Note the latencies above are **CPU-only**: that 20× edge is real when the index is resident in RAM, but if each query hits disk (~100 µs per NVMe read, §1.5) both algorithms are I/O-bound and the difference disappears.

### 1.5. Performance summary

Reference measurements on an Apple M1 Max, 100M keys, MPHF mode, pre-sorted input:

| Metric | Bijection | PTRHash |
|--------|-----------|---------|
| Index size (MPHF) | ~2.46 bits/key | ~2.70 bits/key |
| Query latency (CPU only) | ~1.5 µs | ~0.07 µs |
| Build throughput (1 / 4 workers) | ~16 / ~65 M keys/sec | ~17 / ~62 M keys/sec |
| Peak heap (1 / 4 workers) | ~1 / ~8 MB | ~8 / ~61 MB |

| | Pre-sorted input | Unsorted input |
|---|---|---|
| Build throughput (1 / 4 workers) | ~16 / ~62 M keys/sec | ~16 / ~41 M keys/sec |
| Build I/O | 1× read | 1× read + 1× write (temp files) |
| Temp disk | none | `N × (16 + PayloadSize)` bytes |
| Build RAM | single-digit MB (1 worker) | tens of MB write phase; read phase bounded (§6.3) |

Notes:

- **Query latency excludes disk I/O.** MPHF mode is one read per query; payload/fingerprint mode is two (metadata region + payload region, at different offsets). On NVMe a read is ~100 µs, which dominates the CPU time above.
- **Unsorted builds** match sorted throughput at one worker. At higher worker counts they trail sorted (~60% at 4 workers) because the partition read-back is the bottleneck.
- **Scale** is bounded by the 40-bit RAM-index fields: at most `2^40 − 1` keys (~1.1 trillion). See §7.3.

---

## 2. How It Works

StreamHash answers every query in **two levels**: the *framework* routes a key to one **block** (a few thousand keys), then that block's *algorithm* finds the key's **slot** inside it. The answer combines them — `rank = keysBefore + slot`. Keep the **block → bucket → slot** hierarchy in mind: keys are partitioned into blocks, each algorithm subdivides a block into small **buckets** (~3 keys) it solves independently, and the output is a slot. This section follows that path — how key bytes are read (§2.1), the two-level dispatch in detail (§2.2), how the block count is chosen (§2.3), the framework/algorithm split (§2.4), fingerprints (§2.5), and the fully assembled query (§2.6).

### 2.1. How keys are read

The framework and the algorithms read the same key bytes in different ways; a key must be at least 16 bytes (`MinKeySize`). Two of the three "views" below are the *same eight bytes* read two ways — `prefix` (big-endian) and `k0` (little-endian), related by `prefix = ReverseBytes64(k0)`. The framework needs `prefix` for sort-order-preserving routing (§2.2); the algorithms use `k0`/`k1` for fast native arithmetic.

| Name | Bytes | Interpretation | Used by | Why this interpretation |
|------|-------|----------------|---------|-------------------------|
| `prefix` | 0–7 | **big-endian** uint64 | framework | sort-order-preserving ⇒ monotonic routing (§2.2) |
| `k0` | 0–7 | **little-endian** uint64 | algorithms | native CPU format; no byte-swap in hash math |
| `k1` | 8–15 | **little-endian** uint64 | algorithms | a second, independent 64-bit value |

### 2.2. Two-level dispatch

Every query is answered in two steps:

```
Query(key) →  Level 1: framework routes the key to a block
          →  Level 2: the algorithm computes a slot within that block
          →  combine into a global rank (see "Final rank" below)
```

**Level 1 — Framework (routing).** The framework selects a block from the key's `prefix`:

```
blockIdx = fastRange32(prefix, numBlocks)
```

`fastRange32` maps a 64-bit hash near-uniformly onto `[0, n)`, avoiding the much larger modulo bias of `hash % n`. Each output value gets `⌊2⁶⁴/n⌋` or `⌈2⁶⁴/n⌉` of the 2⁶⁴ possible hashes — exactly uniform only when `n` is a power of two, with imbalance below `n / 2⁶⁴`, negligible at these sizes:

```
function fastRange32(hash: uint64, n: uint32) → uint32:
    hi, _ = Mul128(hash, uint64(n))   // high 64 bits of the 128-bit product
    return uint32(hi)
```

`Mul128(a, b)` returns the high and low 64-bit halves of the 128-bit product `a × b` (the spec uses it throughout for 64×64→128 multiplication); the "32" in the name is the 32-bit output range, so `n` must fit in a uint32. Because `fastRange32` is monotonic in `hash`, sorted prefixes route to non-decreasing block indices — the property that enables streaming construction one block at a time.

**Level 2 — Algorithm (slot).** The framework reads the chosen block's metadata and hands the algorithm `(k0, k1, metadata, keysInBlock)`. The algorithm returns a local **slot** in `[0, keysInBlock)`. Each algorithm has its own bucket layout, seed/pilot search, and metadata encoding (§4, §5).

**Final rank.** The framework tracks how many keys precede each block, so:

```
rank = keysBefore + slot      // globally unique, in [0, N)
```

### 2.3. How `numBlocks` is chosen

The framework asks the algorithm for the block count at build time. Each algorithm derives it from two of its own constants — `lambda` (target average keys per bucket) and `bucketsPerBlock`:

```
totalBuckets = ceil(N / lambda)
numBlocks    = max(2, ceil(totalBuckets / bucketsPerBlock))
```

| Algorithm | lambda | bucketsPerBlock | ≈ keys/block |
|-----------|--------|-----------------|--------------|
| Bijection | 3.0 | 1024 | ~3,072 |
| PTRHash | 3.16 | 10,000 | ~31,600 |

*Example (Bijection, 10M keys):* `totalBuckets = ceil(10M/3) ≈ 3.33M`, `numBlocks = ceil(3.33M/1024) = 3,256` blocks of ~3,072 keys each.

`numBlocks` is the only one of these values stored in the file (in the header). `lambda` and `bucketsPerBlock` are algorithm-internal constants and are *not* in the header; a decoder reconstructs everything it needs from `numBlocks` plus its compile-time constants.

### 2.4. The framework / algorithm contract

The framework owns everything that is not perfect-hashing math; an algorithm owns the math.

| Framework provides | Algorithm provides |
|--------------------|--------------------|
| Key-length validation (≥16 bytes) | Block count (`numBlocks`) |
| Routing (`fastRange32(prefix, numBlocks)`) | Bucket layout within a block |
| RAM index, file layout, regions, footer | MPHF solving (seed/pilot search) |
| Payload + fingerprint storage | Metadata encoding and decoding |
| Fingerprint extraction (§2.5) | Slot computation at query time |
| Parallel coordination, temp files | Duplicate-key detection while solving |
| Integrity hashing (hash-of-hashes) | |

An algorithm must satisfy four requirements:

1. **Produce a minimal perfect hash** — slots must cover exactly `[0, keysInBlock)` with no gaps, because the framework indexes payloads directly at `rank × entrySize`. (A non-minimal algorithm may include its own remap table, as PTRHash does.)
2. **Work from `(k0, k1)` only** — 128 bits per key. Algorithms needing more entropy would require framework changes.
3. **Encode all query state into a flat metadata blob** — the decoder must rebuild the hash from `(k0, k1, metadata, keysInBlock)` alone.
4. **Handle any block size, including empty** — block sizes vary (keys land in blocks by a Poisson process). For `keysInBlock = 0` the framework short-circuits before calling the decoder, but the solver's reset must still handle the transition.

The **solver is single-threaded** (each parallel worker owns one instance); the **decoder is stateless and thread-safe** (created once at open time, shared across all queries).

> **For new-algorithm authors (relates to requirement 2): a note on within-block correlation.** Because keys are routed by `prefix = BigEndian(key[0:8])`, keys in one block share the top `log₂(numBlocks)` bits of the prefix — which are the *low* `log₂(numBlocks)` bits of `k0`, from just the low byte at small block counts up to ~28 bits (the low ~3.5 bytes) at the 2⁴⁰-key maximum. Those pinned bits always stay below bit 32, so `k0`'s high 32 bits, and all of `k1` (bytes 8–15, independent of the prefix), remain effectively random. Neither shipped algorithm is affected: Bijection buckets on `fastRange32(k0, …)`, dominated by `k0`'s high bits, and PTRHash buckets on `k1`. A new algorithm sensitive to this can internally re-hash `(k0, k1)`.

### 2.5. Fingerprints

Fingerprints are extracted by the **framework**, not the algorithms, with a single mixer over both key halves:

```
function extractFingerprint(k0, k1) → uint32:
    h = k0 XOR (k1 × 0x517cc1b727220a95)
    return uint32(h >> 32) & mask(FingerprintSize)
```

The constant `0x517cc1b727220a95` is odd, so `k1 × C` is a bijection on uint64 and preserves `k1`'s entropy. The fingerprint reads the **high** 32 bits of the mix because routing constrains only `k0`'s low bits (≤ ~28, always below bit 32 — see the §2.4 note), and XOR has no carry, so those pinned bits never reach bits 32–63. There, neither `k0`'s high bits nor `k1 × C`'s contribution depends on the routing prefix — so the fingerprint is near-independent of routing, and a mismatch reliably flags a non-member. See §7.2 for how queries use it.

### 2.6. Query, end to end

This uses the on-disk RAM index (§3.4) and metadata region (§3.6); the field names below are defined there — read it as the data-flow.

```
function Query(key) → (rank, error):
    k0     = LittleEndian.Uint64(key[0:8])
    k1     = LittleEndian.Uint64(key[8:16])
    prefix = BigEndian.Uint64(key[0:8])

    blockIdx  = fastRange32(prefix, numBlocks)          // route (§2.2)

    entry     = ramIndex[blockIdx]                       // RAM index (§3.4)
    nextEntry = ramIndex[blockIdx + 1]
    keysBefore  = entry.KeysBefore
    keysInBlock = nextEntry.KeysBefore - entry.KeysBefore
    if keysInBlock == 0:
        return error(NotFound)

    metadata = metadataRegion[entry.MetadataOffset : nextEntry.MetadataOffset]
    slot     = decoder.QuerySlot(k0, k1, metadata, keysInBlock)   // dispatch (§4.6 / §5.5)
    rank     = keysBefore + slot

    // Optional, only when FingerprintSize > 0:
    if storedFingerprint(rank) != extractFingerprint(k0, k1):
        return error(NotFound)

    return rank
```

Queries are lock-free: the index is immutable after construction and each query reads an independent block, so no synchronization is needed between concurrent queries.

## 3. File Format

The format is language-independent. The byte layouts, encodings, and hash functions below are sufficient to produce interoperable files from any language.

### 3.1. File layout

```
+--------------------------------------------------+
|  Header (64 B)                                   |
+--------------------------------------------------+
|  UserMetadataLen (4 B) + UserMetadata (variable) |
+--------------------------------------------------+
|  AlgoConfigLen (4 B) + AlgoConfig (variable)     |
+--------------------------------------------------+
|  RAM Index   ((NumBlocks + 1) × 10 B)            |
+--------------------------------------------------+
|  Payload Region  (N × entrySize B)               |
+--------------------------------------------------+
|  Metadata Region  (variable)                     |
+--------------------------------------------------+
|  Footer (32 B)                                   |
+--------------------------------------------------+
```

```
Offset            Size                Content
------------------------------------------------------------------
0                 64 B                Header
64                4 B                 UserMetadataLen (uint32_le)
68                UML B               UserMetadata
68+UML            4 B                 AlgoConfigLen (uint32_le)
72+UML            ACL B               AlgoConfig
72+UML+ACL        (NumBlocks+1)×10 B  RAM Index
ramIndexEnd       N × entrySize B     Payload Region
payloadEnd        variable            Metadata Region
metadataEnd       32 B                Footer
```

where `UML = UserMetadataLen`, `ACL = AlgoConfigLen`, `N = TotalKeys`, and `entrySize = PayloadSize + FingerprintSize`.

### 3.2. Header (64 bytes)

| Offset | Size | Field | Type | Description |
|--------|------|-------|------|-------------|
| 0 | 4 | Magic | uint32_le | `0x53544D48` ("STMH") |
| 4 | 2 | Version | uint16_le | `0x0001` |
| 6 | 8 | TotalKeys | uint64_le | Total keys (N) |
| 14 | 4 | NumBlocks | uint32_le | Number of blocks |
| 18 | 4 | PayloadSize | uint32_le | Payload bytes per key (0 = MPHF only) |
| 22 | 1 | FingerprintSize | uint8 | Fingerprint bytes (0–4) |
| 23 | 8 | Seed | uint64_le | Global hash seed |
| 31 | 2 | BlockAlgorithm | uint16_le | 0 = Bijection, 1 = PTRHash |
| 33 | 31 | Reserved | bytes | Zero-filled |

`TotalBuckets`, `BucketsPerBlock`, and similar values are **not** in the header — they are algorithm-internal and derived from `NumBlocks` (§2.3). Note that `Seed` and `BlockAlgorithm` are intentionally unaligned (they start at offsets 23 and 31).

### 3.3. Variable-length sections

Immediately after the header:

```
[UserMetadataLen: uint32_le][UserMetadata: UserMetadataLen bytes]
[AlgoConfigLen:   uint32_le][AlgoConfig:   AlgoConfigLen bytes]
```

- **UserMetadata** — application-defined bytes, not interpreted by the index. May be zero-length.
- **AlgoConfig** — per-algorithm configuration. Currently zero-length for both shipped algorithms (they use compile-time constants); reserved for future tunable algorithms.

### 3.4. RAM Index

`NumBlocks + 1` entries. The final entry is a **sentinel**: it stores `KeysBefore == TotalKeys` and the end offset of the last block's metadata, so the last block's size can be computed by subtraction like any other.

**Entry (10 bytes):**

| Offset | Size | Field | Type |
|--------|------|-------|------|
| 0 | 5 | KeysBefore | uint40_le |
| 5 | 5 | MetadataOffset | uint40_le |

- `KeysBefore` — cumulative keys in all blocks before this one.
- `MetadataOffset` — byte offset of this block's metadata, relative to the start of the metadata region.

Per-block values are derived by subtraction (this is the query path in §2.6):

```
keysInBlock  = ramIndex[b+1].KeysBefore     - ramIndex[b].KeysBefore
metadataSlice = metadataRegion[ramIndex[b].MetadataOffset : ramIndex[b+1].MetadataOffset]
```

Payloads need no offset in the index — they are at a fixed stride: `payloadOffset = rank × entrySize`.

**The RAM index is an optimization, not a requirement.** It lives at a fixed, computable file offset, so a query can instead read the two needed entries (20 contiguous bytes) straight from disk — one extra read per query. Keeping it in memory is cheap (10 B/block, e.g. ~32 KB for a 10M-key Bijection index of ~3,256 blocks), so the reference implementation does.

### 3.5. Payload Region

Contiguous, one entry per key at its rank position:

```
entryOffset = payloadRegionOffset + rank × (PayloadSize + FingerprintSize)
entry layout: [Fingerprint: FingerprintSize bytes][Payload: PayloadSize bytes]

fingerprintOffset = entryOffset + 0
payloadOffset     = entryOffset + FingerprintSize
```

Fingerprints come first (for fast access during verification). Both fields are little-endian: a 4-byte payload `0x12345678` is stored `78 56 34 12`. The region is empty when `entrySize = 0` (pure MPHF mode).

### 3.6. Metadata Region

Per-block metadata in block order, variable length, algorithm-specific:

- **Bijection** — checkpoints (28 B) + Elias-Fano data + Golomb-Rice seed stream + fallback list (§4.5).
- **PTRHash** — pilot bytes (`bucketsPerBlock`) + remap table (§5.4).

Empty blocks (`keysInBlock = 0`) still occupy a metadata entry, so block offsets stay computable by subtraction (§3.4); the query path short-circuits empty blocks before decoding (§2.6). Their fixed encoded sizes are:

- **Bijection:** 28-byte zero checkpoints + Elias-Fano for 1024 zero cumulatives (128 B) + 1 zero terminator byte = **157 bytes**.
- **PTRHash:** `bucketsPerBlock` zero pilot bytes + `uint16_le(0)` empty remap = **10,002 bytes**.

### 3.7. Footer (32 bytes)

| Offset | Size | Field | Type |
|--------|------|-------|------|
| 0 | 8 | PayloadRegionHash | uint64_le |
| 8 | 8 | MetadataRegionHash | uint64_le |
| 16 | 16 | Reserved | bytes |

Both use canonical **unseeded xxHash64**:

- **MetadataRegionHash** — a single pass over the raw metadata-region bytes (region start to footer start).
- **PayloadRegionHash** — a deterministic **hash-of-hashes** so it can be folded incrementally during a parallel build:

  ```
  hasher = xxHash64_streaming()
  for blockID in 0 .. NumBlocks-1:
      startKey = ramIndex[blockID].KeysBefore
      endKey   = ramIndex[blockID+1].KeysBefore
      if endKey > startKey and entrySize > 0:
          blockHash = xxHash64(payloadRegion[startKey×entrySize : endKey×entrySize])
      else:
          blockHash = xxHash64(empty)        // = 0xEF46DB3751D8E999
      hasher.Write(LittleEndian.Bytes8(blockHash))
  PayloadRegionHash = hasher.Sum64()
  ```

  Each block's payload bytes are hashed independently, then each 8-byte block hash is fed in block order into a streaming xxHash64.

### 3.8. Worked example

A minimal Bijection index, 5 keys in 2 blocks, MPHF mode (no payloads or fingerprints). Block 0 holds 3 keys, block 1 holds 2.

```
Offset  Bytes                                            Meaning
────────────────────────────────────────────────────────────────────────────
0x0000  48 4D 54 53 01 00 05 00 00 00 00 00 00 00 02 00  Magic "STMH", Version 1,
                                                          TotalKeys=5, NumBlocks=2 (lo)
0x0010  00 00 00 00 00 00 00 12 34 56 78 9A BC DE F0 00  NumBlocks (hi), PayloadSize=0,
                                                          FingerprintSize=0,
                                                          Seed=0xF0DEBC9A78563412,
                                                          BlockAlgorithm=0 (lo, 0x1F–0x20)
0x0020  00 ... (zeros)                                   BlockAlgorithm (hi) + Reserved (0x21–0x3F)
0x0030  00 ... (zeros)                                   Reserved
0x0040  00 00 00 00                                      UserMetadataLen = 0
0x0044  00 00 00 00                                      AlgoConfigLen = 0
0x0048  00 00 00 00 00  00 00 00 00 00                   RAM[0]: KeysBefore=0,  MetaOff=0
0x0052  03 00 00 00 00  A5 00 00 00 00                   RAM[1]: KeysBefore=3,  MetaOff=0xA5
0x005C  05 00 00 00 00  42 01 00 00 00                   RAM[2] sentinel: KeysBefore=5,
                                                          MetaOff=0x142
0x0066  ... (block 0 metadata, 0xA5 = 165 B)             Metadata region begins here
0x010B  ... (block 1 metadata, 0x9D = 157 B)
0x01A8  [32-byte footer]                                 Footer
```

The metadata region starts at `0x66` (RAM index ends at `0x48 + 3×10 = 0x66`; the payload region is empty). The per-block byte sizes shown are illustrative encoded lengths; they come straight from the RAM-index offsets by subtraction: block 0 = `0xA5 − 0 = 165 B`, block 1 = `0x142 − 0xA5 = 0x9D = 157 B`. Block 1 here happens to equal the empty-block Bijection size from §3.6 (157 B) purely by coincidence — there is nothing to reconcile between the 165 B and 157 B figures. The footer therefore sits at `0x66 + 0x142 = 0x1A8`.

---

## 4. Algorithm: Bijection

Bijection is the default algorithm: the most compact index and the lowest build RAM.

### 4.1. Overview and parameters

Bijection combines three established techniques: per-bucket bijection solving by brute-force seed search (as in [CHD](https://cmph.sourceforge.net/papers/esa09.pdf) and [RecSplit](https://epubs.siam.org/doi/pdf/10.1137/1.9781611976007.14)); Elias-Fano + Golomb-Rice succinct encoding; and one level of binary splitting for large buckets. Unlike RecSplit's recursive splitting tree, Bijection uses a *flat* per-bucket layout with 128-bucket checkpoints — trading ~0.8–0.9 bits/key of compression for a flat, checkpoint-indexed layout that builds without recursion.

Throughout this section, `globalSeed` is the 64-bit `Seed` field from the file header (§3.2); it randomizes the whole index.

| Constant | Value | Role |
|----------|-------|------|
| `lambda` | 3.0 | average keys per bucket |
| `bucketsPerBlock` | 1024 | keeps per-block metadata ~1 KB (one 4 KB page) |
| `splitThreshold` | 8 | buckets with ≥8 keys are split once |
| `checkpointInterval` | 128 | a checkpoint every 128 buckets ⇒ a query decodes ≤128 buckets |

### 4.2. Bucket assignment

```
localBucket = fastRange32(k0, 1024)        // k0 = LittleEndian(key[0:8])
```

### 4.3. The Mix function and solving

`Mix` maps a key to a slot within its bucket; the solver searches for a per-bucket `seed` that makes `Mix` collision-free (a bijection) across the bucket's keys.

```
function Mix(k0, k1, seed, bucketSize, globalSeed) → slot:
    mixed = wymix(k0 ^ globalSeed ^ seed, k1 ^ globalSeed)
    return fastRange32(mixed, bucketSize)
```

Each `seed` is XORed into `wymix`'s first operand only — the second, `k1 ^ globalSeed`, is fixed per key — and `globalSeed` decorrelates whole builds (a new global seed yields an entirely different index). `wymix(a, b)` is the WyHash v4 primitive — a 128-bit multiply with XOR fold: `hi, lo = Mul128(a, b); return hi ^ lo`. Its strong avalanche makes each seed an effectively independent trial, so a bucket of `m` keys needs about `mᵐ/m!` seeds on average to hit a bijection — ~2, ~4.5, ~11 for sizes 2–4, then rising steeply to ~26, ~65, ~163 for sizes 5–7. That steep growth is why the Golomb-Rice `k`-table (§4.4) reserves wider seed ranges for bigger buckets.

**Solving a bucket:**

- **Size 0–1:** nothing to search (slot is fixed); emit nothing in the seed stream.
- **Size 2–7 (direct):** try `seed = 0, 1, 2, …` until all keys land on distinct slots.
- **Size ≥ 8 (split):** find `seed0` so that exactly `splitPoint = floor(bucketSize / 2)` keys map below `splitPoint` (a bijection on the first half), then solve `seed1` for the second half. Both halves are then direct bijections.

Seeds too large to Golomb-Rice-encode go to the block's **fallback list** (§4.5).

### 4.4. Encodings

All bit-packed data (Elias-Fano and Golomb-Rice) is **LSB-first** within 64-bit little-endian words: bit 0 is the least significant bit of the first byte.

**Elias-Fano — cumulative bucket sizes.** The encoder stores exactly `n = bucketsPerBlock` values: the running total *after* each bucket (there is no leading zero; bucket 0's start is implicit). Splitting the values into low and high bits:

```
U       = keysInBlock
lowBits = floor(log2(floor(U / n)))    when U > n, else 0    // integer division
Lower:  n × lowBits bits, packed LSB-first
Upper:  n + (U >> lowBits) bits, unary gaps between successive high parts
```

*Worked example* — 8 buckets, 12 keys, sizes `[2,1,3,0,2,1,1,2]`:

```
Cumulative (n = 8 values, running totals): [2, 3, 6, 6, 8, 9, 10, 12]
U = 12, n = 8  →  lowBits = floor(log2(12/8)) = 0   (all bits go to the upper/unary part)

Upper bits (LSB-first), n + U = 8 + 12 = 20 bits:  0010 1000 1100 1010 1001
Lower bits: none (lowBits = 0)
Encoded bytes: [0x14, 0x53, 0x09]
```

The upper bits are the unary gaps between successive cumulative values (gap of 2 → `2 zeros, 1`; gap of 1 → `1 zero, 1`; gap of 3 → `3 zeros, 1`; …), written LSB-first. Packing the 20-bit string into bytes — also LSB-first, so the first bit of the string is bit 0 of byte 0 — reproduces `[0x14, 0x53, 0x09]`:

```
byte0 = bits 0–7   (LSB-first) = 00101000 = 0x14
byte1 = bits 8–15  (LSB-first) = 11001010 = 0x53
byte2 = bits 16–19 (LSB-first) = 1001, then 4 zero pad bits = 10010000 = 0x09
```

For larger blocks `lowBits > 0`, and the low-order bits of each value move to the packed lower section.

**Golomb-Rice — seeds.** Each seed is coded with a parameter `k` chosen from the bucket size:

```
quotient  = seed >> k
remainder = seed & ((1 << k) - 1)
emit: q ones, a 0 terminator, then k remainder bits (LSB-first)

fallback marker: 16 consecutive ones  (the seed is in the fallback list instead)
```

| Bucket size | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | ≥8 |
|-------------|---|---|---|---|---|---|---|---|-----|
| k | 0 | 0 | 1 | 2 | 3 | 4 | 5 | 7 | 8 |

Size-0 and size-1 buckets emit nothing (seed is always 0). The quotient maxes out at 15 (16 ones is the fallback marker), so the largest encodable seed is `(16 << k) − 1`; anything larger goes to the fallback list.

*Worked example* — seed 13, bucket size 3 (so `k = 2`):

```
quotient  = 13 >> 2 = 3
remainder = 13 & 3 = 1
emit: 3 ones, 0 terminator, then remainder 1 in 2 bits LSB-first (bits 1,0)
stream (LSB-first): 1 1 1 0 1 0   →  111010
```

*Fallback example* — seed 70, bucket size 3: `(16 << 2) − 1 = 63 < 70`, so emit the 16-ones marker and store 70 in the fallback list.

**Split buckets** emit two consecutive Golomb-Rice codes: `seed0` with `k = golombParameter(splitPoint)`, then `seed1` with `k = golombParameter(bucketSize − splitPoint)`. Either may be a fallback marker.

### 4.5. Block metadata layout

```
Offset    Size       Content
─────────────────────────────────────────────────────────
0         28 B       Checkpoints
28        variable   Elias-Fano data (cumulative bucket sizes)
…         variable   Golomb-Rice seed stream
end - FL  variable   Fallback list
```

**Checkpoints (28 bytes).** Two grouped arrays of seven `uint16_le` **bit offsets**, captured at the 128-bucket boundaries (buckets 128, 256, …, 896; bucket 0 is implicit at offset 0). They let a query jump to its segment and decode at most 128 buckets:

- bytes 0–13 — the seven bit offsets into the Elias-Fano **high-bits** sub-vector;
- bytes 14–27 — the seven bit offsets into the Golomb-Rice seed stream.

The two arrays are stored **grouped** (all seven Elias-Fano offsets, then all seven seed offsets), *not* interleaved as pairs. Each of the fourteen values is a `uint16_le`, consistent with the rest of the format. The values are *bit* offsets, not byte offsets, and the Elias-Fano ones index the high-bits sub-vector specifically.

**Fallback list** (located at decode time by scanning backward from the end of the block for a valid `count`/`validation` pair):

```
[count: uint8][entries: count × 4 bytes][validation: uint8]
validation = count XOR 0x55

Each 4-byte entry is a uint32_le packing three fields, with blockBits = ceil(log2(bucketsPerBlock)) = 10:
  seedBits = 31 - blockBits
  packed   = (bucketIndex << (1 + seedBits)) | (subBucket << seedBits) | seed
    bucketIndex  [blockBits bits]   which bucket
    subBucket    [1 bit]            0 = first half, 1 = second half (split buckets)
    seed         [seedBits bits]    the actual seed
```

*Packing example* (`blockBits = 10`, `seedBits = 21`): `bucketIndex = 300`, `subBucket = 1`, `seed = 70` →
`packed = (300 << 22) | (1 << 21) | 70 = 0x4B200046`, stored little-endian as bytes `[0x46, 0x00, 0x20, 0x4B]`.

*Backward-scan rule.* Starting at offset `len(metadata) − 2` and scanning down to `max(seedStreamStart, len(metadata) − 1022)`, read `count = metadata[tryOffset]`; accept that location iff `2 + count×4 == len(metadata) − tryOffset` **and** `metadata[len(metadata) − 1] == count XOR 0x55`. (`1022 = 2 + 255×4` is the maximum list size: a one-byte `count`, up to 255 four-byte entries, and a one-byte validation byte.)

### 4.6. Query

```
function QuerySlotBijection(k0, k1, metadata, keysInBlock) → slot:
    bucketIdx = fastRange32(k0, 1024)
    cp        = decodeCheckpoints(metadata[0:28])
    segment   = bucketIdx / 128                       // jump via checkpoint cp[segment]

    bucketStart = cumulative[bucketIdx - 1]           // via Elias-Fano (0 if bucketIdx == 0)
    bucketEnd   = cumulative[bucketIdx]
    bucketSize  = bucketEnd - bucketStart

    if bucketSize >= 8:                               // split bucket
        seed0, seed1 = decodeSplitSeeds(...)
        splitPoint = bucketSize / 2
        h = Mix(k0, k1, seed0, bucketSize, globalSeed)
        if h < splitPoint:
            slot = bucketStart + h
        else:
            slot = bucketStart + splitPoint + Mix(k0, k1, seed1, bucketSize - splitPoint, globalSeed)
    else if bucketSize >= 2:
        seed = decodeSeed(...)
        slot = bucketStart + Mix(k0, k1, seed, bucketSize, globalSeed)
    else:                                             // size 0 or 1
        slot = bucketStart

    return slot
```

The Mix-based slot logic above is exact; the rest of this section spells out how `bucketStart`, `bucketEnd`, and the seed(s) are obtained — the steps elided as `...` in the pseudocode. None of it changes the math.

1. **Decode the 28-byte checkpoints** from `metadata[0:28]` (§4.5): the seven Elias-Fano high-bits offsets `cp.efBitPos[0..6]` and the seven seed-stream offsets `cp.seedBitPos[0..6]`.
2. **Locate the bucket.** `bucketIdx = fastRange32(k0, 1024)` and `segment = bucketIdx / 128`. Buckets `0..127` are segment 0 (offset 0, implicit); segment `s ≥ 1` starts at `segmentStart = s × 128`, with Elias-Fano high-bits offset `cp.efBitPos[s − 1]` and seed-stream offset `cp.seedBitPos[s − 1]`.
3. **Decode the cumulatives.** Seek into the Elias-Fano high-bits sub-vector at bit offset for `segmentStart` (0 for segment 0, else `cp.efBitPos[segment − 1]`) and decode the cumulative values from `segmentStart` up through `bucketIdx`, recovering `bucketStart = cumulative[bucketIdx − 1]` (0 when `bucketIdx == 0`), `bucketEnd = cumulative[bucketIdx]`, and `bucketSize = bucketEnd − bucketStart`.
4. **Skip to the target bucket's seed(s).** Seek into the Golomb-Rice seed stream at bit offset `cp.seedBitPos[segment − 1]` (0 for segment 0), then for each bucket in `[segmentStart, bucketIdx)` decode that bucket's size from the cumulatives to know how many Golomb-Rice code(s) and bits to skip. Each code is a unary quotient (≤15 ones) then a 0 terminator then `k` remainder bits, or the 16-ones fallback marker; size-0 and size-1 buckets emit nothing, and split buckets (size ≥8) emit two codes.
5. **Decode the target seed(s).** Read the target bucket's seed (or `seed0, seed1` for a split bucket). A 16-ones fallback marker is resolved by looking the seed up in the fallback list (§4.5), keyed by `bucketIdx` and `subBucket`.
6. **Compute the slot** using the Mix-based logic in the pseudocode: `slot = bucketStart + Mix(...)` for a direct bucket, or the split-point branch for a split bucket.

Fingerprint handling is at the framework level (§2.5).

## 5. Algorithm: PTRHash

PTRHash gives O(1) queries — a single pilot-byte read — at a slightly larger index. It is an adaptation of [PtrHash by Groot Koerkamp](https://github.com/RagnarGrootKoerkamp/PtrHash) ([paper](https://arxiv.org/abs/2502.15539)), modified for StreamHash's small, fixed-size blocks (§5.6).

### 5.1. Overview and parameters

A **pilot** is a one-byte steering value per bucket. A query identifies the key's bucket, reads that bucket's pilot, and combines key and pilot into a slot. During construction the solver searches pilot values (with cuckoo-style eviction) until every key in the bucket lands on a collision-free slot. Throughout this section, `globalSeed` is the 64-bit `Seed` field from the file header (§3.2).

| Constant | Value | Role |
|----------|-------|------|
| `lambda` | 3.16 | average keys per bucket |
| `alpha` | 0.99 | load factor: `numSlots = ceil(keysInBlock / alpha)` (~1% spare slots) |
| `bucketsPerBlock` | 10,000 | fixed block size (~31,600 keys) |
| `numPilotValues` | 256 | pilots are bytes, 0–255 |

*Example (one ~31,600-key block):* 10,000 buckets averaging 3.16 keys; `numSlots = ceil(31,600/0.99) = 31,920` (320 spare slots); metadata ≈ 10,000 pilot bytes + a ~642-byte remap table (`2 + 320×2`) ≈ **10.4 KB**. The skewed bucket distribution (below) gives a largest bucket of ~341 keys.

### 5.2. Bucket assignment (CubicEps)

PTRHash deliberately makes bucket sizes **skewed**: a few large buckets and many tiny ones (the largest holds ~1.08% of keys — ~341 in a 31,600-key block — while most hold 1–3). The solver always places buckets largest-first; under this skew the large buckets land first into a nearly empty pool (easy to place), and the many size-1/2 buckets placed last almost always drop into a remaining gap without triggering cuckoo eviction. A uniform distribution loses that second effect — with every bucket ~3.16 keys, the ones placed last still face a crowded pool and evict far more often.

```
localBucket = CubicEpsBucket(k1, 10000)       // k1 = LittleEndian(key[8:16])
```

```
function CubicEpsBucket(x: uint64, numBuckets: uint32) → uint32:
    if numBuckets <= 1: return 0
    x2     = Hi64(Mul128(x, x))               // x²
    xHalf  = (x >> 1) | (1 << 63)             // (1 + x) / 2 in fixed point
    cubic  = Hi64(Mul128(x2, xHalf))          // x² · (1 + x)/2
    scaled = (cubic / 256) × 255 + x / 256    // × 255/256 + x/256
    return fastRange32(scaled, numBuckets)
```

The cubic CDF `x²(1+x)/2` concentrates keys in the lowest-numbered buckets — bucket 0 gets ~1.08% of keys (~341 in a 31,600-key block). The `x/256` epsilon term guarantees every bucket gets at least a few keys, avoiding empty-bucket pathologies.

### 5.3. Pilot hash and slot computation

```
function PilotHash(pilot, globalSeed) → hp:
    x = 0x517cc1b727220a95 × (pilot ^ globalSeed)
    // SplitMix64 finalizer (Stafford "Mix13" variant)
    x ^= x >> 30
    x *= 0xbf58476d1ce4e5b9
    x ^= x >> 27
    x *= 0x94d049bb133111eb
    x ^= x >> 31
    return x | 1                              // odd ⇒ bijective multiply; also rules out hp = 0
```

```
function PilotSlot(k0, k1, pilot, numSlots, globalSeed) → slot:
    hp = PilotHash(pilot, globalSeed)
    h  = k0 ^ k1
    return fastRange32((h ^ (h >> 32)) × hp, numSlots)
```

The slot input (`k0 ^ k1`) and the multiply-mixing (`(h ^ (h>>32)) × hp`) are both forced by the small block size; §5.6 tabulates all five divergences from canonical PtrHash and why.

### 5.4. Remap table and metadata layout

Because `alpha < 1`, `numSlots > keysInBlock`. Slots in `[keysInBlock, numSlots)` are "overflow" slots that must be remapped into the holes left in `[0, keysInBlock)`.

```
Block metadata:
  Offset           Size                Content
  ────────────────────────────────────────────────────────
  0                bucketsPerBlock B   Pilots (1 byte per bucket)
  bucketsPerBlock  2 B                 RemapCount (uint16_le)
  +2               RemapCount × 2 B    Remap entries (uint16_le each)

At query time:
  if slot >= keysInBlock:
      slot = remapTable[slot - keysInBlock]   // entry i maps overflow slot keysInBlock+i
```

### 5.5. Query

```
function QuerySlotPTRHash(k0, k1, metadata, keysInBlock) → slot:
    bucket   = CubicEpsBucket(k1, 10000)
    pilot    = metadata[bucket]                        // direct byte read, O(1)
    numSlots = ceil(keysInBlock / alpha)               // alpha = 0.99 (§5.1)
    slot     = PilotSlot(k0, k1, pilot, numSlots, globalSeed)
    if slot >= keysInBlock:
        slot = lookupRemap(metadata[10000:], slot, keysInBlock)
    return slot
```

For the shipped PTRHash, `alpha` is the compile-time constant 0.99 and is **not** read from AlgoConfig. Fingerprint handling is at the framework level (§2.5).

### 5.6. Why this differs from canonical PtrHash

Canonical PtrHash builds one monolithic structure over *millions* of keys per part. StreamHash builds tiny, fixed blocks (~31,600 keys) so construction stays bounded-RAM and parallel. At that scale several of PtrHash's design choices become unreliable, so the adaptation diverges in five ways:

| Aspect | Canonical PtrHash | StreamHash | Reason |
|--------|-------------------|------------|--------|
| Slot mixing | XOR (`hx.low() ^ hp`) | multiply (`(h ^ h>>32) × hp`) | XOR can't break correlations in the ~341-key bucket; multiply can (see below) |
| Slot input | `hx.low()` (a hash half) | `k0 ^ k1` | StreamHash hashes no full key; the bucket comes from `k1` alone, so folding in the unused `k0` keeps the slot input well-spread across keys sharing a bucket (per-pair chance of becoming indistinguishable ~2⁻⁶⁴) |
| Pilot hash | `C × (pilot ^ seed)` | + SplitMix64 finalizer, then set the low bit (OR 1) | the raw `C × pilot` product leaves nearby pilots correlated at small block sizes; the finalizer fixes it (see below) |
| Block sizing | dynamic (Vigna formula, millions/part) | fixed 10,000 buckets (~31,600 keys) | streaming needs small, bounded, independently-solvable blocks |
| Key hashing | hash the full key upfront | use bytes 0–15 directly as `k0`, `k1` | the caller already guarantees uniform input (or pre-hashes), so 128 bits suffice — no hash on the hot path |

**Why these are linked.** Three of these rows — slot mixing, pilot hash, and the small fixed block size — are really one story. Streaming forces small fixed blocks; CubicEps then packs ~341 keys into bucket 0 (§5.2); and that one bucket must be solved within just 256 pilot values. As a first-order model, a single pilot makes the bucket internally collision-free with probability `p`, and if the 256 pilots acted as `K` *independent* trials the bucket would be unsolvable with probability `≈ (1 − p)^K`. (This sketches only the easy empty-pool phase — cuckoo eviction recovers some otherwise-failing pilots, so it is a conservative model, not the exact rate.) Two of canonical PtrHash's choices collapse the effective `K` at this scale, so StreamHash changes both:

- **XOR slot-mixing** makes collisions nearly pilot-invariant: `FastReduce` keys off a value's *top* bits, and XOR-ing both keys by the same `hp` cancels in that top-bit comparison, so a colliding pair tends to collide under every pilot — leaving only ~3–5 effectively-independent trials (measured). Multiplicative mixing has no such cancellation (`hp` enters non-linearly), restoring near-256 independent trials and making the largest bucket reliably solvable in a handful of pilot tries.
- **The raw `C × pilot` product**, over pilots 0–255, is an arithmetic progression that leaves nearby pilots sharing high-bit patterns. The SplitMix64 finalizer scrambles that residue so the 256 values behave near-independently — measured ~253–259 vs ~180–225 without — which carries the 1T-key build-failure rate from roughly 1-in-30 (no finalizer) to ~1-in-7,200.

Putting numbers on it (from the implementation's analysis): the *mean* ~341-key bucket-0, placed first into the empty ~31,920-slot pool, has single-pilot success `p ≈ 0.16`, so even at the achieved near-256 independent trials `(1 − p)^256 ≈ 2.5×10⁻²⁰`. The **per-block** failure probability is far larger — about `4.4×10⁻¹²` — because it integrates over the *distribution* of bucket-0 sizes, not just the mean. Per-bucket failure is exponentially sensitive to size, so the rare blocks whose bucket 0 runs well above 341 keys dominate the tail. That per-block `~4.4×10⁻¹²` is the figure that compounds with block count in §7.4; the fix when a build does fail is to retry with a different `globalSeed`.

At PtrHash's native million-key part sizes this barely bites: with parts that large, each bucket's failure probability is tiny relative to the slot pool even when only a few pilots act independently — which is exactly why the original algorithm never needed these changes.

## 6. Construction

The framework offers three build paths. All produce **byte-identical** output for the same keys, seed, and options (verified by the reference test suite's build-mode-equivalence tests).

- **Sorted** (`NewSortedBuilder`) — keys arrive in block-sorted prefix order; true streaming, lowest RAM, no temp disk.
- **Unsorted** (`NewUnsortedBuilder`) — keys arrive in any order; buffered to temp files, then read back in block order.
- **Parallel** (`WithWorkers(n)`) — available for both, building blocks concurrently.

All paths share the block model of §2: route each key, accumulate a block's keys, hand the block to the algorithm's solver, write the metadata, advance.

### 6.1. Sorted mode

Keys arrive in non-decreasing `blockIdx` order (a key whose block index is *less* than the previous one is rejected with `ErrUnsortedInput`). The builder accumulates keys for the current block; when `blockIdx` advances it solves and writes the finished block (emitting empty blocks for any skipped indices) and resets. Because only one block is ever in flight, a single-worker Bijection build runs in ~1 MB of RAM regardless of dataset size. Pre-sorted input needs no temp disk and reads the input once.

### 6.2. Unsorted mode

Unsorted input is handled in two phases with a clean memory hand-off: a **write phase** that partitions keys to temp files, and a **read phase** that reads them back in block order and builds. Write-phase buffers are freed before read-phase buffers are allocated, so peak RAM is the max of the two, not the sum.

**Partitions.** Blocks are grouped into `numPartitions` contiguous ranges of `blocksPerPart = ceil(numBlocks / numPartitions)` blocks each. For datasets up to ~30 billion keys this is a fixed 2,048 partitions; only beyond that does it grow to cap read-phase RAM at 2 GiB — the count balances two pressures, since fewer partitions mean larger, more efficient flush batches while more partitions keep read-phase memory bounded:

```
numReadSlots = readPipelineDepth(3) × slotsPerReader(2) = 6
readBound    = ceil(numReadSlots × N × 24 / 2 GiB)        // 24 = in-memory entry size
numPartitions = max(2048, readBound)                       // then capped at numBlocks
```

The `2048` floor binds for everything up to ~30 billion keys (and the `numBlocks` cap binds for small indexes); only past that does `readBound` grow the count to keep read-phase memory under the 2 GiB target. There is no fixed file-descriptor cap beyond `numBlocks`.

**Temp files are per-writer, not per-partition.** Each writer owns one file holding all `numPartitions` regions, region `P` at byte offset `P × regionSize`. A single-threaded `AddKey` build uses one writer; concurrent `AddKeys` gives each writer its own file so writers never synchronize. Each region is sized to absorb skew, so a single region rarely overflows; the formula follows the plain sizing rule below:

```
meanEntries      = N × blocksPerPart / numBlocks / numWriters
maxRegionEntries = ceil(meanEntries × 1.2 + 8 × sqrt(meanEntries))   // 1.2× writer-skew, 8σ Poisson
regionSize       = maxRegionEntries × (16 + PayloadSize)
```

All temp files are created up front, pre-allocated with `fallocate`, and **immediately unlinked** (`os.Remove`); I/O continues through the retained file descriptors, and the kernel reclaims the space when the last descriptor closes (the POSIX unlink-while-open guarantee). On any exit, including a crash, the data is freed automatically. *TempDir must be a local filesystem (ext4/xfs/btrfs); NFS is unsupported (it uses "silly rename" instead of true unlink-while-open), and tmpfs works but stores data in RAM.*

**Write phase — double-buffered flush.** Each writer keeps two flat in-memory buffers (~12 MB cap each: `maxFlushBufferBytes / 24 = 524,288` entries). A key is routed to its partition and appended; when a buffer fills, the writer swaps to its other buffer and a background goroutine encodes and `pwrite`s the full one, hiding flush latency behind continued ingestion. The on-disk entry is compact:

```
[k0: 8 B LE][k1: 8 B LE][payload: PayloadSize B LE]    // 16 + PayloadSize bytes
```

Neither the fingerprint nor the block ID is stored — both are recomputed on read-back (fingerprint from `k0/k1` via §2.5; block ID via `fastRange32(ReverseBytes64(k0), numBlocks)`). During the same encode pass the writer also tallies a per-block key count, which the read phase uses to place entries in one pass.

**Fast path.** Small builds skip all of the above: if a build never fills a buffer (fewer than ~524K keys at the default settings), nothing is flushed to disk — the builder counting-sorts the in-memory entries by block and builds directly, with no file I/O at all.

**Read phase — single-pass scatter.** Per-block counts were already tallied during the write phase, so reading a partition is a single pass: prefix-sum the counts into per-block offsets, then read each writer's region for that partition and scatter every entry straight into its final per-block slot. (There is no second "grouping" pass and no scratch copy — the counts make one pass sufficient.) The framework then feeds each block's entries to the solver in order.

Reading is pipelined: while blocks from one partition are being solved, the next partition is read in the background. The single-threaded finish uses two read slots; the parallel finish uses `readPipelineDepth × slotsPerReader = 6`, with reader goroutines stride-assigned (reader `r` handles partitions `r, r+N, …`) so the main thread consumes results in partition order.

**Resource usage** (bounded, scale-invariant):

- *Temp disk:* `N × (16 + PayloadSize)` bytes.
- *Write RAM:* two ~12 MB flat buffers per writer (~27 MB including headroom and the encode buffer), so it scales with worker count.
- *Read RAM:* bounded by a **2 GiB cap** on the sum of the read-pipeline buffers — the partition count is chosen to enforce it. Typical usage is far below the cap (≈100 MB at 1B keys); the cap only binds above ~30B keys. Plus a 4 MB streaming read buffer per reader.

### 6.3. Parallel mode

With `WithWorkers(n)`, both sorted and unsorted builds solve blocks concurrently. A pool of `n` worker goroutines each runs its own solver instance; a single **writer** goroutine reorders results and emits metadata in block order (and folds the payload hash-of-hashes, §3.7, in that order for determinism). Workers also write their payload slices directly via `pwrite` while the data is hot in cache.

Throughput scales near-linearly because the serialized step (writing metadata, folding hashes) is cheap relative to solving. The pipeline is carefully torn down on error or cancellation so a failing block, a writer-side I/O error, or a context cancel never deadlocks the pool — any internal failure cancels the worker context exactly once, releasing every parked goroutine.

### 6.4. Thread safety

- **Queries** are thread-safe and lock-free once the index is open: the data is immutable and each query reads an independent block. The RAM index must be fully loaded and visible before concurrent queries begin.
- **Solvers** are single-threaded; each parallel worker has its own.
- A **builder** is driven by one goroutine via `AddKey`, except `AddKeys`, which is explicitly multi-writer (each writer callback runs in its own goroutine with its own buffers).

---

## 7. Operations

### 7.1. Keys and collisions

Keys must be at least 16 bytes; the first 16 become `k0` and `k1`. There are two build-time collision concerns:

- **`ErrDuplicateKey`** — two keys the chosen algorithm cannot tell apart. The trigger differs by algorithm: **Bijection** raises it only for genuine full duplicates (identical `k0` *and* `k1`), a 128-bit coincidence that is negligible for uniformly random keys (~1.5×10⁻²¹ at 1B keys; see §8.1). **PTRHash** raises it when two keys in the same bucket share the slot input `k0 ^ k1` — a 64-bit coincidence, made rarer still by the same-bucket requirement, that also catches true duplicates. For structured input, pre-hash with xxHash3-128 (§8.1), which also makes the prefix uniform so blocks stay balanced.
- **`ErrIndistinguishableHashes`** (PTRHash only) — the solver exhausts all 256 pilots for a bucket without finding a viable placement, even though no two keys share `k0 ^ k1` (an unsolvable cuckoo configuration, not a true duplicate). Mitigated by block sizing and pilot independence (§5.6); retry with a different `globalSeed`. (An exact `k0 ^ k1` collision is reported as `ErrDuplicateKey` instead.)

### 7.2. Payload and fingerprint modes

| PayloadSize | FingerprintSize | Mode | Query returns |
|-------------|-----------------|------|---------------|
| 0 | 0 | Pure MPHF | rank (no verification) |
| 0 | >0 | MPHF + verification | rank, or `ErrNotFound` |
| >0 | 0 | Payload | payload (no verification) |
| >0 | >0 | Payload + verification | payload, or `ErrNotFound` |

A non-member passes the fingerprint check with probability `2^(−8 × FingerprintSize)` — its expected false-positive rate, not a flaw.

### 7.3. Dataset size limits

StreamHash targets large datasets (N > ~100K). For smaller sets, fixed costs (header, footer, RAM index) dominate the useful MPHF data. Bijection uses smaller blocks (~3,072 keys) than PTRHash (~31,600), so it produces more blocks for the same N.

| N | Bijection blocks | PTRHash blocks | Recommendation |
|---|------------------|----------------|----------------|
| < 1,000 | 2 | 2 | use a simpler structure |
| 1K–10K | 2–4 | 2 | high relative overhead |
| 10K–100K | 4–33 | 2–4 | reasonable for Bijection |
| > 100K | 33+ | 4+ | optimal |

**Maximum:** `2^40 − 1` keys (~1.1 trillion), set by the 40-bit RAM-index fields. The sentinel entry stores `KeysBefore == TotalKeys`, so exactly `2^40` would wrap to 0 and is rejected. The 40-bit `MetadataOffset` likewise supports metadata regions up to ~1.1 TB.

The resident RAM index is ≈ 10 bytes × numBlocks (≈ 3.3 MB per billion keys for Bijection, ≈ 0.3 MB per billion for PTRHash). At the `2^40 − 1` maximum this is ~3.6 GB (Bijection, ~358M blocks) or ~348 MB (PTRHash, ~34.8M blocks).

### 7.4. PTRHash build-failure frequency

PTRHash's per-block failure probability (~4.4×10⁻¹², §5.6) compounds with block count:

| Dataset | Blocks | Approx. failure rate |
|---------|--------|----------------------|
| 10M keys | ~317 | ~1 in 7×10⁸ (effectively never) |
| 1B keys | ~31,600 | ~1 in 7×10⁶ (rare) |
| 100B keys | ~3.16M | ~1 in 72,000 |
| 1T keys | ~31.6M | ~1 in 7,200 |

(1T keys is near the `2^40` ceiling of §7.3, so it is the largest dataset the format admits — the table does not assume anything larger.)

On `ErrIndistinguishableHashes`, retry with a new `globalSeed` — it re-randomizes all pilot hashes, and each retry fails independently, so a few retries always suffice. The framework cannot retry on its own (it does not own the key source), so above ~100B keys callers should wrap the build in a retry loop. Bijection has no such failure mode: its seed search is unbounded (large seeds spill to the fallback list).

### 7.5. Error reference

Error names are from the reference implementation; other implementations may differ.

**Construction:**

| Error | Condition |
|-------|-----------|
| `ErrEmptyIndex` | N = 0 |
| `ErrKeyTooShort` | key < 16 bytes |
| `ErrKeyTooLong` | key > 65,535 bytes |
| `ErrDuplicateKey` | Bijection: identical `k0` and `k1`. PTRHash: two keys in a bucket share `k0 ^ k1` (§7.1) |
| `ErrPayloadTooLarge` | PayloadSize > 8 |
| `ErrFingerprintTooLarge` | FingerprintSize > 4 |
| `ErrPayloadOverflow` | payload value exceeds PayloadSize capacity |
| `ErrKeyCountMismatch` | declared `totalKeys` ≠ keys added |
| `ErrTooManyKeys` | key count exceeds `2^40 − 1` |
| `ErrUnsortedInput` | out-of-order key (sorted mode) |
| `ErrSplitBucketSeedSearchFailed` | Bijection split-bucket search exhausted → retry with new `globalSeed` |
| `ErrIndistinguishableHashes` | PTRHash bucket unsolvable → retry with new `globalSeed` |

**Query / open:**

| Error | Condition |
|-------|-----------|
| `ErrInvalidMagic` | magic ≠ `0x53544D48` |
| `ErrInvalidVersion` | version ≠ `0x0001` |
| `ErrTruncatedFile` | file shorter than the layout requires |
| `ErrCorruptedIndex` | invalid metadata or out-of-bounds offset |
| `ErrChecksumFailed` | footer hash mismatch (from `Verify`) |
| `ErrNotFound` | empty block, or fingerprint mismatch |

### 7.6. Integrity and corruption safety

A reader **should** bounds-check before trusting an offset:

```
metadataStart = metadataRegionOffset + entry.MetadataOffset
if metadataStart >= metadataRegionOffset + metadataRegionSize:
    return ErrCorruptedIndex
```

For end-to-end integrity, the footer's two unseeded-xxHash64 region hashes (§3.7) can be verified after open (the reference implementation exposes this as `Verify`, returning `ErrChecksumFailed` on mismatch). **`Verify` covers only the payload and metadata regions, not the header or RAM index** — detecting tampering there requires a checksum stored **outside** the file (e.g. a manifest), since any in-file checksum would itself be part of what needs protecting.

### 7.7. Security considerations

StreamHash assumes uniformly random input. Consequences of violating that:

- **Non-uniform keys** cluster into blocks, overflowing temp-file regions (unsorted mode) — the region margin (§6.2) is calibrated for uniform keys. Pre-hash structured or correlated keys with xxHash3-128 (required, not optional).
- **Adversarial keys:** an attacker who knows `globalSeed` can craft keys that collide into one block. Mitigation: use a random `globalSeed` that untrusted sources cannot learn.
- **Fingerprints** are a probabilistic filter (false-positive rate `2^(−8 × FingerprintSize)`), not a cryptographic authenticator.

### 7.8. Durability and compatibility

- **Failed builds leave no usable index.** If `Finish` fails, the partial output file is deleted, so a crashed or errored build never leaves behind a file that looks like a valid index.
- **Single format version.** The format is Version `0x0001`; a reader rejects any other version with `ErrInvalidVersion`. There is no forward- or backward-compatibility guarantee across format versions yet.

---

## 8. Appendices

### 8.1. Pre-hash transformation

For non-random input, hash keys to a uniform 128-bit value before both building and querying. The reference uses xxHash3-128:

```
function PreHash(input) → 16 bytes:
    h = xxHash3_128(input)                       // unseeded
    return LittleEndian(h.low64) || LittleEndian(h.high64)
```

The output is the low 64 bits first, then the high 64 bits, each little-endian. 128 bits (not 64) is necessary because of the birthday bound:

| Keys | 64-bit collision P | 128-bit collision P |
|------|--------------------|---------------------|
| 100 million | ~0.03% | ≈ 0 |
| 1 billion | ~2.7% | ≈ 0 |
| 4 billion | ~35% | ≈ 0 |

A collision surfaces as `ErrDuplicateKey` at build time — a clean failure, never silent corruption.

### 8.2. Load factors and storage

**Bijection** — `lambda = 3.0` with split threshold 8 balances build speed against size:

| lambda | bits/key | Fallback rate | Split rate | |
|--------|----------|---------------|------------|---|
| 2.0 | ~2.8 | <0.01% | ~1% | conservative |
| 3.0 | ~2.46 | ~0.01% | ~1.2% | **default** |
| 3.6 | ~2.40 | ~0.02% | ~3% | |
| 5.0 | ~2.30 | ~0.3% | ~7% | aggressive |

**PTRHash** — `lambda = 3.16`, `alpha = 0.99` (~1% spare slots, absorbed by the remap table).

**Storage breakdown (MPHF mode, approximate contributions):**

*Bijection* (bucketsPerBlock 1024, lambda 3.0):

| Component | bits/key |
|-----------|----------|
| Elias-Fano (cumulative sizes) | ~1.17 |
| Seed stream (direct, sizes 2–7) | ~1.06 |
| Seed stream (split, sizes 8+) | ~0.12 |
| Checkpoints (28 B/block) | ~0.07 |
| Fallback list | ~0.01 |
| RAM index (10 B/block) | ~0.03 |
| **Total** | **~2.46** |

*PTRHash* (bucketsPerBlock 10,000, lambda 3.16, alpha 0.99):

| Component | bits/key |
|-----------|----------|
| Pilots (1 B × 10,000 / block) | ~2.53 |
| Remap table (~1% overflow) | ~0.16 |
| RAM index (10 B/block) | ~0.003 |
| **Total** | **~2.70** |

With payloads/fingerprints, total bits/key ≈ `routing_overhead + 8 × (PayloadSize + FingerprintSize)`, where `routing_overhead` is the MPHF figure above (~2.46 Bijection, ~2.70 PTRHash). For example, a 4-byte payload + 1-byte fingerprint on Bijection is ≈ `2.46 + 40 = ~42.5` bits/key.

### 8.3. Glossary

| Term | Definition |
|------|------------|
| **MPHF** | Minimal Perfect Hash Function — a collision-free, gap-free map from N keys to `[0, N)`. |
| **Bijection** | (1) a one-to-one mapping; (2) StreamHash's default algorithm (§4), which finds bijective seed→slot maps per bucket. |
| **prefix** | key bytes 0–7 as **big-endian** uint64; used for block routing. |
| **k0 / k1** | key bytes 0–7 / 8–15 as **little-endian** uint64; used by algorithms. |
| **Block** | a self-contained group of keys; one block's metadata is read per query. |
| **Bucket** | a small within-block group (~3 keys) that an algorithm solves independently. |
| **Slot** | a position in `[0, keysInBlock)`; the algorithm's output for a key. |
| **Rank** | the final MPHF output in `[0, N)`, = `keysBefore + slot`. |
| **Pilot / Seed** | the per-bucket search variable steering a bucket's keys to collision-free slots (a 0–255 "pilot" in PTRHash; an unbounded "seed" in Bijection). |
| **Global Seed** | 64-bit header value randomizing the whole index; change it to retry a failed build. The header `Seed` field (§3.2) and the `globalSeed` in pseudocode are the same value; Bijection's per-bucket `seed` (§4.3) is a different, unrelated quantity. |
| **lambda** | target average keys per bucket (3.0 Bijection, 3.16 PTRHash). |
| **alpha** | PTRHash load factor (0.99); `numSlots = ceil(keysInBlock / alpha)`. |
| **fastRange32** | near-bias-free map of a hash to `[0, n)`: high 64 bits of `hash × n` (exactly uniform only for power-of-two `n`). Weakly monotonic. |
| **Elias-Fano** | succinct encoding of a monotonic integer sequence (cumulative bucket sizes). |
| **Golomb-Rice** | variable-length code for geometrically distributed values (Bijection seeds). |
| **Checkpoints** | Bijection's 28-byte block header — seven Elias-Fano high-bits bit offsets followed by seven seed-stream bit offsets, captured at 128-bucket boundaries — bounding query decode to ≤128 buckets. |
| **Fallback list** | explicit storage for Bijection seeds too large to Golomb-Rice encode. |
| **Binary splitting** | one-level split of a Bijection bucket with ≥8 keys into two independently-solved halves. |
| **CubicEps** | PTRHash's skewed bucket distribution `x²(1+x)/2 × 255/256 + x/256`. |
| **Remap table** | PTRHash overflow table mapping slots ≥ keysInBlock into holes in `[0, keysInBlock)`. |
| **wymix** | WyHash v4 primitive: 128-bit multiply with XOR fold (`hi ^ lo`). |
| **SplitMix64** | the bit-mixing finalizer used in PTRHash's pilot hash (Stafford "Mix13" variant). |
| **Cuckoo hashing** | collision resolution by displacing already-placed items; used in PTRHash pilot search. |
| **Payload** | fixed-size value (1–8 bytes) stored per key. |
| **Fingerprint** | per-key bytes (1–4) for probabilistic non-member detection. |
| **UserMetadata / AlgoConfig** | variable-length application data / per-algorithm config after the header. |
| **Partition** | unsorted-mode unit: a contiguous range of blocks, with a region in each writer's temp file. |
| **Streaming construction** | building the index in RAM that does not grow with the dataset. |
