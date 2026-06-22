# StreamHash

A Go library for building and querying **Minimal Perfect Hash Function (MPHF)**
indexes over billions of keys, using bounded RAM and streaming construction. An
MPHF maps N keys to N consecutive integers `[0, N)` with no collisions, giving
O(1) lookups without storing the keys themselves. The output is a single mmap'd
file; payloads and fingerprints are optional.

Two block algorithms (see `algorithm.go`):
- `AlgoBijection` — EF/GR encoding, compact (~2.5 bits/key), O(128) query.
- `AlgoPTRHash` — Cuckoo with 8-bit pilots, slightly larger (~2.7 bits/key), O(1) query.

Three build paths: `NewSortedBuilder` (sorted input), `NewUnsortedBuilder`
(unsorted, uses per-writer temp files), and parallel builds via `WithWorkers(n)`.

## Build & Test

- `go test ./...` — full suite.
- `go test -short ./...` — fast subset. **Note:** `-short` skips large-scale and
  the heaviest concurrency tests (`TestParallelBuilderStress`,
  `TestParallelBuilderConcurrentBuilds`, `TestRapidOpenClose`, …).
- `go test -race ./...` — run this for any change touching the builders. Do **not**
  pair it with `-short`: that would skip exactly the concurrency tests worth racing.
- `go vet ./...`, `gofmt -l .`, and `golangci-lint run` must all be clean — CI enforces them.

## Platform

`platform_linux.go`/`platform_darwin.go` use `golang.org/x/sys/unix` (for
`fallocate`/`fcntl`); `platform_other.go` (`!linux && !darwin`, incl. Windows)
uses `os.File.Truncate`. File I/O is portable `*os.File.WriteAt`/`ReadAt`, and
the unsorted builder's temp files use unlink-while-open on Unix and
`FILE_FLAG_DELETE_ON_CLOSE` on Windows. Builds and runs on Windows, but CI is
ubuntu + macos only — so Windows is compile-verified, not runtime-tested.

## Concurrency

The builders are heavily concurrent and are the main correctness risk:
- **Sorted parallel** (`builder_parallel.go`): an `errgroup` worker pool builds
  blocks; a single writer goroutine emits them in block order via channels.
- **Unsorted** (`builder_unsorted*.go`): concurrent writers each own a temp file
  (lock-free positional `WriteAt`); the finish phase uses a reader goroutine pool
  with per-slot fences.

Treat the race detector as a first-class gate, and keep per-test timeouts on the
channel/`errgroup` code so a deadlock fails the run instead of hanging it. If a
test genuinely times out only under `-race`, fix it or skip it under race with a
documented reason — never leave it flaky.

## CI

- `ci.yml` — build, gofmt, vet, tests (ubuntu+macos), full `-race`, golangci-lint,
  govulncheck. Runs on push to `main` and all PRs.
- `race-stress.yml` — deeper concurrency fuzzing on merge to `main` (and manual
  dispatch): full suite `-race -count=3`, plus the concurrency tests at
  `-count=20 -cpu=1,2,4` to sample many goroutine interleavings.
- `codeql.yml` — CodeQL security-and-quality, push/PR + weekly.
- `dependabot.yml` — weekly grouped gomod + actions updates.

## Performance Regression Testing

Use `cmd/bench` to check for regressions; run before and after a change and
compare "Build throughput" (M/sec).

```bash
# MPHF mode — Cuckoo solver only, no payload/fingerprint overhead.
go run ./cmd/bench -keys 10000000 -payload 0 -algo ptrhash -workers 1

# Default mode — full code path (4-byte payload, 1-byte fingerprint).
go run ./cmd/bench -keys 10000000 -algo ptrhash -workers 1
```

To compare against a baseline, run on `HEAD`, then `git stash && git checkout
<baseline>`, run again, and restore. **Run benchmarks serially** — never two at
once; they contend for CPU/memory bandwidth and produce noise. Do 5+ iterations
each; a difference under ~5% is within noise.

## Correctness Priorities

Correctness across the **full parameter space** is the top priority — not just the
default configuration. Code that only works for the common case is buggy.

- Every valid combination of payload size, fingerprint size, and algorithm must
  produce correct results.
- When a specialized/optimized path exists alongside the generic one, verify they
  produce **byte-identical** output.
- Watch for hardcoded assumptions about configurable sizes (e.g. `<< 8` assuming a
  1-byte field). These are silent for the default config and only break others.
- For any unbounded counter, analyze wrap-around and the consequence of a stale
  match; prefer a cheap guard over relying on probability.
- Before changing a function's behavior (adding a panic, removing a fallback), grep
  all callers including tests.

## What NOT to Change

- **Intentional hot-path duplication**: unrolled/specialized functions that
  eliminate branches — the duplication *is* the optimization.
- **Unsafe / bounds-check-free operations on query hot paths**, where correctness is
  guaranteed at a higher level. Don't add per-query validation that hurts throughput.
- Don't refactor performance-critical code for readability if it would regress
  performance; document the intent instead.
