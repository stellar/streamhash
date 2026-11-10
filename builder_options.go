package streamhash

import "context"

const (
	maxPayloadSize     = 8
	maxFingerprintSize = 4
)

// BuildOption is a functional option for configuring builds.
type BuildOption func(*buildConfig)

type buildConfig struct {
	workers         int
	payloadSize     int
	payloadSet      bool // WithPayload was given
	fingerprintSize int  // in bytes
	globalSeed      uint64
	userMetadata    []byte
	unsortedTempDir string // temp directory for unsorted partition files
	algorithm       Algorithm
	bytesPerSync    int // start writeback of the index every this many bytes written; 0 = off
	yield           func(context.Context) error

	totalKeys uint64 // Pre-known key count for single-pass builds
}

func defaultBuildConfig() *buildConfig {
	return &buildConfig{
		workers:    0,                  // Default to single-threaded; use WithWorkers(n) to parallelize
		globalSeed: 0x1234567890abcdef, // Arbitrary default; overridden via WithGlobalSeed
	}
}

// newBuildConfig applies opts to the default configuration.
func newBuildConfig(opts []BuildOption) *buildConfig {
	cfg := defaultBuildConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithWorkers sets the number of parallel workers for block building during Finish.
func WithWorkers(n int) BuildOption {
	return func(c *buildConfig) {
		c.workers = n
	}
}

// WithPayload configures payload storage. Size must be 0-8 bytes.
// 0 means no payload (MPHF-only mode, the default).
func WithPayload(sizeBytes int) BuildOption {
	return func(c *buildConfig) {
		c.payloadSize = sizeBytes
		c.payloadSet = true
	}
}

// WithFingerprint enables fingerprint verification. Size must be 0-4 bytes.
// 0 means no fingerprints (the default). With fingerprints enabled,
// QueryRank and QueryPayload return ErrNotFound for most non-member keys;
// streamhash-spec.md §7.2 says how many get through.
func WithFingerprint(sizeBytes int) BuildOption {
	return func(c *buildConfig) {
		c.fingerprintSize = sizeBytes
	}
}

// WithGlobalSeed sets the global hash seed.
func WithGlobalSeed(seed uint64) BuildOption {
	return func(c *buildConfig) {
		c.globalSeed = seed
	}
}

// WithMetadata sets the variable-length user metadata.
// The metadata is copied, so the caller can reuse the slice after this call.
func WithMetadata(data []byte) BuildOption {
	return func(c *buildConfig) {
		c.userMetadata = append([]byte(nil), data...) // Copy slice
	}
}

// WithBytesPerSync starts writeback of the index file every n bytes written
// (on Linux, sync_file_range without waiting), so a large build's dirty pages
// do not pile up and then hold up other writers' fsyncs on the same disk.
// 0, the default, leaves writeback to the kernel.
func WithBytesPerSync(n int) BuildOption {
	return func(c *buildConfig) {
		c.bytesPerSync = n
	}
}

// WithYield makes the build yield to other work: before each block, the
// goroutine about to build it calls yield and goes on when it returns. A
// yield that blocks holds the build back, and one that returns an error
// fails the build with it. The context is the build's, done once the build
// is canceled or has failed. Nothing is held while yield blocks: a build at
// rest holds only its buffers and its open files. The default never yields,
// and WithYield(nil) never yields: it is the same as not passing the option.
func WithYield(yield func(ctx context.Context) error) BuildOption {
	return func(c *buildConfig) {
		c.yield = yield
	}
}

// WithAlgorithm sets the block construction algorithm.
// Default is AlgoBijection.
func WithAlgorithm(algo Algorithm) BuildOption {
	return func(c *buildConfig) {
		c.algorithm = algo
	}
}
