package streamhash

const (
	maxPayloadSize     = 8
	maxFingerprintSize = 4
)

// BuildOption is a functional option for configuring builds.
type BuildOption func(*buildConfig)

type buildConfig struct {
	workers         int
	payloadSize     int
	fingerprintSize int // in bytes
	globalSeed      uint64
	userMetadata    []byte
	unsortedTempDir string // temp directory for unsorted partition files
	algorithm       Algorithm

	totalKeys uint64 // Pre-known key count for single-pass builds
}

func defaultBuildConfig() *buildConfig {
	return &buildConfig{
		workers:    0,                  // Default to single-threaded; use WithWorkers(n) to parallelize
		globalSeed: 0x1234567890abcdef, // Arbitrary default; overridden via WithGlobalSeed
	}
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
	}
}

// WithFingerprint enables fingerprint verification. Size must be 0-4 bytes.
// 0 means no fingerprints (the default). With fingerprints enabled,
// QueryRank and QueryPayload return ErrNotFound for non-member keys.
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

// WithAlgorithm sets the block construction algorithm.
// Default is AlgoBijection.
func WithAlgorithm(algo Algorithm) BuildOption {
	return func(c *buildConfig) {
		c.algorithm = algo
	}
}
