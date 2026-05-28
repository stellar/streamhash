package streamhash

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
)

// TestVerificationHelpers self-tests the verification helpers to ensure they
// catch real problems and don't silently accept bad data.
func TestVerificationHelpers(t *testing.T) {
	t.Run("ValidMPHF", func(t *testing.T) {
		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, 100, 24)
		slices.SortFunc(keys, bytes.Compare)

		idx := buildAndOpen(t, keys, nil)
		defer idx.Close()

		verifyMPHF(t, idx, keys)
	})

	t.Run("PayloadRoundTrip", func(t *testing.T) {
		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, 50, 24)
		payloads := make([]uint64, len(keys))
		for i := range payloads {
			payloads[i] = uint64(i * 1000)
		}
		// Sort keys and payloads together so payloads track their keys
		sortKeysAndPayloads(keys, payloads)

		idx := buildAndOpen(t, keys, payloads, WithPayload(4))
		defer idx.Close()

		verifyPayloads(t, idx, keys, payloads, 4)
	})

	t.Run("NonMemberRejection", func(t *testing.T) {
		rng := newTestRNG(t)
		keys := generateRandomKeys(rng, 100, 24)
		slices.SortFunc(keys, bytes.Compare)

		idx := buildAndOpen(t, keys, nil, WithFingerprint(2))
		defer idx.Close()

		verifyNonMemberRejection(t, rng, idx, 1000)
	})
}

// TestIntegrationMatrix is the parameterized integration test that replaces ~100
// individual tests. It exercises all combinations of:
//   - Algorithm: {bijection, ptrhash}
//   - Build mode: {sorted, unsorted, parallel}
//   - Key size: {16, 24}
//   - Data mode: 6 representative payload/fingerprint combos
//   - Scale: {100, 5000} + {50000 when !testing.Short()}
func TestIntegrationMatrix(t *testing.T) {
	type dataMode struct {
		name    string
		payload int
		fp      int
	}

	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	buildModes := []struct {
		name     string
		unsorted bool
		opts     func(t *testing.T) []BuildOption
	}{
		{"sorted", false, func(t *testing.T) []BuildOption { return nil }},
		{"unsorted", true, func(t *testing.T) []BuildOption { return nil }},
		{"parallel", false, func(t *testing.T) []BuildOption { return []BuildOption{WithWorkers(4)} }},
	}

	keySizes := []int{16, 24}

	dataModes := []dataMode{
		{"p0_fp0", 0, 0}, // MPHF only
		{"p4_fp0", 4, 0}, // payload only, fast path (entrySize=4)
		{"p4_fp1", 4, 1}, // payload+fp, fast path (entrySize=5)
		{"p4_fp4", 4, 4}, // payload+fp, fast path (entrySize=8)
		{"p2_fp1", 2, 1}, // generic path (entrySize=3)
		{"p6_fp3", 6, 3}, // generic path (entrySize=9)
	}

	scales := []int{100, 5000}
	if !testing.Short() {
		scales = append(scales, 50000)
	}

	for _, algo := range algorithms {
		for _, bm := range buildModes {
			for _, ks := range keySizes {
				for _, dm := range dataModes {
					for _, n := range scales {
						name := fmt.Sprintf("%s/%s/k%d/%s/N%d", algo.name, bm.name, ks, dm.name, n)
						algo, bm, ks, dm, n := algo, bm, ks, dm, n // capture

						t.Run(name, func(t *testing.T) {
							t.Parallel()

							// Generate random keys
							rng := newTestRNG(t)
							keys := generateRandomKeys(rng, n, ks)

							// Generate payloads
							var payloads []uint64
							if dm.payload > 0 {
								payloads = make([]uint64, n)
								for i := range payloads {
									payloads[i] = uint64(i)
								}
							}

							// Build options
							var opts []BuildOption
							opts = append(opts, WithAlgorithm(algo.algo))
							if dm.payload > 0 {
								opts = append(opts, WithPayload(dm.payload))
							}
							if dm.fp > 0 {
								opts = append(opts, WithFingerprint(dm.fp))
							}

							bmOpts := bm.opts(t)
							opts = append(opts, bmOpts...)

							// Build and open
							var idx *Index
							if bm.unsorted {
								idx = buildAndOpenUnsorted(t, keys, payloads, t.TempDir(), opts...)
							} else {
								sortKeysAndPayloads(keys, payloads)
								idx = buildAndOpen(t, keys, payloads, opts...)
							}
							defer idx.Close()

							// Verify MPHF: unique ranks in [0, N)
							verifyMPHF(t, idx, keys)

							// Verify payloads
							if dm.payload > 0 {
								verifyPayloads(t, idx, keys, payloads, dm.payload)
							}

							// Verify non-member rejection (only with fingerprint)
							if dm.fp > 0 {
								verifyNonMemberRejection(t, rng, idx, 1000)
							}

							// Verify integrity
							if err := idx.Verify(); err != nil {
								t.Errorf("Verify() error: %v", err)
							}
						})
					}
				}
			}
		}
	}
}

// TestIntegrationMatrixClustered exercises empty block handling, degenerate splits,
// and large-bucket fallback paths using clustered keys.
func TestIntegrationMatrixClustered(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping clustered matrix in short mode")
	}

	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	buildModes := []struct {
		name     string
		unsorted bool
		opts     func(t *testing.T) []BuildOption
	}{
		{"sorted", false, func(t *testing.T) []BuildOption { return nil }},
		{"unsorted", true, func(t *testing.T) []BuildOption { return nil }},
		{"parallel", false, func(t *testing.T) []BuildOption { return []BuildOption{WithWorkers(4)} }},
	}

	type dataMode struct {
		name    string
		payload int
		fp      int
	}

	dataModes := []dataMode{
		{"p0_fp0", 0, 0},
		{"p4_fp1", 4, 1},
		{"p6_fp3", 6, 3},
	}

	n := 5000

	for _, algo := range algorithms {
		for _, bm := range buildModes {
			for _, dm := range dataModes {
				name := fmt.Sprintf("%s/%s/%s", algo.name, bm.name, dm.name)
				algo, bm, dm := algo, bm, dm

				t.Run(name, func(t *testing.T) {
					t.Parallel()

					// Generate clustered keys: ~4 clusters with empty blocks between
					rng := newTestRNG(t)
					keys := make([][]byte, n)
					for i := range keys {
						keys[i] = make([]byte, 24)
						// Force keys into ~4 prefix clusters
						cluster := uint64(i%4) * 0x4000000000000000
						binary.BigEndian.PutUint64(keys[i][0:8], cluster+uint64(i/4))
						fillFromRNG(rng, keys[i][8:])
					}

					var payloads []uint64
					if dm.payload > 0 {
						payloads = make([]uint64, n)
						for i := range payloads {
							payloads[i] = uint64(i)
						}
					}

					var opts []BuildOption
					opts = append(opts, WithAlgorithm(algo.algo))
					if dm.payload > 0 {
						opts = append(opts, WithPayload(dm.payload))
					}
					if dm.fp > 0 {
						opts = append(opts, WithFingerprint(dm.fp))
					}
					opts = append(opts, bm.opts(t)...)

					var idx *Index
					if bm.unsorted {
						idx = buildAndOpenUnsorted(t, keys, payloads, t.TempDir(), opts...)
					} else {
						sortKeysAndPayloads(keys, payloads)
						idx = buildAndOpen(t, keys, payloads, opts...)
					}
					defer idx.Close()

					verifyMPHF(t, idx, keys)
					if dm.payload > 0 {
						verifyPayloads(t, idx, keys, payloads, dm.payload)
					}
					if dm.fp > 0 {
						verifyNonMemberRejection(t, rng, idx, 1000)
					}
					if err := idx.Verify(); err != nil {
						t.Errorf("Verify() error: %v", err)
					}
				})
			}
		}
	}
}

// TestAllPayloadFingerprintCombos exhaustively tests all 45 valid payload/fingerprint
// combinations to catch encoding bugs for uncommon sizes.
func TestAllPayloadFingerprintCombos(t *testing.T) {
	algorithms := []struct {
		name string
		algo Algorithm
	}{
		{"bijection", AlgoBijection},
		{"ptrhash", AlgoPTRHash},
	}

	n := 100

	for _, algo := range algorithms {
		for payload := 0; payload <= 8; payload++ {
			for fp := 0; fp <= 4; fp++ {
				name := fmt.Sprintf("%s/p%d_fp%d", algo.name, payload, fp)
				algo, payload, fp := algo, payload, fp

				t.Run(name, func(t *testing.T) {
					t.Parallel()

					rng := newTestRNG(t)
					keys := generateRandomKeys(rng, n, 24)
					var payloads []uint64
					if payload > 0 {
						payloads = make([]uint64, n)
						for i := range payloads {
							payloads[i] = uint64(i)
						}
					}

					var opts []BuildOption
					opts = append(opts, WithAlgorithm(algo.algo))
					if payload > 0 {
						opts = append(opts, WithPayload(payload))
					}
					if fp > 0 {
						opts = append(opts, WithFingerprint(fp))
					}

					sortKeysAndPayloads(keys, payloads)

					idx := buildAndOpen(t, keys, payloads, opts...)
					defer idx.Close()

					verifyMPHF(t, idx, keys)
					if payload > 0 {
						verifyPayloads(t, idx, keys, payloads, payload)
					}
					if fp > 0 {
						verifyNonMemberRejection(t, rng, idx, 100)
					}

					if err := idx.Verify(); err != nil {
						t.Errorf("Verify() error: %v", err)
					}
				})
			}
		}
	}
}
