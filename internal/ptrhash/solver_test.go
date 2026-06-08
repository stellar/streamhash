package ptrhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"testing"

	"github.com/stellar/streamhash/internal/sherr"
)

// Named seeds for deterministic reproduction.
const (
	testSeed1 = 0x1234567890ABCDEF
	testSeed2 = 0xFEDCBA9876543210
)

// pilotSlotFromHashes is a test helper that computes slot from both hash halves.
// This is the reference implementation combining pilotHash + foldSlotInput + pilotSlotFolded.
// Production code uses precomputed pilotHPs tables instead.
func pilotSlotFromHashes(k0, k1 uint64, pilot uint8, numSlots uint32, globalSeed uint64) uint32 {
	hp := pilotHash(pilot, globalSeed)
	hFolded := foldSlotInput(k0, k1)
	return pilotSlotFolded(hFolded, hp, numSlots)
}

// Test-only helpers for generation-based slot tracking.
// These are used by tests to set up solver state; production code uses
// direct field access with hoisted locals for performance.

func (s *solver) isTaken(slot uint32) bool {
	return s.slotGen[slot] == s.generation
}

func (s *solver) setTaken(slot uint32) {
	s.slotGen[slot] = s.generation
}

func (s *solver) clearTaken(slot uint32) {
	s.slotGen[slot] = 0
}

func newTestRNG(t testing.TB) *rand.Rand {
	t.Helper()
	h := fnv.New128a()
	h.Write([]byte(t.Name()))
	sum := h.Sum(nil)
	s1 := binary.LittleEndian.Uint64(sum[:8])
	s2 := binary.LittleEndian.Uint64(sum[8:])
	return rand.New(rand.NewPCG(testSeed1^s1, testSeed2^s2))
}

// createSolverWithBuckets sets up a solver with the given bucket sizes.
func createSolverWithBuckets(rng *rand.Rand, bucketSizes []int) (*solver, [][]bucketEntry) {
	numKeys := 0
	for _, s := range bucketSizes {
		numKeys += s
	}

	buckets := make([][]bucketEntry, len(bucketSizes))
	for i, size := range bucketSizes {
		buckets[i] = make([]bucketEntry, size)
		for j := range size {
			buckets[i][j] = bucketEntry{
				suffix: rng.Uint64(),
				k0:     rng.Uint64(),
			}
		}
	}

	s := newSolver(len(bucketSizes), numKeys)
	return s, buckets
}

// solveWithRetry attempts to solve with retries on eviction limit.
func solveWithRetry(t *testing.T, rng *rand.Rand, s *solver, buckets [][]bucketEntry, numKeys int, maxAttempts int) ([]uint8, []uint16) {
	t.Helper()

	for range maxAttempts {
		globalSeed := rng.Uint64()
		pilotsDst := make([]byte, len(buckets))
		s.reset(buckets, numKeys, globalSeed, pilotsDst)

		pilots, remap, err := s.solve(0)
		if err == nil {
			return pilots, remap
		}
		// errEvictionLimitExceeded -> retry with different seed
	}
	t.Fatalf("solver failed after %d attempts", maxAttempts)
	return nil, nil
}

const testGlobalSeed = uint64(0xA5A5A5A5A5A5A5A5)

// TestSolverBasic tests that the solver works for a simple case.
func TestSolverBasic(t *testing.T) {
	numBuckets := 10
	numKeys := numBuckets // One key per bucket
	rs := newSolver(numBuckets, numKeys)

	// Create simple test data - one key per bucket with random-like suffix values
	rng := newTestRNG(t)
	buckets := make([][]bucketEntry, numBuckets)
	for i := range numBuckets {
		buckets[i] = []bucketEntry{{k0: rng.Uint64(), suffix: rng.Uint64()}}
	}

	nSlots := int(computeNumSlots(numKeys))
	pilotsDst := make([]byte, numBuckets)
	rs.reset(buckets, numKeys, testGlobalSeed, pilotsDst)

	pilots, _, err := rs.solve(0)
	if err != nil {
		t.Fatalf("solve failed: %v", err)
	}

	if len(pilots) != numBuckets {
		t.Errorf("pilots length = %d, want %d", len(pilots), numBuckets)
	}

	// Verify all slots are unique
	slotUsed := make(map[uint32]bool)
	for bi, bucket := range buckets {
		if len(bucket) == 0 {
			continue
		}
		pilot := pilots[bi]
		slot := pilotSlotFromHashes(bucket[0].k0, bucket[0].suffix, pilot, uint32(nSlots), testGlobalSeed)
		if slotUsed[slot] {
			t.Errorf("slot %d collision at bucket %d", slot, bi)
		}
		slotUsed[slot] = true
	}
}

// TestDuplicateKeyDetection tests that the solver detects duplicate keys.
func TestDuplicateKeyDetection(t *testing.T) {
	testCases := []struct {
		name        string
		bucketSizes []int // Each bucket with the given sizes
		dupBucket   int   // Bucket containing duplicates
		dupCount    int   // Number of duplicate keys (same suffix) in that bucket
	}{
		{"size2_2dups", []int{2}, 0, 2},
		{"size3_2dups", []int{3}, 0, 2},
		{"size4_2dups", []int{4}, 0, 2},
		{"size5_2dups", []int{5}, 0, 2},
		{"size6_2dups", []int{6}, 0, 2},
		{"size7_2dups", []int{7}, 0, 2},
		{"size8_2dups", []int{8}, 0, 2},
		{"size10_2dups", []int{10}, 0, 2},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			numBuckets := len(tc.bucketSizes)
			numKeys := 0
			for _, s := range tc.bucketSizes {
				numKeys += s
			}
			rs := newSolver(numBuckets, numKeys)

			// Create buckets with unique suffixes, except for duplicates
			buckets := make([][]bucketEntry, numBuckets)
			dupRNG := newTestRNG(t)
			for i, size := range tc.bucketSizes {
				buckets[i] = make([]bucketEntry, size)
				for j := range size {
					if i == tc.dupBucket && j < tc.dupCount {
						// Use the same k0 and suffix for duplicates
						buckets[i][j] = bucketEntry{k0: 0xCAFEBABE12345678, suffix: 0xDEADBEEFDEADBEEF}
					} else {
						buckets[i][j] = bucketEntry{k0: dupRNG.Uint64(), suffix: dupRNG.Uint64()}
					}
				}
			}

			pilotsDst := make([]byte, numBuckets)
			rs.reset(buckets, numKeys, testGlobalSeed, pilotsDst)

			_, _, err := rs.solve(0)
			if !errors.Is(err, sherr.ErrDuplicateKey) {
				t.Errorf("expected sherr.ErrDuplicateKey, got: %v", err)
			}
		})
	}
}

// TestDuplicateKeyMissedWhenPilot1SlotsTaken verifies that duplicate key detection
// works even when the slots at pilot=1 are already taken by other buckets.
//
// This is a regression test for a bug where the duplicate check at pilot=1
// was skipped if anyTaken was true, causing duplicates to be reported as
// ErrIndistinguishableHashes instead of sherr.ErrDuplicateKey.
func TestDuplicateKeyMissedWhenPilot1SlotsTaken(t *testing.T) {
	// Create a single bucket with 2 duplicate keys (same k0 and suffix)
	numBuckets := 1
	numKeys := 2
	rs := newSolver(numBuckets, numKeys)

	// Duplicate entries - same k0 and suffix means they're indistinguishable
	k0 := uint64(0x1234567890ABCDEF)
	suffix := uint64(0xFEDCBA0987654321)
	buckets := [][]bucketEntry{
		{
			{k0: k0, suffix: suffix},
			{k0: k0, suffix: suffix}, // duplicate
		},
	}

	pilotsDst := make([]byte, numBuckets)
	rs.reset(buckets, numKeys, testGlobalSeed, pilotsDst)

	// Compute the slot that these duplicates would use at pilot=1
	numSlots := rs.numSlots
	slot1 := pilotSlotFromHashes(k0, suffix, 1, numSlots, testGlobalSeed)

	// Pre-mark that slot as taken to force anyTaken=true at pilot=1
	// This simulates another bucket having already claimed this slot
	rs.setTaken(slot1)

	_, _, err := rs.solve(0)

	// With the bug: returns ErrIndistinguishableHashes (duplicate check skipped)
	// With the fix: returns sherr.ErrDuplicateKey
	if !errors.Is(err, sherr.ErrDuplicateKey) {
		t.Errorf("expected sherr.ErrDuplicateKey, got: %v", err)
	}
}

// TestNoDuplicatesNoError tests that the solver doesn't falsely detect duplicates.
func TestNoDuplicatesNoError(t *testing.T) {
	// Use a realistic distribution similar to production (cubic bucket distribution)
	// Many small buckets (1-2 keys), fewer large buckets
	numBuckets := 50
	bucketSizes := []int{
		1, 1, 1, 1, 1, 1, 1, 1, 1, 1, // 10 buckets with 1 key
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 10 buckets with 2 keys
		2, 2, 2, 2, 2, 2, 2, 2, 2, 2, // 10 more buckets with 2 keys
		3, 3, 3, 3, 3, 3, 3, 3, 3, 3, // 10 buckets with 3 keys
		4, 4, 4, 4, 4, 5, 5, 5, 6, 6, // 10 buckets with 4-6 keys
	}
	if len(bucketSizes) != numBuckets {
		t.Fatalf("bucketSizes length mismatch: got %d, want %d", len(bucketSizes), numBuckets)
	}

	numKeys := 0
	for _, s := range bucketSizes {
		numKeys += s
	}
	rs := newSolver(numBuckets, numKeys)

	// Create buckets with all unique suffixes
	buckets := make([][]bucketEntry, numBuckets)
	noDupRNG := newTestRNG(t)
	for i, size := range bucketSizes {
		buckets[i] = make([]bucketEntry, size)
		for j := range size {
			buckets[i][j] = bucketEntry{k0: noDupRNG.Uint64(), suffix: noDupRNG.Uint64()}
		}
	}

	// Verify all suffixes are unique
	suffixSet := make(map[uint64]bool)
	for _, bucket := range buckets {
		for _, entry := range bucket {
			if suffixSet[entry.suffix] {
				t.Fatalf("test setup error: duplicate suffix %x", entry.suffix)
			}
			suffixSet[entry.suffix] = true
		}
	}

	// Retry loop like production
	var err error
	for attempt := range 10 {
		pilotsDst := make([]byte, numBuckets)
		rs.reset(buckets, numKeys, testGlobalSeed+uint64(attempt), pilotsDst)
		_, _, err = rs.solve(0)
		if err == nil {
			return // Success
		}
		if errors.Is(err, sherr.ErrDuplicateKey) {
			t.Logf("attempt %d: falsely detected duplicate key with seed %x", attempt, testGlobalSeed+uint64(attempt))
			t.Fatalf("falsely detected duplicate key - this should never happen with unique suffixes")
		}
		// Other errors (eviction limit, indistinguishable hashes) are expected with some seeds
	}
	t.Fatalf("solve failed after 10 attempts: %v", err)
}

// TestFindFreePilotEquivalence verifies that specialized findFreePilot{N}
// and findFreePilotSlice produce the same pilot value and slot contents.
// Size 1 excluded: findFreePilot1 is trivial (single entry can't have duplicates).
func TestFindFreePilotEquivalence(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 100

	for size := 2; size <= 8; size++ {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			for i := range iterations {
				// Generate random bucket entries
				globalSeed := rng.Uint64()
				entries := make([]bucketEntry, size)
				for j := range entries {
					entries[j] = bucketEntry{
						suffix: rng.Uint64(),
						k0:     rng.Uint64(),
					}
				}

				// Need enough slots for the bucket
				numKeys := size * 4 // generous
				numSlots := computeNumSlots(numKeys)

				// Create TWO solver instances with identical state
				s1 := newSolver(1, numKeys)
				s2 := newSolver(1, numKeys)

				// Build matching bucket layouts
				buckets1 := make([][]bucketEntry, 1)
				buckets1[0] = make([]bucketEntry, size)
				copy(buckets1[0], entries)

				buckets2 := make([][]bucketEntry, 1)
				buckets2[0] = make([]bucketEntry, size)
				copy(buckets2[0], entries)

				// Use pilotsDst buffers
				pilots1 := make([]byte, 1)
				pilots2 := make([]byte, 1)

				s1.reset(buckets1, numKeys, globalSeed, pilots1)
				s2.reset(buckets2, numKeys, globalSeed, pilots2)

				// Mark some random slots as taken on both
				numTaken := int(rng.IntN(int(numSlots) / 3))
				for range numTaken {
					slot := uint32(rng.IntN(int(numSlots)))
					s1.setTaken(slot)
					s2.setTaken(slot)
				}

				// Call specialized on s1
				pilot1, ok1 := s1.findFreePilot(buckets1[0], size)

				// Call slice variant on s2
				pilot2, ok2 := s2.findFreePilotSlice(buckets2[0])

				// Both should agree on whether a free pilot was found
				if ok1 != ok2 {
					t.Fatalf("iter %d size %d: ok mismatch: specialized=%v slice=%v",
						i, size, ok1, ok2)
				}
				if !ok1 {
					continue // both failed to find free pilot
				}

				// Both should find the same pilot
				if pilot1 != pilot2 {
					t.Fatalf("iter %d size %d: pilot mismatch: specialized=%d slice=%d",
						i, size, pilot1, pilot2)
				}

				// Verify the slots match
				for j := 0; j < size; j++ {
					if s1.slotsBuffer[j] != s2.slotsBuffer[j] {
						t.Fatalf("iter %d size %d: slot[%d] mismatch: specialized=%d slice=%d",
							i, size, j, s1.slotsBuffer[j], s2.slotsBuffer[j])
					}
				}
			}
		})
	}
}

// TestBuildRemapCompleteness verifies remap produces unique slots in [0, numKeys).
// Uses the production pilotSlotFromHashes and lookupRemap functions rather than
// reimplementing their logic.
func TestBuildRemapCompleteness(t *testing.T) {
	const iterations = 10
	rng := newTestRNG(t)

	for i := range iterations {
		// Create realistic bucket distribution
		numBuckets := 20 + int(rng.IntN(30))
		bucketSizes := make([]int, numBuckets)
		numKeys := 0
		for j := range bucketSizes {
			bucketSizes[j] = 1 + int(rng.IntN(5))
			numKeys += bucketSizes[j]
		}

		s, buckets := createSolverWithBuckets(rng, bucketSizes)
		pilots, remap := solveWithRetry(t, rng, s, buckets, numKeys, 20)

		numSlots := computeNumSlots(numKeys)
		remapData := encodeRemapTable(remap)

		// Build final slot assignment using production functions
		finalSlots := make(map[uint32]bool)
		for bucketIdx, pilot := range pilots {
			for _, entry := range buckets[bucketIdx] {
				slot := pilotSlotFromHashes(entry.k0, entry.suffix, pilot, numSlots, s.globalSeed)
				finalSlot := lookupRemap(remapData, slot, uint32(numKeys))
				if finalSlots[finalSlot] {
					t.Fatalf("iter %d: duplicate final slot %d", i, finalSlot)
				}
				finalSlots[finalSlot] = true
			}
		}

		// Verify all slots in [0, numKeys)
		for slot := range finalSlots {
			if int(slot) >= numKeys {
				t.Errorf("iter %d: final slot %d >= numKeys %d", i, slot, numKeys)
			}
		}
		if len(finalSlots) != numKeys {
			t.Errorf("iter %d: got %d unique slots, want %d", i, len(finalSlots), numKeys)
		}
	}
}

// TestGenerationWrapAround verifies that generation wraps correctly
// after 253+ resets and slot tracking remains valid.
func TestGenerationWrapAround(t *testing.T) {
	numBuckets := 5
	bucketSizes := []int{2, 2, 2, 2, 2}
	numKeys := 10

	rng := newTestRNG(t)
	s, buckets := createSolverWithBuckets(rng, bucketSizes)

	for reset := range 260 {
		globalSeed := rng.Uint64()
		pilotsDst := make([]byte, numBuckets)
		s.reset(buckets, numKeys, globalSeed, pilotsDst)

		// After reset: no slot should be taken
		numSlots := computeNumSlots(numKeys)
		for slot := range numSlots {
			if s.isTaken(slot) {
				t.Fatalf("reset %d: slot %d falsely taken after reset", reset, slot)
			}
		}

		// setTaken / isTaken should work
		testSlot := uint32(reset % int(numSlots))
		s.setTaken(testSlot)
		if !s.isTaken(testSlot) {
			t.Fatalf("reset %d: slot %d not taken after setTaken", reset, testSlot)
		}

		// clearTaken should work
		s.clearTaken(testSlot)
		if s.isTaken(testSlot) {
			t.Fatalf("reset %d: slot %d still taken after clearTaken", reset, testSlot)
		}
	}
}

// TestGenerationWrapAroundSolve verifies that the solver produces correct
// results even after generation wraps around past 255. This is a stronger
// version of TestGenerationWrapAround that actually solves blocks rather
// than just testing setTaken/isTaken/clearTaken.
func TestGenerationWrapAroundSolve(t *testing.T) {
	numBuckets := 5
	bucketSizes := []int{2, 2, 2, 2, 2}
	numKeys := 10

	rng := newTestRNG(t)
	s, buckets := createSolverWithBuckets(rng, bucketSizes)

	// Solve 270 blocks, forcing generation to wrap past 255.
	// Verify every successful solve produces valid results.
	successCount := 0
	for reset := range 270 {
		globalSeed := rng.Uint64()
		pilotsDst := make([]byte, numBuckets)
		s.reset(buckets, numKeys, globalSeed, pilotsDst)

		pilots, remap, err := s.solve(0)
		if err != nil {
			// Eviction limit or indistinguishable hashes can happen with some seeds.
			// This is normal — just skip that seed.
			continue
		}
		successCount++

		// Verify all slots are unique for every successful solve
		numSlots := computeNumSlots(numKeys)
		remapData := encodeRemapTable(remap)
		finalSlots := make(map[uint32]bool)
		for bucketIdx, pilot := range pilots {
			for _, entry := range buckets[bucketIdx] {
				slot := pilotSlotFromHashes(entry.k0, entry.suffix, pilot, numSlots, globalSeed)
				finalSlot := lookupRemap(remapData, slot, uint32(numKeys))
				if finalSlots[finalSlot] {
					t.Fatalf("reset %d: duplicate final slot %d", reset, finalSlot)
				}
				finalSlots[finalSlot] = true
			}
		}
		if len(finalSlots) != numKeys {
			t.Fatalf("reset %d: got %d unique slots, want %d", reset, len(finalSlots), numKeys)
		}
	}
	if successCount == 0 {
		t.Fatal("no successful solves in 270 attempts")
	}
}

// TestSolverPinning verifies the pinning circular buffer behavior.
func TestSolverPinning(t *testing.T) {
	numBuckets := 32
	bucketSizes := make([]int, numBuckets)
	numKeys := 0
	for j := range bucketSizes {
		bucketSizes[j] = 1
		numKeys++
	}

	rng := newTestRNG(t)
	s, buckets := createSolverWithBuckets(rng, bucketSizes)
	pilotsDst := make([]byte, numBuckets)
	s.reset(buckets, numKeys, 42, pilotsDst)

	// Pin pinnedSize buckets
	for i := range pinnedSize {
		s.pin(i)
		if !s.isPinned(i) {
			t.Fatalf("bucket %d not pinned after pin()", i)
		}
	}

	// All pinnedSize buckets should be pinned
	for i := range pinnedSize {
		if !s.isPinned(i) {
			t.Fatalf("bucket %d not pinned (all slots full)", i)
		}
	}

	// Pin one more — oldest (bucket 0) should be evicted
	s.pin(pinnedSize)
	if !s.isPinned(pinnedSize) {
		t.Fatal("new bucket not pinned after evicting oldest")
	}
	if s.isPinned(0) {
		t.Fatal("bucket 0 should have been evicted (FIFO)")
	}

	// Continue pinning through full rotation
	for i := pinnedSize + 1; i < pinnedSize*3; i++ {
		bucketIdx := i % numBuckets
		s.pin(bucketIdx)
		if !s.isPinned(bucketIdx) {
			t.Fatalf("bucket %d not pinned after pin()", bucketIdx)
		}
	}
}

// TestDuplicateKeyDetectionAllSizes verifies that duplicate keys are
// detected for all bucket sizes 2-16 via the solver's error path.
func TestDuplicateKeyDetectionAllSizes(t *testing.T) {
	for size := 2; size <= 16; size++ {
		t.Run(fmt.Sprintf("size=%d", size), func(t *testing.T) {
			globalSeed := uint64(0xDEADBEEF)
			numKeys := size * 4

			s := newSolver(1, numKeys)

			// Create bucket with duplicate keys
			entries := make([]bucketEntry, size)
			dupK0 := uint64(0x1111111111111111)
			dupK1 := uint64(0x2222222222222222)
			for j := range entries {
				entries[j] = bucketEntry{
					suffix: dupK1,
					k0:     dupK0,
				}
			}

			buckets := [][]bucketEntry{entries}
			pilotsDst := make([]byte, 1)
			s.reset(buckets, numKeys, globalSeed, pilotsDst)

			_, _, err := s.solve(0)
			if !errors.Is(err, sherr.ErrDuplicateKey) {
				t.Errorf("size %d: expected ErrDuplicateKey, got %v", size, err)
			}
		})
	}
}

// TestSolverPhase2Eviction verifies that Phase 2 eviction is exercised during
// solving. Uses a Poisson-like bucket distribution (many small + some large)
// which reliably triggers evictions on the first attempt.
func TestSolverPhase2Eviction(t *testing.T) {
	rng := newTestRNG(t)

	// Generate a Poisson-like bucket distribution with ~2000 keys.
	// This matches the natural lambda≈3.2 distribution the solver is designed for,
	// and reliably produces evictions (typically 30-60 per solve).
	const targetKeys = 2000
	numBuckets := targetKeys * 10 / 32 // ~625 buckets for lambda≈3.2
	bucketSizes := make([]int, numBuckets)
	for range targetKeys {
		bucketSizes[rng.IntN(numBuckets)]++
	}
	// Remove empty buckets
	var nonEmpty []int
	for _, s := range bucketSizes {
		if s > 0 {
			nonEmpty = append(nonEmpty, s)
		}
	}
	bucketSizes = nonEmpty

	numKeys := 0
	for _, s := range bucketSizes {
		numKeys += s
	}

	s, buckets := createSolverWithBuckets(rng, bucketSizes)

	const maxAttempts = 50
	for attempt := range maxAttempts {
		globalSeed := rng.Uint64()
		pilotsDst := make([]byte, len(bucketSizes))
		s.reset(buckets, numKeys, globalSeed, pilotsDst)

		pilots, _, err := s.solve(0)
		if err != nil {
			// Eviction limit or indistinguishable hashes — retry with different seed.
			continue
		}

		// Successful solve — verify evictions were exercised.
		if s.evictions == 0 {
			t.Fatal("solve succeeded but evictions == 0; expected Phase 2 to be exercised")
		}
		t.Logf("solve succeeded on attempt %d with %d evictions (numKeys=%d, numBuckets=%d)",
			attempt, s.evictions, numKeys, len(bucketSizes))

		// Verify each key's slot is unique via the pilot assignment.
		numSlots := computeNumSlots(numKeys)
		slotSet := make(map[uint32]bool, numKeys)
		for bi, pilot := range pilots {
			for _, entry := range buckets[bi] {
				slot := pilotSlotFromHashes(entry.k0, entry.suffix, pilot, numSlots, globalSeed)
				if slotSet[slot] {
					t.Fatalf("duplicate slot %d for bucket %d", slot, bi)
				}
				slotSet[slot] = true
			}
		}
		if len(slotSet) != numKeys {
			t.Fatalf("expected %d unique slots, got %d", numKeys, len(slotSet))
		}

		return // success
	}
	t.Fatalf("no successful solve in %d attempts", maxAttempts)
}
