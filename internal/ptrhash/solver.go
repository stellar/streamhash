package ptrhash

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"

	"github.com/stellar/streamhash/internal/sherr"
)

// errEvictionLimitExceeded is returned when the PTRHash solver exceeds the
// maximum allowed evictions. This is rare (<1 in 100,000 blocks) but can
// occur with pathological input. The caller should retry with a different
// random seed. This is an internal retry signal and is never user-facing.
var errEvictionLimitExceeded = fmt.Errorf("ptrhash solver: eviction limit exceeded")

// PTRHash solver.
//
// Key optimizations from PTRHash (https://github.com/jeroen-mostert/ptrhash):
//  1. Two-phase pilot search: Phase 1 finds collision-free pilot (fast path ~80%),
//     Phase 2 finds pilot with minimal eviction cost
//  2. Eviction chains with bucket heap: Evicted buckets go on a max-heap ordered by size
//  3. 16-bucket pinning: Circular buffer prevents eviction cycles
//  4. Bucket-size-squared scoring: Prefer evicting smaller buckets
//  5. Seeded pilot start in Phase 2: a per-attempt PRNG seeded from the global
//     seed varies the pilot scan start to distribute evictions and avoid
//     deterministic cycles, while keeping the build reproducible (see solve)
//  6. Eviction limit: Abort if evictions exceed 10 x numSlots
//  7. Bitvector for taken slots: O(1) lookup, 8x less memory than bool array
//  8. Counting sort: O(n) bucket ordering vs heap O(n log n)
//  9. Chunked 4-slot checking: Reduces branch mispredictions by 3x
//  10. Specialized small bucket dispatch: Separate code paths for sizes 1-8

const (
	// pinnedSize is the size of the circular buffer for cycle prevention.
	// Prevents re-evicting buckets that were just placed.
	pinnedSize = 16

	// maxEvictionMultiplier limits total evictions to prevent infinite loops.
	// If evictions exceed maxEvictionMultiplier x numSlots, we retry with new seed.
	maxEvictionMultiplier = 10

	// bitsPerWord is the number of bits per uint64 word in bitvector operations.
	bitsPerWord = 64

	// minBufferAlloc is the minimum allocation to amortize overhead.
	minBufferAlloc = 16
)

// Cuckoo remap: The remap table is a []uint16 where remapTable[i] maps
// overflow slot (numKeys + i) to a hole in [0, numKeys). This provides
// O(1) direct indexing at query time instead of O(n) linear search.

// solver holds state for solving a single block.
type solver struct {
	// Geometry
	numBuckets uint32
	numSlots   uint32
	numKeys    int
	globalSeed uint64 // For suffix-based slot computation

	// Pre-computed pilot hash values (avoids recomputing SplitMix64 finalizer per call)
	pilotHPs [numPilotValues]uint64

	// Bucket data (direct reference, no copy - aligned with bijection solver)
	buckets      [][]bucketEntry // Reference to block builder's buckets (not copied)
	bucketStarts []uint16        // Cumulative key counts for bucket ordering (len = numBuckets+1)
	bucketOrder  []uint16        // Bucket indices sorted by size (largest first)

	// Solving state
	// Both slotGen and processedGen share the same generation counter
	// but are independent: slotGen[i] tracks slot occupancy, processedGen[i]
	// tracks whether bucket i has been assigned a pilot this block.
	pilots       []uint8  // Output: pilot per bucket (8-bit)
	slotOwner    []uint16 // slot -> bucket index (only valid if slotGen[slot] == generation)
	slotGen      []uint8  // Generation when each slot was last written
	processedGen []uint8  // Generation when bucket was processed (== generation means processed)
	generation   uint8    // Current generation (incremented each block for O(1) clearing)

	// Phase 2 duplicate detection (separate from slot tracking to avoid corruption)
	phase2SlotGen []uint32 // Separate generation tracking for Phase 2 duplicate detection
	phase2Gen     uint32   // Incremented each Phase 2 check - no cleanup needed

	// Eviction tracking
	pinned     [pinnedSize]int16 // Circular buffer of recently-placed buckets (-1 = empty)
	pinnedBits []uint64          // Bitvector for O(1) isPinned check
	pinnedIdx  int               // Current position in pinned buffer
	evictions  int               // Total eviction count

	// Reusable buffers (avoid allocation in hot path)
	slotsBuffer   []uint16    // Temp buffer for slot computation (max bucket size)
	foldedBuffer  []uint64    // Temp buffer for precomputed h_folded values (max bucket size)
	pendingHeap   *bucketHeap // Reusable heap for eviction processing
	bestSlots     []uint16    // Reusable buffer for best slots in Phase 2
	evictedOwners []uint16    // Reusable buffer for evicted bucket owners
	remapBuffer   []uint16    // Reusable buffer for remap table (numSlots - numKeys)

	// Counting sort buffers (reusable)
	sortCounts    []int // Count occurrences of each bucket size
	sortPositions []int // Position for each bucket size

	// Buffer capacity tracking (for reuse across blocks)
	maxNumBuckets uint32
	maxNumSlots   uint32
}

// bucketSize returns the number of keys in bucket idx.
func (s *solver) bucketSize(idx int) int {
	return len(s.buckets[idx])
}

// bucketKeys returns the bucket entries for bucket idx (direct reference, no copy).
func (s *solver) bucketKeys(idx int) []bucketEntry {
	return s.buckets[idx]
}

// isProcessed returns true if bucket has been processed this generation.
func (s *solver) isProcessed(bucketIdx int) bool {
	return s.processedGen[bucketIdx] == s.generation
}

// setProcessed marks bucket as processed (generation-based).
func (s *solver) setProcessed(bucketIdx int) {
	s.processedGen[bucketIdx] = s.generation
}

// clearProcessed marks bucket as unprocessed (for eviction).
func (s *solver) clearProcessed(bucketIdx int) {
	s.processedGen[bucketIdx] = 0 // Any value != s.generation means unprocessed
}

// getOwner returns the bucket that owns slot, or -1 if the slot is free.
// This is the safe way to read slotOwner with generation-based clearing.
func (s *solver) getOwner(slot uint32) int {
	if s.slotGen[slot] != s.generation {
		return -1
	}
	return int(s.slotOwner[slot])
}

// isPinned returns true if bucket is in the recent placement buffer.
// O(1) using bitvector lookup.
func (s *solver) isPinned(bucketIdx int) bool {
	return s.pinnedBits[bucketIdx/bitsPerWord]&(1<<(bucketIdx%bitsPerWord)) != 0
}

// pin adds bucket to the recent placement buffer.
// Maintains both the circular buffer (for FIFO eviction) and bitvector (for O(1) lookup).
func (s *solver) pin(bucketIdx int) {
	// Clear bit for bucket being evicted from circular buffer
	oldBucket := s.pinned[s.pinnedIdx]
	if oldBucket >= 0 {
		s.pinnedBits[int(oldBucket)/bitsPerWord] &^= 1 << (int(oldBucket) % bitsPerWord)
	}

	// Add new bucket to circular buffer and set its bit
	s.pinned[s.pinnedIdx] = int16(bucketIdx)
	s.pinnedBits[bucketIdx/bitsPerWord] |= 1 << (bucketIdx % bitsPerWord)
	s.pinnedIdx = (s.pinnedIdx + 1) % pinnedSize
}

// splitMix64 is a fast, well-distributed finalizer used to derive deterministic
// RNG seeds from the global seed (see solve).
func splitMix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

// solve assigns pilots to all buckets and builds the remap table.
// Returns (pilots, remapEntries, error).
// Returns errEvictionLimitExceeded if eviction limit is hit (caller should retry).
func (s *solver) solve(attempt int) ([]uint8, []uint16, error) {
	if s.numKeys == 0 {
		// All buckets empty - return zero pilots
		return s.pilots, nil, nil
	}

	// Slot indices are stored as uint16 (keys map into [0,numSlots) and the
	// remap table is []uint16). numSlots is bounded far below this today (peak
	// ~33k for the largest block under the enforced 2^40 key ceiling), but
	// enforce the invariant explicitly so a future change to block sizing fails
	// cleanly instead of silently truncating slot indices into wrong answers.
	if s.numSlots > math.MaxUint16 {
		return nil, nil, fmt.Errorf("%w: block needs %d slots, exceeds the uint16 slot limit %d",
			sherr.ErrBlockOverflow, s.numSlots, math.MaxUint16)
	}

	// Deterministic per-attempt eviction RNG. Seed from the global seed so
	// WithGlobalSeed controls the build and output is reproducible and
	// byte-identical across build paths; mix in the retry attempt so each retry
	// still explores different Phase-2 pilot start offsets (preserving the
	// original retry behavior). This RNG only perturbs tie-breaking / eviction
	// order — the global seed used for slot computation is unchanged, so the
	// produced pilots remain valid for the decoder.
	seedBase := splitMix64(s.globalSeed ^ ((uint64(attempt) + 1) * 0x9E3779B97F4A7C15))
	rng := rand.New(rand.NewPCG(seedBase, splitMix64(seedBase)))
	maxEvictions := maxEvictionMultiplier * int(s.numSlots)

	// Use pre-allocated buffers (clear for reuse)
	s.pendingHeap.clear()
	s.bestSlots = s.bestSlots[:0]
	s.evictedOwners = s.evictedOwners[:0]

	// Process buckets in sorted order (largest first).
	// Direct iteration - heap is only used if evictions occur.
	evictionLimitHit := false
	for _, bucketIdx16 := range s.bucketOrder[:s.numBuckets] {
		bucketIdx := int(bucketIdx16)
		if s.bucketSize(bucketIdx) == 0 {
			// Empty bucket: explicitly write pilot=0 (avoids bulk zeroing in reset)
			s.pilots[bucketIdx] = 0
			s.setProcessed(bucketIdx)
			continue
		}
		if s.isProcessed(bucketIdx) {
			continue
		}

		if err := s.processBucketWithHeap(bucketIdx, rng, &s.bestSlots, &s.evictedOwners, s.pendingHeap); err != nil {
			return nil, nil, err
		}

		// If evictions occurred, drain the heap before continuing
		for s.pendingHeap.len() > 0 {
			currentIdx, _ := s.pendingHeap.pop()
			if s.isProcessed(currentIdx) {
				continue
			}
			if err := s.processBucketWithHeap(currentIdx, rng, &s.bestSlots, &s.evictedOwners, s.pendingHeap); err != nil {
				return nil, nil, err
			}
			if s.evictions > maxEvictions {
				evictionLimitHit = true
				break
			}
		}

		if s.evictions > maxEvictions {
			evictionLimitHit = true
			break
		}
	}

	if evictionLimitHit {
		return nil, nil, errEvictionLimitExceeded
	}

	// Build remap table for any slots >= numKeys (alpha overflow)
	remap := s.buildRemap()

	return s.pilots, remap, nil
}

// processBucketWithHeap processes a single bucket using Phase 1 (fast path) or Phase 2 (eviction).
// Returns sherr.ErrIndistinguishableHashes if no valid pilot can be found.
func (s *solver) processBucketWithHeap(bucketIdx int, rng *rand.Rand, bestSlots *[]uint16, evictedOwners *[]uint16, pendingHeap *bucketHeap) error {
	bucketSize := s.bucketSize(bucketIdx)
	bucket := s.bucketKeys(bucketIdx)

	// Phase 1: Try to find collision-free pilot (fast path)
	// findFreePilot writes slots to s.slotsBuffer on success, so we can use placeBucket directly
	pilot, ok := s.findFreePilot(bucket, bucketSize)
	if ok {
		s.placeBucket(bucketIdx, pilot, s.slotsBuffer[:bucketSize])
		s.setProcessed(bucketIdx)
		return nil
	}

	// Phase 2: Find pilot with minimal eviction cost
	// PtrHash order: compute collision score first, then check for internal duplicates.
	// Early exit when score <= bucketSize^2 (best possible: single collision with same-size bucket).
	p0 := uint8(rng.IntN(numPilotValues))
	bestPilot := uint8(0)
	bestScore := math.MaxInt
	*bestSlots = (*bestSlots)[:0]

	slots := s.slotsBuffer[:bucketSize]
	minPossibleScore := bucketSize * bucketSize

	// Precompute folded hash values for Phase 2 pilot loop.
	folded := s.foldedBuffer[:bucketSize]
	for i, entry := range bucket {
		folded[i] = foldSlotInput(entry.k0, entry.suffix)
	}

	for delta := range numPilotValues {
		pilot := uint8((int(p0) + delta) % numPilotValues)

		// First: compute all slots using precomputed folded values
		for i := range slots {
			slots[i] = uint16(pilotSlotFolded(folded[i], s.pilotHPs[pilot], s.numSlots))
		}

		// Second: compute collision score
		// Early break when score already exceeds bestScore (optimization from Rust PTRHash)
		score := 0
		viable := true
		for _, slot := range slots[:bucketSize] {
			owner := s.getOwner(uint32(slot))
			if owner < 0 {
				continue
			}
			if s.isPinned(owner) {
				viable = false
				break
			}
			ownerSize := s.bucketSize(owner)
			score += ownerSize * ownerSize
			// Early break: no point continuing if we already exceed best score
			if score >= bestScore {
				viable = false
				break
			}
		}

		if !viable {
			continue
		}

		// Third: check for internal duplicates
		if !s.hasNoDuplicateSlotsPhase2(slots[:bucketSize]) {
			continue
		}

		if score < bestScore {
			bestPilot = pilot
			bestScore = score
			*bestSlots = append((*bestSlots)[:0], slots...)
			if score <= minPossibleScore {
				break
			}
		}
	}

	if len(*bestSlots) == 0 {
		// Check if failure is due to duplicate keys (same k0 ^ suffix).
		// Duplicate keys produce identical slots for all pilots, so the solver
		// is mathematically guaranteed to reach this error path.
		if hasDuplicateSlotInput(bucket) {
			return sherr.ErrDuplicateKey
		}
		// Find bucket size distribution for debugging
		maxSize := 0
		for i := 0; i < int(s.numBuckets); i++ {
			if s.bucketSize(i) > maxSize {
				maxSize = s.bucketSize(i)
			}
		}
		return fmt.Errorf("%w: bucket=%d size=%d maxBucketSize=%d numSlots=%d",
			sherr.ErrIndistinguishableHashes, bucketIdx, bucketSize, maxSize, s.numSlots)
	}

	// Evict conflicting buckets
	*evictedOwners = (*evictedOwners)[:0]
	for _, slot := range *bestSlots {
		owner := s.getOwner(uint32(slot))
		if owner >= 0 && owner != bucketIdx {
			found := false
			owner16 := uint16(owner)
			if slices.Contains(*evictedOwners, owner16) {
				found = true
			}
			if !found {
				*evictedOwners = append(*evictedOwners, owner16)
			}
		}
	}

	for _, owner16 := range *evictedOwners {
		owner := int(owner16)
		s.evictBucket(owner)
		s.clearProcessed(owner)
		pendingHeap.push(owner, s.bucketSize(owner)) // Use heap instead of slice
		s.evictions++
	}

	s.placeBucket(bucketIdx, bestPilot, *bestSlots)
	s.pin(bucketIdx)
	s.setProcessed(bucketIdx)
	return nil
}

// placeBucket assigns a pilot to a bucket and marks its slots as taken.
// gen and slotGen are hoisted into locals to avoid repeated struct field loads;
// the same pattern is used in evictBucket, buildRemap, and the findFreePilotN
// functions (which additionally use unsafe pointer access — see slotGenIsTaken).
func (s *solver) placeBucket(bucketIdx int, pilot uint8, slots []uint16) {
	s.pilots[bucketIdx] = pilot
	gen := s.generation
	slotGen := s.slotGen
	for _, slot := range slots {
		s.slotOwner[slot] = uint16(bucketIdx)
		slotGen[slot] = gen
	}
}

// evictBucket removes a bucket's slots from the taken set.
func (s *solver) evictBucket(bucketIdx int) {
	pilot := s.pilots[bucketIdx]
	bucket := s.bucketKeys(bucketIdx)
	hp := s.pilotHPs[pilot]
	gen := s.generation
	slotGen := s.slotGen

	for _, entry := range bucket {
		hFolded := foldSlotInput(entry.k0, entry.suffix)
		slot := pilotSlotFolded(hFolded, hp, s.numSlots)
		// Only clear slots still owned by this bucket. A slot may have been
		// reassigned to a different bucket during a prior eviction in the
		// same processBucketWithHeap call.
		if slotGen[slot] == gen && s.slotOwner[slot] == uint16(bucketIdx) {
			slotGen[slot] = 0 // Makes slot logically free
		}
	}
}

// buildRemap builds the remap table for overflow slots.
// Slots >= numKeys need to be remapped to [0, numKeys).
// Reuses s.remapBuffer to avoid allocation.
func (s *solver) buildRemap() []uint16 {
	numKeys := uint32(s.numKeys)
	numSlots := s.numSlots

	// Table size = S - N (~1% of keys at alpha=0.99)
	// We must allocate the full range to allow O(1) direct indexing (slot - N).
	remapSize := int(numSlots - numKeys)

	// Grow buffer if needed
	if cap(s.remapBuffer) < remapSize {
		s.remapBuffer = make([]uint16, remapSize)
	} else {
		s.remapBuffer = s.remapBuffer[:remapSize]
		// Zero the buffer (required: unset entries must be 0)
		clear(s.remapBuffer)
	}

	// Cursor to find holes in the valid range [0, N)
	holeCursor := uint32(0)

	// Hoist slotGen and generation into locals for the linear scan.
	slotGen := s.slotGen
	gen := s.generation

	// Iterate ONLY over the Overflow Range [N, S)
	for overflowSlot := numKeys; overflowSlot < numSlots; overflowSlot++ {
		// Optimization: If the solver didn't put a key here, skip it.
		// remapBuffer[i] is already 0 (cleared above or from make).
		// Since no key hashes to this pilot/slot combo, this 0 will never be read.
		if slotGen[overflowSlot] != gen {
			continue
		}

		// Find next hole in valid range
		for slotGen[holeCursor] == gen {
			holeCursor++
			if holeCursor >= numKeys {
				// This implies we have more keys than slots, which violates invariants
				panic("ptrhash buildRemap: out of holes in valid range")
			}
		}

		// Map the Overflow Slot -> The Hole
		s.remapBuffer[overflowSlot-numKeys] = uint16(holeCursor)

		// Advance cursor so we don't reuse this hole
		holeCursor++
	}

	return s.remapBuffer
}

// newSolver creates a solver with buffers pre-allocated for maxKeys.
func newSolver(maxNumBuckets int, maxNumKeys int) *solver {
	maxNumSlots := computeNumSlots(maxNumKeys)

	pinnedBitsWords := (uint32(maxNumBuckets) + bitsPerWord - 1) / bitsPerWord

	var pinned [pinnedSize]int16
	for i := range pinned {
		pinned[i] = -1
	}

	// Initial buffer size estimate (will be grown in reset() if actual max bucket size exceeds this)
	maxBucketSize := max(int(math.Ceil(lambda*3)),
		// Floor to avoid degenerate small allocations
		minBufferAlloc)

	// Heap rarely holds more than ~10% of buckets (evictions are rare)
	heapCapacity := max(maxNumBuckets/10,
		// Floor to avoid degenerate small allocations
		minBufferAlloc)

	// Pre-allocate remap buffer: size is numSlots - numKeys ~= 1% of keys at alpha=0.99
	maxRemapSize := max(int(maxNumSlots)-maxNumKeys, 0)

	return &solver{
		bucketStarts:  make([]uint16, maxNumBuckets+1),
		bucketOrder:   make([]uint16, maxNumBuckets),
		pilots:        make([]uint8, maxNumBuckets),
		slotOwner:     make([]uint16, maxNumSlots),
		slotGen:       make([]uint8, maxNumSlots),
		generation:    1, // Start at 1 so generation 0 means "not taken"
		processedGen:  make([]uint8, maxNumBuckets),
		pinned:        pinned,
		pinnedBits:    make([]uint64, pinnedBitsWords),
		slotsBuffer:   make([]uint16, maxBucketSize),
		foldedBuffer:  make([]uint64, maxBucketSize),
		pendingHeap:   newBucketHeap(heapCapacity),
		bestSlots:     make([]uint16, 0, maxBucketSize),
		evictedOwners: make([]uint16, 0, minBufferAlloc),
		remapBuffer:   make([]uint16, maxRemapSize),
		sortCounts:    make([]int, maxBucketSize+1),
		sortPositions: make([]int, maxBucketSize+1),
		maxNumBuckets: uint32(maxNumBuckets),
		maxNumSlots:   maxNumSlots,
	}
}

// reset prepares the solver for a new block.
// Stores reference to buckets directly (no copy) - aligned with bijection solver.
// If pilotsDst is provided, pilots are written directly there (zero-copy to destination buffer).
// If pilotsDst is nil, uses internal pilots buffer.
func (s *solver) reset(buckets [][]bucketEntry, numKeys int, globalSeed uint64, pilotsDst []byte) {
	numBuckets := uint32(len(buckets))
	s.globalSeed = globalSeed

	// Compute numSlots
	numSlots := computeNumSlots(numKeys)

	s.numBuckets = numBuckets
	s.numSlots = numSlots
	s.numKeys = numKeys
	s.evictions = 0
	s.pinnedIdx = 0

	// Precompute pilot hash values (avoids SplitMix64 per call in hot loops)
	initPilotHPs(&s.pilotHPs, globalSeed)

	// Store reference to buckets directly (no copy!)
	s.buckets = buckets

	// Ensure buffers are large enough for this block
	if numSlots > s.maxNumSlots {
		s.maxNumSlots = numSlots
		s.slotOwner = make([]uint16, numSlots)
		s.slotGen = make([]uint8, numSlots)
	}
	if numBuckets > s.maxNumBuckets {
		s.maxNumBuckets = numBuckets
		s.bucketStarts = make([]uint16, numBuckets+1)
		s.bucketOrder = make([]uint16, numBuckets)
		// Only allocate internal pilots buffer if not using direct destination
		if pilotsDst == nil {
			s.pilots = make([]uint8, numBuckets)
		}
		s.processedGen = make([]uint8, numBuckets)
		pinnedBitsWords := (numBuckets + bitsPerWord - 1) / bitsPerWord
		s.pinnedBits = make([]uint64, pinnedBitsWords)
	}

	// Use direct destination or internal buffer for pilots
	if pilotsDst != nil {
		s.pilots = pilotsDst[:numBuckets]
	} else if len(s.pilots) < int(numBuckets) {
		s.pilots = make([]uint8, numBuckets)
	}

	// Build bucket starts (for counting sort) and find max bucket size
	totalKeys := 0
	maxBucketSize := 0
	for i, bucket := range buckets {
		s.bucketStarts[i] = uint16(totalKeys)
		totalKeys += len(bucket)
		if len(bucket) > maxBucketSize {
			maxBucketSize = len(bucket)
		}
	}
	s.bucketStarts[numBuckets] = uint16(totalKeys)

	// Ensure slotsBuffer and foldedBuffer are large enough
	if maxBucketSize > len(s.slotsBuffer) {
		s.slotsBuffer = make([]uint16, maxBucketSize)
		s.foldedBuffer = make([]uint64, maxBucketSize)
	}

	// Ensure phase2SlotGen is large enough (numSlots size for O(1) duplicate detection)
	if int(numSlots) > len(s.phase2SlotGen) {
		s.phase2SlotGen = make([]uint32, numSlots)
	}

	// Ensure sort buffers are large enough for counting sort
	requiredSortLen := maxBucketSize + 1
	if requiredSortLen > len(s.sortCounts) {
		s.sortCounts = make([]int, requiredSortLen)
		s.sortPositions = make([]int, requiredSortLen)
	}

	// Build bucket order using counting sort (reuses bucketOrder slice and sort buffers)
	countingSortBucketsInto(s.bucketStarts[:numBuckets+1], s.bucketOrder[:numBuckets], s.sortCounts, s.sortPositions)

	// O(1) clearing: increment generation instead of clearing slot arrays.
	// All slots with slotGen[slot] != s.generation are considered free.
	// Both slotGen and processedGen use this same generation counter.
	s.generation++
	// Handle overflow: generation is uint8. hasNoDuplicateSlots uses
	// s.generation+1 as a temporary marker, so the in-use generation must stay
	// below 255. Resetting at >= 254 keeps it <= 253 (so generation+1 <= 254,
	// never wrapping to 0). Both slotGen and processedGen must be cleared to
	// prevent stale matches from ~253 blocks ago when the generation last held
	// this value.
	if s.generation >= 254 {
		s.generation = 1
		clear(s.slotGen)
		clear(s.processedGen[:numBuckets])
	}

	// Note: Pilots are written during solve(). Empty buckets explicitly get pilot=0.
	// No bulk zeroing needed - saves expensive buffer writes.

	// Reset pinned buffer (fixed size 16)
	for i := range s.pinned {
		s.pinned[i] = -1
	}

	// Clear pinned bits
	pinnedBitsWords := (numBuckets + bitsPerWord - 1) / bitsPerWord
	clear(s.pinnedBits[:pinnedBitsWords])
}
