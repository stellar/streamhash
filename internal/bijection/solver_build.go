package bijection

import (
	"github.com/tamirms/streamhash/internal/sherr"
)

func (bb *Builder) solveBijections() error {
	// Clear state here defensively — needed for test paths that call solveBijections directly without Reset()
	bb.splitSeeds = bb.splitSeeds[:0]
	bb.fallbackList = bb.fallbackList[:0]

	for i := range bb.splitSeedIdx {
		bb.splitSeedIdx[i] = -1
	}

	for bucketIdx, bucket := range bb.buckets {
		size := len(bucket)
		if size <= 1 {
			bb.seeds[bucketIdx] = 0
			continue
		}

		if size >= splitThreshold {
			seed0, seed1, err := bb.solveSplitBucket(bucketIdx, bucket)
			if err != nil {
				return err
			}

			bb.splitSeedIdx[bucketIdx] = len(bb.splitSeeds)
			bb.splitSeeds = append(bb.splitSeeds, seed0, seed1)
		} else {
			seed, needsFallback := bb.solveDirectBucket(bucket, size)

			if needsFallback {
				var err error
				seed, err = bb.solveExtended(bucket, size)
				if err != nil {
					return err
				}
				bb.fallbackList = append(bb.fallbackList, fallbackEntry{
					bucketIndex: uint16(bucketIdx),
					subBucket:   0,
					seed:        seed,
				})
				bb.seeds[bucketIdx] = fallbackMarker
			} else {
				bb.seeds[bucketIdx] = seed
			}
		}
	}

	return nil
}

func (bb *Builder) solveSplitBucket(bucketIdx int, bucket []bucketEntry) (uint32, uint32, error) {
	bucketSize := len(bucket)
	splitPoint := bucketSize / 2

	if len(bb.slots) < bucketSize {
		bb.slots = make([]bool, bucketSize*2)
	}

	// Half of maxExtendedSeedForB(blockBits) = 1<<21. Split seed search is more
	// expensive per iteration (must verify both halves), so budget is halved.
	const splitBucketMaxSeed = 1 << 20
	maxSeed := uint32(splitBucketMaxSeed)

	var seed0 uint32
	var foundSeed0 bool

	for seed := range maxSeed {
		firstHalfSlots := bb.slots[:splitPoint]
		clear(firstHalfSlots)

		bb.subBucket0 = bb.subBucket0[:0]
		bb.subBucket1 = bb.subBucket1[:0]

		valid := true
		for _, entry := range bucket {
			h := mixFromParts(entry.mixParts, seed, uint32(bucketSize))
			if h < uint32(splitPoint) {
				if firstHalfSlots[h] {
					valid = false
					break
				}
				firstHalfSlots[h] = true
				bb.subBucket0 = append(bb.subBucket0, entry)
			} else {
				bb.subBucket1 = append(bb.subBucket1, entry)
			}
		}

		if valid && len(bb.subBucket0) == splitPoint {
			seed0 = seed
			foundSeed0 = true
			break
		}
	}

	if !foundSeed0 {
		// A duplicate key (identical mixParts) in the first half can never be
		// split into distinct slots, so the search above exhausts. Distinguish
		// that genuine duplicate from a true seed-search exhaustion; the
		// second-half case is already reported via solveExtended below.
		if hasDuplicateMixParts(bucket) {
			return 0, 0, sherr.ErrDuplicateKey
		}
		return 0, 0, sherr.ErrSplitBucketSeedSearchFailed
	}

	// Check if seed0 exceeds Golomb-Rice encoding limit for this splitPoint
	// If so, add to fallback list and mark as fallbackMarker
	if splitPoint > 8 || seed0 >= maxEncodableSeed(splitPoint) {
		bb.fallbackList = append(bb.fallbackList, fallbackEntry{
			bucketIndex: uint16(bucketIdx),
			subBucket:   0,
			seed:        seed0,
		})
		seed0 = fallbackMarker
	}

	var seed1 uint32

	if size1 := len(bb.subBucket1); size1 >= 2 {
		// Golomb-Rice can't encode seeds for sub-buckets larger than 8, so for
		// those skip the direct search (its result would be discarded) and go
		// straight to the extended seed search + fallback list.
		needsFallback := size1 > 8
		if !needsFallback {
			seed1, needsFallback = bb.solveDirectBucket(bb.subBucket1, size1)
		}
		if needsFallback {
			var err error
			seed1, err = bb.solveExtended(bb.subBucket1, size1)
			if err != nil {
				return 0, 0, err
			}
			bb.fallbackList = append(bb.fallbackList, fallbackEntry{
				bucketIndex: uint16(bucketIdx),
				subBucket:   1,
				seed:        seed1,
			})
			seed1 = fallbackMarker
		}
	}

	// Note: We do NOT need to solve a bijection for the first half.
	// The decoder uses the splitting seed directly: h = mixFromParts(mixParts, seed0, bucketSize)
	// If h < splitPoint, the slot IS h (the hash value itself).
	// The splitting seed ensures each first-half key gets a unique h in [0, splitPoint).

	return seed0, seed1, nil
}

func (bb *Builder) solveDirectBucket(bucket []bucketEntry, size int) (uint32, bool) {
	if size <= 1 {
		return 0, false
	}

	maxSeed := maxEncodableSeed(size)

	switch size {
	case 2:
		return solveBucket2(bucket, maxSeed)
	case 3:
		return solveBucket3(bucket, maxSeed)
	case 4:
		return solveBucket4(bucket, maxSeed)
	case 5:
		return solveBucket5(bucket, maxSeed)
	case 6:
		return solveBucket6(bucket, maxSeed)
	case 7:
		return solveBucket7(bucket, maxSeed)
	case 8:
		return solveBucket8(bucket, maxSeed)
	}

	if size <= 64 {
		return solveBucketBitmask(bucket, size, maxSeed)
	}
	return bb.solveDirectBucketArray(bucket, size, maxSeed)
}

func (bb *Builder) solveDirectBucketArray(bucket []bucketEntry, size int, maxSeed uint32) (uint32, bool) {
	bucketSize := uint32(size)

	if len(bb.slots) < size {
		bb.slots = make([]bool, size*2)
	}
	slots := bb.slots[:size]

	for seed := range maxSeed {
		clear(slots)

		collision := false
		for _, entry := range bucket {
			slot := mixFromParts(entry.mixParts, seed, bucketSize)
			if slots[slot] {
				collision = true
				break
			}
			slots[slot] = true
		}

		if !collision {
			return seed, false
		}
	}

	return 0, true
}

func (bb *Builder) solveExtended(bucket []bucketEntry, size int) (uint32, error) {
	if size <= 1 {
		return 0, nil
	}

	// Equal mixParts implies equal keys (XOR with globalSeed is bijective),
	// which means every seed produces a collision for that pair.
	if hasDuplicateMixParts(bucket[:size]) {
		return 0, sherr.ErrDuplicateKey
	}

	if size <= 64 {
		return bb.solveExtendedBitmask(bucket, size)
	}
	return bb.solveExtendedArray(bucket, size)
}

// hasDuplicateMixParts reports whether any two entries share identical mixParts.
// Equal mixParts implies equal keys (XOR with globalSeed is bijective), so no
// seed can ever separate them — such a pair is a duplicate key. O(n²), used
// only on already-failed seed-search paths.
func hasDuplicateMixParts(bucket []bucketEntry) bool {
	for i := 0; i < len(bucket)-1; i++ {
		for j := i + 1; j < len(bucket); j++ {
			if bucket[i].mixParts == bucket[j].mixParts {
				return true
			}
		}
	}
	return false
}

func (bb *Builder) solveExtendedBitmask(bucket []bucketEntry, size int) (uint32, error) {
	maxExtendedSeed := maxExtendedSeedForB(blockBits)
	bucketSize := uint32(size)

	for seed := range maxExtendedSeed {
		var occupied uint64
		collision := false
		for _, entry := range bucket {
			slot := mixFromParts(entry.mixParts, seed, bucketSize)
			mask := uint64(1) << slot
			if occupied&mask != 0 {
				collision = true
				break
			}
			occupied |= mask
		}
		if !collision {
			return seed, nil
		}
	}

	// Seed search exhausted on DISTINCT keys: genuine duplicates were already
	// ruled out by hasDuplicateMixParts in solveExtended, so this is the
	// indistinguishable-hashes case (retryable with a different global seed),
	// matching the ptrhash solver's labeling.
	return 0, sherr.ErrIndistinguishableHashes
}

func (bb *Builder) solveExtendedArray(bucket []bucketEntry, size int) (uint32, error) {
	maxExtendedSeed := maxExtendedSeedForB(blockBits)
	bucketSize := uint32(size)

	if len(bb.slots) < size {
		bb.slots = make([]bool, size*2)
	}
	slots := bb.slots[:size]

	for seed := range maxExtendedSeed {
		clear(slots)

		collision := false
		for _, entry := range bucket {
			slot := mixFromParts(entry.mixParts, seed, bucketSize)
			if slots[slot] {
				collision = true
				break
			}
			slots[slot] = true
		}

		if !collision {
			return seed, nil
		}
	}

	// Seed search exhausted on DISTINCT keys (duplicates ruled out in
	// solveExtended): the indistinguishable-hashes case, retryable with a
	// different global seed.
	return 0, sherr.ErrIndistinguishableHashes
}
