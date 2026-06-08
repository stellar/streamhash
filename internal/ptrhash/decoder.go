package ptrhash

import (
	"fmt"

	"github.com/stellar/streamhash/internal/sherr"
)

// Decoder handles query-time slot computation for PTRHash blocks.
type Decoder struct {
	pilotHPs [numPilotValues]uint64 // Precomputed pilotHash values for all 256 pilots
}

// NewDecoder creates a new PTRHash block decoder.
// globalConfig is algorithm-specific configuration (currently unused for ptrhash).
// Returns an error if globalConfig is unexpectedly non-empty.
func NewDecoder(globalConfig []byte, globalSeed uint64) (*Decoder, error) {
	if len(globalConfig) != 0 {
		return nil, fmt.Errorf("%w: ptrhash: unexpected non-empty global config (len=%d)", sherr.ErrCorruptedIndex, len(globalConfig))
	}
	d := &Decoder{}
	initPilotHPs(&d.pilotHPs, globalSeed)
	return d, nil
}

// QuerySlot computes the slot for a key within a block.
// k0, k1: key representations (k1 is suffix for bucket assignment)
// metadata is the block's encoded metadata.
// keysInBlock is the number of keys in this block.
// Returns the local slot index (0-based within the block).
func (d *Decoder) QuerySlot(k0, k1 uint64, metadata []byte, keysInBlock int) (int, error) {
	if keysInBlock == 0 {
		return 0, sherr.ErrNotFound
	}

	numBuckets := bucketsPerBlock
	numSlots := int(computeNumSlots(keysInBlock))

	// Pilot section: 8 bits per bucket (direct bytes)
	if len(metadata) < numBuckets {
		return 0, sherr.ErrCorruptedIndex
	}

	// Use k1 (bytes 8-15 as little-endian) as the suffix for bucket assignment.
	// Compute local bucket index using suffix directly with CubicEps distribution.
	// This matches the builder's bucket assignment.
	localBucket := int(cubicEpsBucket(k1, bucketsPerBlock))

	// Extract pilot for this bucket and compute slot using k0^k1
	pilot := metadata[localBucket]
	hp := d.pilotHPs[pilot]
	hFolded := foldSlotInput(k0, k1)
	slot := pilotSlotFolded(hFolded, hp, uint32(numSlots))

	// If slot is in overflow range, use direct remap lookup
	if int(slot) >= keysInBlock {
		remapData := metadata[numBuckets:]
		// metadata is untrusted (per-block contents are not validated at Open),
		// so verify the remap region is large enough for every overflow slot in
		// this block before indexing into it. slot < numSlots, so checking the
		// largest possible offset covers it. Without this, a truncated/corrupt
		// remap region panics instead of returning ErrCorruptedIndex.
		requiredRemap := remapTableHeaderSize + (numSlots-keysInBlock)*remapTableEntrySize
		if len(remapData) < requiredRemap {
			return 0, sherr.ErrCorruptedIndex
		}
		slot = lookupRemap(remapData, slot, uint32(keysInBlock))
	}

	return int(slot), nil
}
