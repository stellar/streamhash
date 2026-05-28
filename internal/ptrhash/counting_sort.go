package ptrhash

// countingSortBucketsInto sorts buckets by size (largest first) using counting sort.
// Reuses the result slice and pre-allocated counts/positions buffers to avoid per-block allocations.
func countingSortBucketsInto(bucketStarts []uint16, result []uint16, counts []int, positions []int) {
	n := len(bucketStarts) - 1
	if n <= 0 {
		return
	}

	// Find max bucket size
	maxSize := 0
	for i := range n {
		size := int(bucketStarts[i+1] - bucketStarts[i])
		if size > maxSize {
			maxSize = size
		}
	}

	if maxSize == 0 {
		for i := range result[:n] {
			result[i] = uint16(i)
		}
		return
	}

	// Clear counts buffer up to maxSize+1
	for i := 0; i <= maxSize; i++ {
		counts[i] = 0
	}

	// Count occurrences of each size
	for i := range n {
		size := int(bucketStarts[i+1] - bucketStarts[i])
		counts[size]++
	}

	// Convert to positions (reverse order for largest first)
	pos := 0
	for size := maxSize; size >= 0; size-- {
		positions[size] = pos
		pos += counts[size]
	}

	// Place buckets in sorted order
	for i := range n {
		size := int(bucketStarts[i+1] - bucketStarts[i])
		result[positions[size]] = uint16(i)
		positions[size]++
	}
}
