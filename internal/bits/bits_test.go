package bits

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"testing"
)

// Named seeds for deterministic reproduction.
const (
	testSeed1 = 0x1234567890ABCDEF
	testSeed2 = 0xFEDCBA9876543210
)

func newTestRNG(t testing.TB) *rand.Rand {
	t.Helper()
	h := fnv.New128a()
	h.Write([]byte(t.Name()))
	sum := h.Sum(nil)
	s1 := binary.LittleEndian.Uint64(sum[:8])
	s2 := binary.LittleEndian.Uint64(sum[8:])
	return rand.New(rand.NewPCG(testSeed1^s1, testSeed2^s2))
}

// TestFastRange32Monotonicity verifies that for a fixed n,
// FastRange32 is monotone: h1 < h2 implies FastRange32(h1,n) <= FastRange32(h2,n).
func TestFastRange32Monotonicity(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 10000

	for i := range iterations {
		n := uint32(rng.Uint32N(math.MaxUint32)) + 1 // n in [1, MaxUint32]
		h1 := rng.Uint64()
		h2 := rng.Uint64()
		if h1 > h2 {
			h1, h2 = h2, h1
		}

		r1 := FastRange32(h1, n)
		r2 := FastRange32(h2, n)
		if r1 > r2 {
			t.Fatalf("iter %d: monotonicity violated: FastRange32(0x%X, %d)=%d > FastRange32(0x%X, %d)=%d",
				i, h1, n, r1, h2, n, r2)
		}
	}
}

// TestFastRange32Range verifies that the result is always in [0, n).
func TestFastRange32Range(t *testing.T) {
	rng := newTestRNG(t)
	const iterations = 10000

	for i := range iterations {
		n := uint32(rng.Uint32N(math.MaxUint32)) + 1 // n in [1, MaxUint32]
		h := rng.Uint64()

		got := FastRange32(h, n)
		if got >= n {
			t.Fatalf("iter %d: FastRange32(0x%X, %d)=%d >= %d",
				i, h, n, got, n)
		}
	}
}

// TestFastRange32EdgeCases tests deterministic edge cases:
// n=0->0, n=1->0, n=MaxUint32->result<MaxUint32, n=MaxUint32-1->result<MaxUint32-1,
// h=0->0, h=MaxUint64->n-1.
func TestFastRange32EdgeCases(t *testing.T) {
	// n=0 always returns 0
	for _, h := range []uint64{0, 1, math.MaxUint64, 0xDEADBEEF} {
		if got := FastRange32(h, 0); got != 0 {
			t.Errorf("FastRange32(0x%X, 0) = %d, want 0", h, got)
		}
	}

	// n=1 always returns 0
	for _, h := range []uint64{0, 1, math.MaxUint64, 0xDEADBEEF, math.MaxUint64 / 2} {
		if got := FastRange32(h, 1); got != 0 {
			t.Errorf("FastRange32(0x%X, 1) = %d, want 0", h, got)
		}
	}

	// n=MaxUint32 -> result < MaxUint32
	got := FastRange32(math.MaxUint64, math.MaxUint32)
	if got == math.MaxUint32 {
		t.Errorf("FastRange32(MaxUint64, MaxUint32) = %d, want < MaxUint32", got)
	}
	if got != math.MaxUint32-1 {
		t.Errorf("FastRange32(MaxUint64, MaxUint32) = %d, want %d", got, uint32(math.MaxUint32-1))
	}

	// n=MaxUint32-1 -> result < MaxUint32-1
	got2 := FastRange32(math.MaxUint64, math.MaxUint32-1)
	if got2 >= math.MaxUint32-1 {
		t.Errorf("FastRange32(MaxUint64, MaxUint32-1) = %d, want < %d", got2, uint32(math.MaxUint32-1))
	}

	// h=0 always maps to 0 for any n
	for n := uint32(1); n <= 100; n++ {
		if got := FastRange32(0, n); got != 0 {
			t.Errorf("FastRange32(0, %d) = %d, want 0", n, got)
		}
	}

	// h=MaxUint64 maps to n-1 for any n >= 2
	for n := uint32(2); n <= 100; n++ {
		got := FastRange32(math.MaxUint64, n)
		if got != n-1 {
			t.Errorf("FastRange32(MaxUint64, %d) = %d, want %d", n, got, n-1)
		}
	}
}
