package streamhash

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// testGate is a gate a caller would hand WithYield: a yield waits until open
// is closed. It counts the yields entered.
type testGate struct {
	open    chan struct{}
	entered atomic.Int64
}

func newTestGate() *testGate { return &testGate{open: make(chan struct{})} }

func (g *testGate) yield(ctx context.Context) error {
	g.entered.Add(1)
	select {
	case <-g.open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitParked waits for n goroutines to park at g. It fails the test if the
// build returns on done first, and with a goroutine dump if neither happens
// within a minute.
func waitParked(t *testing.T, g *testGate, n int, done <-chan error) {
	t.Helper()
	err := runBuildWithWatchdog(t, time.Minute, func() error {
		for g.entered.Load() < int64(n) {
			select {
			case err := <-done:
				return fmt.Errorf("the build returned before %d goroutines parked: %v", n, err)
			case <-time.After(time.Millisecond):
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// yieldBuild is one of the builds WithYield applies to.
type yieldBuild struct {
	name    string
	workers int
	build   func(ctx context.Context, output string, opts ...BuildOption) error
	want    []byte
}

// buildWithWatchdog runs c's build with yield, failing the test if it does
// not return.
func (c yieldBuild) buildWithWatchdog(t *testing.T, output string, yield func(context.Context) error) error {
	t.Helper()
	return runBuildWithWatchdog(t, time.Minute, func() error {
		return c.build(context.Background(), output, WithWorkers(c.workers), WithYield(yield))
	})
}

// yieldBuilds returns the builds WithYield applies to over n keys, each inline
// and parallel, with the index they all build: the sorted-files build, the
// SortedBuilder, and the UnsortedBuilder through AddKey, which keeps this
// many keys in memory, and through AddKeys, whose writers spill them to disk.
func yieldBuilds(t *testing.T, dir string, n int) []yieldBuild {
	t.Helper()
	rng := newTestRNG(t)
	recs := uniqueSortedRecords(rng, n, 3)
	files := writeLayout(t, dir, rng, recs, 3, layoutOneFile)
	entries := make([]entry, len(recs))
	for i, r := range recs {
		entries[i] = entry{Key: recs[i].key[:], Payload: r.payload}
	}
	opts := []BuildOption{WithFingerprint(1), WithPayload(3)}
	want := referenceIndex(t, filepath.Join(dir, "reference.idx"), recs, opts...)
	fromFiles := func(ctx context.Context, output string, more ...BuildOption) error {
		return buildFromSortedFiles(ctx, output, files, sortedBuildTuning{stripeKeys: 1000}, append(more, opts...)...)
	}
	sorted := func(ctx context.Context, output string, more ...BuildOption) error {
		return buildFromEntries(ctx, output, entries, append(more, opts...)...)
	}
	addKey := func(ctx context.Context, output string, more ...BuildOption) error {
		ub, err := NewUnsortedBuilder(ctx, output, uint64(len(entries)), dir, append(more, opts...)...)
		if err != nil {
			return err
		}
		for _, e := range entries {
			if err := ub.AddKey(e.Key, e.Payload); err != nil {
				ub.Close()
				return err
			}
		}
		return ub.Finish()
	}
	addKeys := func(ctx context.Context, output string, more ...BuildOption) error {
		ub, err := NewUnsortedBuilder(ctx, output, uint64(len(entries)), dir, append(more, opts...)...)
		if err != nil {
			return err
		}
		return ub.AddKeys(1, func(_ int, add func([]byte, uint64) error) error {
			for _, e := range entries {
				if err := add(e.Key, e.Payload); err != nil {
					return err
				}
			}
			return nil
		})
	}
	return []yieldBuild{
		{"files/inline", 1, fromFiles, want},
		{"files/parallel", 4, fromFiles, want},
		{"sorted/inline", 1, sorted, want},
		{"sorted/parallel", 4, sorted, want},
		{"unsorted-addkey/inline", 1, addKey, want},
		{"unsorted-addkey/parallel", 4, addKey, want},
		{"unsorted-addkeys/inline", 1, addKeys, want},
		{"unsorted-addkeys/parallel", 4, addKeys, want},
	}
}

// TestWithYieldHoldsTheBuild closes the gate before the build starts, checks
// that every goroutine building blocks parks at its first block, then opens
// the gate and checks the index and that every block yielded once.
func TestWithYieldHoldsTheBuild(t *testing.T) {
	dir := t.TempDir()
	for _, c := range yieldBuilds(t, dir, 30000) {
		t.Run(c.name, func(t *testing.T) {
			g := newTestGate()
			output := filepath.Join(dir, "held.idx")
			done := make(chan error, 1)
			go func() { done <- c.build(t.Context(), output, WithWorkers(c.workers), WithYield(g.yield)) }()
			waitParked(t, g, c.workers, done)
			time.Sleep(50 * time.Millisecond)
			if got := g.entered.Load(); got != int64(c.workers) {
				t.Fatalf("%d goroutines parked at the gate, want %d", got, c.workers)
			}
			close(g.open)
			if err := runBuildWithWatchdog(t, time.Minute, func() error { return <-done }); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFileBytes(t, output), c.want) {
				t.Fatal("the index differs from the reference")
			}
			checkYieldPerBlock(t, output, g)
		})
	}
}

// checkYieldPerBlock checks that the build of the index at path entered g
// once per block.
func checkYieldPerBlock(t *testing.T, path string, g *testGate) {
	t.Helper()
	idx, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()
	if blocks := int64(idx.NumBlocks()); g.entered.Load() != blocks {
		t.Fatalf("%d yields for %d blocks", g.entered.Load(), blocks)
	}
}

// TestWithYieldError checks that a yield's error fails the build with it,
// whether it is the first block's or the second's, an empty block's among
// them with one key, and that the failed build removes its output.
func TestWithYieldError(t *testing.T) {
	errStop := errors.New("stop")
	for _, n := range []int{1, 30000} {
		dir := t.TempDir()
		for _, c := range yieldBuilds(t, dir, n) {
			for _, failAt := range []int64{1, 2} {
				t.Run(fmt.Sprintf("%s/keys=%d/yield=%d", c.name, n, failAt), func(t *testing.T) {
					var calls atomic.Int64
					yield := func(context.Context) error {
						if calls.Add(1) == failAt {
							return errStop
						}
						return nil
					}
					output := filepath.Join(dir, "failed.idx")
					if err := c.buildWithWatchdog(t, output, yield); !errors.Is(err, errStop) {
						t.Fatalf("the build returned %v, want the yield's error", err)
					}
					if _, err := os.Stat(output); !os.IsNotExist(err) {
						t.Fatalf("the failed build left its output: %v", err)
					}
				})
			}
		}
	}
}

// TestWithYieldCanceled cancels a build parked at the gate and checks that
// it returns the cancellation and removes its output.
func TestWithYieldCanceled(t *testing.T) {
	dir := t.TempDir()
	for _, c := range yieldBuilds(t, dir, 30000) {
		t.Run(c.name, func(t *testing.T) {
			g := newTestGate()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			output := filepath.Join(dir, "canceled.idx")
			done := make(chan error, 1)
			go func() { done <- c.build(ctx, output, WithWorkers(c.workers), WithYield(g.yield)) }()
			waitParked(t, g, c.workers, done)
			cancel()
			if err := runBuildWithWatchdog(t, time.Minute, func() error { return <-done }); !errors.Is(err, context.Canceled) {
				t.Fatalf("the canceled build returned %v", err)
			}
			if _, err := os.Stat(output); !os.IsNotExist(err) {
				t.Fatalf("the canceled build left its output: %v", err)
			}
		})
	}
}

// TestWithYieldPerBlock checks that a build yields for empty blocks too: with
// no keys both blocks are empty, and with one key one of the two is.
func TestWithYieldPerBlock(t *testing.T) {
	for _, n := range []int{0, 1} {
		dir := t.TempDir()
		for _, c := range yieldBuilds(t, dir, n) {
			t.Run(fmt.Sprintf("%s/keys=%d", c.name, n), func(t *testing.T) {
				g := newTestGate()
				close(g.open)
				output := filepath.Join(dir, "counted.idx")
				if err := c.buildWithWatchdog(t, output, g.yield); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(readFileBytes(t, output), c.want) {
					t.Fatal("the index differs from the reference")
				}
				checkYieldPerBlock(t, output, g)
			})
		}
	}
}

// TestWithYieldNil checks that WithYield(nil) builds the index that no option
// does, with no yield to call. One key makes two blocks, one of them empty,
// which reach every place a block is built.
func TestWithYieldNil(t *testing.T) {
	dir := t.TempDir()
	for _, c := range yieldBuilds(t, dir, 1) {
		t.Run(c.name, func(t *testing.T) {
			output := filepath.Join(dir, "nil.idx")
			if err := c.buildWithWatchdog(t, output, nil); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(readFileBytes(t, output), c.want) {
				t.Fatal("the index differs from the reference")
			}
		})
	}
}

// TestWithYieldEchoKeepsTheFailure fails a sorted-files build on a record out
// of order, with a yield that returns its context's error as a gate does once
// the build has failed, and checks that the build reports the record and not
// the cancellation that the yield echoes.
func TestWithYieldEchoKeepsTheFailure(t *testing.T) {
	rng := newTestRNG(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "out-of-order.sorted")
	writeSortedFile(t, path, SortedFileConfig{PayloadSize: 3}, uniqueSortedRecords(rng, 30000, 3))
	data := readFileBytes(t, path)
	swapRecords(data, MinKeySize+3, 777)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	echo := func(ctx context.Context) error { return ctx.Err() }
	for range 20 {
		err := buildFilesWithWatchdog(t, context.Background(), filepath.Join(dir, "failed.idx"), []string{path}, sortedBuildTuning{stripeKeys: 1000}, WithWorkers(4), WithYield(echo))
		if !errors.Is(err, ErrUnsortedInput) {
			t.Fatalf("the build returned %v, want ErrUnsortedInput", err)
		}
	}
}
