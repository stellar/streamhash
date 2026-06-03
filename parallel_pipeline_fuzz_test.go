package streamhash

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/tamirms/streamhash/internal/sherr"
)

// This file is a property/fuzz harness for the parallel build pipeline's
// teardown behavior. Several deadlocks have been found and fixed in this
// pipeline -- triggered by context cancellation, a per-block build error, and a
// pure writer-side error. They shared the same root shape: a goroutine left
// blocked on a channel after a peer exited. Rather than keep finding instances
// one at a time, this harness systematically exercises the trigger space
// (mode x workers x size x scenario x timing) and asserts the single invariant
// that closes the class:
//
//	A parallel build always RETURNS (never hangs); a deadlock is caught by a
//	watchdog and reported with a full goroutine dump.

// runBuildWithWatchdog runs build in a goroutine and returns its error, failing
// the test with a full goroutine dump if it does not finish within timeout. This
// turns a build-pipeline deadlock into a loud, diagnosable failure instead of a
// hung test run (the race detector cannot see deadlocks). Shared by the deadlock
// regression tests in this package.
func runBuildWithWatchdog(t *testing.T, timeout time.Duration, build func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- build() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		t.Fatalf("build did not return within %s -- likely deadlock.\nGoroutine dump:\n%s", timeout, buf[:n])
		return nil // unreachable; Fatalf ends the goroutine
	}
}

type pipeMode uint8

const (
	pipeSorted pipeMode = iota
	pipeUnsortedAddKey
	pipeUnsortedAddKeys
	pipeModeCount
)

func (m pipeMode) String() string {
	switch m {
	case pipeSorted:
		return "sorted"
	case pipeUnsortedAddKey:
		return "unsorted-addkey"
	case pipeUnsortedAddKeys:
		return "unsorted-addkeys"
	default:
		return "mode?"
	}
}

type pipeScenario uint8

const (
	scenValid     pipeScenario = iota // well-formed input: build must succeed + index is a bijection
	scenDuplicate                     // a duplicate key: build must fail, not hang
	scenCancel                        // ctx cancelled mid-build: build must return, not hang
	scenCount
)

func (s pipeScenario) String() string {
	switch s {
	case scenValid:
		return "valid"
	case scenDuplicate:
		return "duplicate"
	case scenCancel:
		return "cancel"
	default:
		return "scen?"
	}
}

type pipeConfig struct {
	mode       pipeMode
	scenario   pipeScenario
	workers    int
	numKeys    int
	cancelFrac float64 // scenCancel: cancel after this fraction of keys are added
	seed       uint64
}

func (c pipeConfig) String() string {
	s := fmt.Sprintf("%s_%s_w%d_n%d", c.mode, c.scenario, c.workers, c.numKeys)
	if c.scenario == scenCancel {
		s += fmt.Sprintf("_c%02d", int(c.cancelFrac*100))
	}
	return s
}

// runPipelineScenario builds an index per cfg under a watchdog and asserts the
// invariant for the scenario. The watchdog turns any pipeline deadlock into a
// loud failure with a goroutine dump instead of a hung run.
func runPipelineScenario(t *testing.T, cfg pipeConfig) {
	t.Helper()

	rng := rand.New(rand.NewPCG(cfg.seed, cfg.seed^0x9e3779b97f4a7c15))
	keys := generateRandomKeys(rng, cfg.numKeys, 16)
	if cfg.scenario == scenDuplicate && cfg.numKeys >= 2 {
		// Collide two slots so one block sees a duplicate during construction.
		i := rng.IntN(cfg.numKeys)
		j := rng.IntN(cfg.numKeys)
		if j == i {
			j = (i + 1) % cfg.numKeys
		}
		copy(keys[j], keys[i])
	}

	ctx := context.Background()
	cancel := context.CancelFunc(func() {}) // no-op default keeps the defer/cancel calls unconditional
	if cfg.scenario == scenCancel {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "idx")
	cancelAfter := int(float64(cfg.numKeys) * cfg.cancelFrac)

	build := func() error {
		switch cfg.mode {
		case pipeSorted:
			sortKeysByBlock(keys, uint64(cfg.numKeys), nil)
			b, err := NewSortedBuilder(ctx, indexPath, uint64(cfg.numKeys), WithWorkers(cfg.workers))
			if err != nil {
				return err
			}
			for n, k := range keys {
				if cfg.scenario == scenCancel && n == cancelAfter {
					cancel()
				}
				if err := b.AddKey(k, uint64(n)); err != nil {
					b.Close()
					return err
				}
			}
			return b.Finish()

		case pipeUnsortedAddKey:
			b, err := NewUnsortedBuilder(ctx, indexPath, uint64(cfg.numKeys), tmpDir, WithWorkers(cfg.workers))
			if err != nil {
				return err
			}
			for n, k := range keys {
				if cfg.scenario == scenCancel && n == cancelAfter {
					cancel()
				}
				if err := b.AddKey(k, uint64(n)); err != nil {
					b.Close()
					return err
				}
			}
			return b.Finish()

		case pipeUnsortedAddKeys:
			b, err := NewUnsortedBuilder(ctx, indexPath, uint64(cfg.numKeys), tmpDir, WithWorkers(cfg.workers))
			if err != nil {
				return err
			}
			nw := cfg.workers
			// AddKeys runs the callback once per writer concurrently and calls
			// Finish internally. Each writer cancels when it reaches its own
			// fraction so cancellation lands mid-build deterministically.
			return b.AddKeys(nw, func(writerID int, addKey func([]byte, uint64) error) error {
				added := 0
				myTotal := (cfg.numKeys - writerID + nw - 1) / nw
				myCancelAt := int(float64(myTotal) * cfg.cancelFrac)
				for n := writerID; n < cfg.numKeys; n += nw {
					if cfg.scenario == scenCancel && added == myCancelAt {
						cancel()
					}
					if err := addKey(keys[n], uint64(n)); err != nil {
						return err
					}
					added++
				}
				return nil
			})
		}
		return nil
	}

	buildErr := runBuildWithWatchdog(t, 60*time.Second, build)

	switch cfg.scenario {
	case scenValid:
		if buildErr != nil {
			t.Fatalf("%s: valid build failed: %v", cfg, buildErr)
		}
		assertBijection(t, indexPath, keys)

	case scenDuplicate:
		if buildErr == nil {
			t.Fatalf("%s: expected a duplicate-key error, got nil", cfg)
		}
		// The precise cause is ErrDuplicateKey; in some interleavings the
		// pipeline surfaces a context cancellation from its own teardown first.
		// Both mean the build failed cleanly; only nil or an unrelated error is
		// a bug.
		if !errors.Is(buildErr, sherr.ErrDuplicateKey) && !errors.Is(buildErr, context.Canceled) {
			t.Fatalf("%s: expected ErrDuplicateKey, got: %v", cfg, buildErr)
		}

	case scenCancel:
		// Best-effort cancellation: the build may finish before the cancel lands
		// (nil error, valid index) or observe it (context.Canceled or a teardown
		// error). The only failure mode is a hang, which the watchdog catches.
		if buildErr == nil {
			assertBijection(t, indexPath, keys)
		}
	}
}

// assertBijection opens the index and checks that every key maps to a distinct
// rank in [0, N) -- i.e. the build produced a valid minimal perfect hash. This
// catches a teardown bug that silently corrupts otherwise-successful output.
func assertBijection(t *testing.T, path string, keys [][]byte) {
	t.Helper()
	idx, err := Open(path)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	defer idx.Close()
	verifyMPHF(t, idx, keys) // shared helper: every key -> distinct rank in [0,N)
}

// TestParallelPipelineTeardownSweep deterministically exercises the parallel
// pipeline teardown across modes, worker counts, sizes, and scenarios. It is the
// in-CI counterpart to the fuzz target: same runner, fixed matrix.
func TestParallelPipelineTeardownSweep(t *testing.T) {
	workerCounts := []int{2, 4}
	sizes := []int{20000}
	if !testing.Short() {
		workerCounts = append(workerCounts, 8)
		sizes = []int{2000, 20000, 60000}
	}
	cancelFracs := []float64{0.1, 0.5, 0.9}

	var seed uint64
	for _, mode := range []pipeMode{pipeSorted, pipeUnsortedAddKey, pipeUnsortedAddKeys} {
		for _, workers := range workerCounts {
			for _, size := range sizes {
				for _, scen := range []pipeScenario{scenValid, scenDuplicate, scenCancel} {
					fracs := []float64{0}
					if scen == scenCancel {
						fracs = cancelFracs
					}
					for _, frac := range fracs {
						seed++
						cfg := pipeConfig{mode, scen, workers, size, frac, seed}
						t.Run(cfg.String(), func(t *testing.T) {
							runPipelineScenario(t, cfg)
						})
					}
				}
			}
		}
	}
}

// TestUnsortedParallelSpillTeardown exercises the disk-spill unsorted finish
// path (finishUnsortedParallel, with its reader-goroutine pool) under both a
// build error and cancellation. The in-memory fast path is well covered above;
// this drives the heavier path that only runs once the per-writer buffers spill
// to disk (> ~524k buffered entries). Skipped under -short.
func TestUnsortedParallelSpillTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping disk-spill teardown test under -short")
	}
	const spillKeys = 600000 // exceeds the ~524k flush threshold -> spills to disk
	for _, scen := range []pipeScenario{scenValid, scenDuplicate, scenCancel} {
		cfg := pipeConfig{pipeUnsortedAddKey, scen, 4, spillKeys, 0.5, 99}
		t.Run(cfg.String(), func(t *testing.T) {
			runPipelineScenario(t, cfg)
		})
	}
}

// FuzzParallelPipelineTeardown lets `go test -fuzz` explore the teardown trigger
// space; the seed corpus runs as ordinary subtests in CI. A hang in any explored
// configuration is caught by the per-build watchdog and reported as a crasher.
func FuzzParallelPipelineTeardown(f *testing.F) {
	// (mode, scenario, workers, numKeys, cancelPct)
	f.Add(uint8(0), uint8(1), uint8(2), uint16(20000), uint8(50)) // sorted, duplicate, 2w
	f.Add(uint8(1), uint8(1), uint8(4), uint16(20000), uint8(50)) // unsorted-addkey, duplicate, 4w
	f.Add(uint8(2), uint8(2), uint8(4), uint16(10000), uint8(30)) // unsorted-addkeys, cancel, 4w
	f.Add(uint8(0), uint8(0), uint8(8), uint16(30000), uint8(0))  // sorted, valid, 8w
	f.Add(uint8(1), uint8(2), uint8(2), uint16(25000), uint8(90)) // unsorted, cancel late
	f.Add(uint8(2), uint8(0), uint8(3), uint16(15000), uint8(0))  // addkeys, valid, 3w

	f.Fuzz(func(t *testing.T, modeB, scenB, workersB uint8, numKeysB uint16, cancelPctB uint8) {
		cfg := pipeConfig{
			mode:       pipeMode(modeB % uint8(pipeModeCount)),
			scenario:   pipeScenario(scenB % uint8(scenCount)),
			workers:    int(workersB%8) + 1,
			numKeys:    int(numKeysB%30000) + 2, // >=2 so a duplicate has room
			cancelFrac: float64(cancelPctB%101) / 100.0,
			// Vary key material with the structural inputs so different shapes
			// exercise different block layouts.
			seed: uint64(modeB)<<24 | uint64(scenB)<<16 | uint64(numKeysB),
		}
		runPipelineScenario(t, cfg)
	})
}

// ---------------------------------------------------------------------------
// Deterministic deadlock regression tests. Narrower than the sweep above (one
// specific trigger each), kept here so the whole pipeline-teardown concern --
// watchdog, scenario runner, sweep, fuzz, and these focused regressions -- lives
// in one file.
//
// NOTE: race-stress.yml folds these into its -count x -cpu interleaving sweep by
// matching "Deadlock"/"WriterError" in the test name; preserve that substring on
// any rename or they silently drop out of the stress gate.
// ---------------------------------------------------------------------------

// TestUnsortedParallelDuplicateNoDeadlockStress is a regression for the
// outer-select deadlock in finishUnsortedParallel: a duplicate key under
// unsorted parallel workers intermittently (~2/20) hung forever because the main
// consumer's OUTER select waited only on resultCh[r] and b.ctx.Done() — not on
// b.writerDone / b.workerCtx — so when a worker failed (cancelling b.workerCtx,
// a child of b.ctx) and orphaned the per-slot fences that park the readers, the
// consumer had no escape.
//
// It runs the deadlock-prone configuration many times across modes and worker
// counts; runPipelineScenario watchdog-bounds every build, so any recurrence
// fails loudly with a goroutine dump instead of hanging the suite.
func TestUnsortedParallelDuplicateNoDeadlockStress(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping deadlock stress under -short")
	}
	const iters = 15
	const numKeys = 30000 // matches the size that surfaced the intermittent hang
	seed := uint64(0)
	for _, mode := range []pipeMode{pipeUnsortedAddKey, pipeUnsortedAddKeys} {
		for _, workers := range []int{2, 3, 4} {
			for range iters {
				seed++
				runPipelineScenario(t, pipeConfig{
					mode:     mode,
					scenario: scenDuplicate,
					workers:  workers,
					numKeys:  numKeys,
					seed:     seed,
				})
			}
		}
	}
}

// TestWriterErrorNoDeadlock is a regression for the pure-writer-error teardown
// deadlock found by the deadlock-freedom audit. When the writer goroutine fails
// (e.g. an ENOSPC/EIO metadata write) during the DRAIN phase — after all blocks
// are dispatched, so the main goroutine is parked in drainParallelPipeline at
// workerGroup.Wait() — it used to send to writerDone and exit WITHOUT cancelling
// workerCtx. With numBlocks > workers*2 the workers wedged on the resultChan
// send (escapable only via workerCtx.Done()) and Wait() hung forever, with no
// timeout. The fix routes every writer error exit through failWriter, which
// cancels workerCtx and releases the workers.
//
// The fault is injected the moment main enters the drain phase via the test-only
// b.writerFaultHook. Each build is watchdog-bounded, so a regression fails loudly
// with a goroutine dump instead of hanging the suite. MPHF-only (no
// payload/fingerprint) mirrors the worst case where the writer's metadata write
// is the only pwrite in the pipeline.
func TestWriterErrorNoDeadlock(t *testing.T) {
	injected := errors.New("injected metadata write fault")
	rng := rand.New(rand.NewPCG(7, 11))
	keys := generateRandomKeys(rng, 100000, 16) // many blocks (> workers*2)

	for _, workers := range []int{2, 4} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			sorted := make([][]byte, len(keys))
			copy(sorted, keys)
			opts := []BuildOption{WithWorkers(workers)}
			sortKeysByBlock(sorted, uint64(len(sorted)), opts)

			sb, err := NewSortedBuilder(context.Background(), filepath.Join(t.TempDir(), "idx"), uint64(len(sorted)), opts...)
			if err != nil {
				t.Fatalf("NewSortedBuilder: %v", err)
			}
			// Fail a metadata write the moment main enters the drain phase
			// (drainParallelPipeline sets workersShutDown after close(workChan),
			// so main is parked at workerGroup.Wait while workers are still
			// draining workChan and sending results). That is exactly the window
			// where workers wedge on the resultChan send with no escape unless
			// the writer cancels workerCtx. (Set before the first AddKey so the
			// channel handoffs establish happens-before to the writer's read.)
			sb.b.writerFaultHook = func(blockID uint32) error {
				if sb.b.workersShutDown.Load() {
					return injected
				}
				return nil
			}

			buildErr := runBuildWithWatchdog(t, 30*time.Second, func() error {
				for _, k := range sorted {
					if e := sb.AddKey(k, 0); e != nil {
						sb.Close()
						return e
					}
				}
				return sb.Finish()
			})
			if buildErr == nil {
				t.Fatal("expected the injected writer error, got nil")
			}
			if !errors.Is(buildErr, injected) {
				t.Fatalf("expected the injected error to surface, got: %v", buildErr)
			}
		})
	}
}
