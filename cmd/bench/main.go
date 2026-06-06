// Bench is a benchmarking and data generation tool for StreamHash.
//
// Subcommands:
//
//	(default)     In-memory benchmark with synthetic keys
//	gen           Generate random entry files (sorted or unsorted)
//	bench-files   Benchmark building an index from entry files on disk
//
// Examples:
//
//	go run ./cmd/bench -keys 10000000 -payload 4 -algo ptrhash
//	go run ./cmd/bench gen -out /data/sorted -files 1000
//	go run ./cmd/bench gen -out /data/unsorted -files 1000 -sorted=false
//	go run ./cmd/bench bench-files -mode unsorted -cw 8 -dir /data/unsorted -out index.bin
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spaolacci/murmur3"

	"github.com/stellar/streamhash"
)

// getMaxRSS returns the maximum resident set size in bytes.
// Uses getrusage(RUSAGE_SELF) which tracks peak RSS since process start.
func getMaxRSS() uint64 {
	var rusage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &rusage); err != nil {
		return 0
	}
	// On macOS, MaxRss is in bytes. On Linux, it's in kilobytes.
	maxRSS := uint64(rusage.Maxrss)
	if runtime.GOOS == "linux" {
		maxRSS *= 1024 // Convert KB to bytes on Linux
	}
	return maxRSS
}

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "gen":
			os.Args = os.Args[1:]
			mainGen()
			return
		case "bench-files":
			os.Args = os.Args[1:]
			mainBenchFiles()
			return
		}
	}

	keysFlag := flag.Int("keys", 10_000_000, "number of keys")
	payloadFlag := flag.Int("payload", 4, "payload size in bytes (0 for MPHF-only)")
	fpFlag := flag.Int("fp", 1, "fingerprint size in bytes")
	workersFlag := flag.Int("workers", 1, "number of parallel workers for building")
	sortedFlag := flag.Bool("sorted", true, "use sorted input mode (false = unsorted input mode)")
	cwFlag := flag.Int("cw", 0, "number of concurrent writers (0 = single-threaded AddKey)")
	algoFlag := flag.String("algo", "bijection", "algorithm: bijection or ptrhash")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file (build phase only)")
	memprofile := flag.String("memprofile", "", "write memory profile to file (build phase only)")
	flag.Parse()

	numKeys := *keysFlag
	payloadSize := *payloadFlag
	fpSize := *fpFlag
	sortedMode := *sortedFlag

	fmt.Println("Generating keys...")
	keys := make([][32]byte, numKeys)
	for i := range keys {
		_, _ = rand.Read(keys[i][:]) // crypto/rand.Read error is fatal system issue; ignore for benchmark
	}

	var sortDuration time.Duration
	if sortedMode {
		fmt.Println("Sorting keys...")
		sortStart := time.Now()
		slices.SortFunc(keys, func(a, b [32]byte) int {
			return bytes.Compare(a[:], b[:])
		})
		sortDuration = time.Since(sortStart)
	} else {
		fmt.Println("Skipping sort (unsorted mode)...")
	}

	fmt.Println("Hashing keys...")
	hashStart := time.Now()
	seed := uint32(0x1234)
	var hashSink uint64
	for i := range keys {
		h1, h2 := murmur3.Sum128WithSeed(keys[i][:], seed)
		hashSink ^= h1 ^ h2
	}
	hashDuration := time.Since(hashStart)
	runtime.KeepAlive(hashSink) // prevent the compiler from eliding the hash loop

	var payloads []uint64
	if payloadSize > 0 && payloadSize <= 8 {
		fmt.Println("Generating payloads...")
		payloads = make([]uint64, numKeys)
		for i := range payloads {
			// Random uint32 in range [2^24, 2^32-1]
			val := mrand.Uint32N(0xFFFFFFFF-0x01000000+1) + 0x01000000
			payloads[i] = uint64(val)
		}
	} else if payloadSize > 8 {
		fmt.Printf("Payload size %d > 8 bytes not supported in this benchmark\n", payloadSize)
		return
	} else {
		fmt.Println("MPHF mode (no payloads)...")
	}

	tmpDir, err := os.MkdirTemp("", "bench-")
	if err != nil {
		fmt.Printf("Failed to create temp dir: %v\n", err)
		return
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()
	indexPath := filepath.Join(tmpDir, "test.idx")

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	baselineRSS := getMaxRSS()

	// 10ms sampling for peak memory (both heap and RSS).
	// Uses runtime/metrics instead of ReadMemStats to avoid stop-the-world pauses
	// that cause ~50ms overhead and distort CPU profiles.
	var peakAlloc atomic.Uint64
	var peakRSS atomic.Uint64
	peakAlloc.Store(baseline.Alloc)
	peakRSS.Store(baselineRSS)
	done := make(chan struct{})
	go func() {
		samples := []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				metrics.Read(samples)
				heapBytes := samples[0].Value.Uint64()
				for {
					old := peakAlloc.Load()
					if heapBytes <= old || peakAlloc.CompareAndSwap(old, heapBytes) {
						break
					}
				}
				rss := getMaxRSS()
				for {
					old := peakRSS.Load()
					if rss <= old || peakRSS.CompareAndSwap(old, rss) {
						break
					}
				}
			}
		}
	}()

	if sortedMode {
		fmt.Println("Building index (sorted mode)...")
	} else {
		fmt.Println("Building index (unsorted mode)...")
	}

	// Start CPU profile for build phase
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Printf("could not create CPU profile: %v\n", err)
			return
		}
		defer func() { _ = f.Close() }()
		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Printf("could not start CPU profile: %v\n", err)
			return
		}
	}

	buildStart := time.Now()

	// Algorithm selection
	var algo streamhash.Algorithm
	switch *algoFlag {
	case "bijection":
		algo = streamhash.AlgoBijection
	case "ptrhash":
		algo = streamhash.AlgoPTRHash
	default:
		fmt.Printf("Unknown algorithm: %s (use 'bijection' or 'ptrhash')\n", *algoFlag)
		return
	}

	opts := []streamhash.BuildOption{
		streamhash.WithPayload(payloadSize),
		streamhash.WithFingerprint(fpSize),
		streamhash.WithWorkers(*workersFlag),
		streamhash.WithAlgorithm(algo),
	}

	numCW := *cwFlag
	var addKeyDuration, finishDuration time.Duration
	var buildErr error

	if sortedMode {
		sb, err := streamhash.NewSortedBuilder(context.Background(), indexPath, uint64(numKeys), opts...)
		if err != nil {
			fmt.Printf("NewSortedBuilder failed: %v\n", err)
			return
		}
		for i := range keys {
			var payload uint64
			if payloads != nil {
				payload = payloads[i]
			}
			if err := sb.AddKey(keys[i][:], payload); err != nil {
				_ = sb.Close()
				fmt.Printf("AddKey failed: %v\n", err)
				return
			}
		}
		addKeyDuration = time.Since(buildStart)
		finishStart := time.Now()
		buildErr = sb.Finish()
		finishDuration = time.Since(finishStart)
	} else if numCW > 0 {
		ub, err := streamhash.NewUnsortedBuilder(context.Background(), indexPath, uint64(numKeys), tmpDir, opts...)
		if err != nil {
			fmt.Printf("NewUnsortedBuilder failed: %v\n", err)
			return
		}
		keysPerWriter := numKeys / numCW
		buildErr = ub.AddKeys(numCW, func(writerID int, addKey func([]byte, uint64) error) error {
			start := writerID * keysPerWriter
			end := start + keysPerWriter
			if writerID == numCW-1 {
				end = numKeys
			}
			for i := start; i < end; i++ {
				var payload uint64
				if payloads != nil {
					payload = payloads[i]
				}
				if err := addKey(keys[i][:], payload); err != nil {
					return err
				}
			}
			return nil
		})
		addKeyDuration = time.Since(buildStart)
		// AddKeys calls Finish internally
	} else {
		ub, err := streamhash.NewUnsortedBuilder(context.Background(), indexPath, uint64(numKeys), tmpDir, opts...)
		if err != nil {
			fmt.Printf("NewUnsortedBuilder failed: %v\n", err)
			return
		}
		for i := range keys {
			var payload uint64
			if payloads != nil {
				payload = payloads[i]
			}
			if err := ub.AddKey(keys[i][:], payload); err != nil {
				_ = ub.Close()
				fmt.Printf("AddKey failed: %v\n", err)
				return
			}
		}
		addKeyDuration = time.Since(buildStart)
		finishStart := time.Now()
		buildErr = ub.Finish()
		finishDuration = time.Since(finishStart)
	}

	buildDuration := time.Since(buildStart)
	fmt.Printf("  AddKey: %.2fs (%.1f M/sec), Finish: %.2fs\n",
		addKeyDuration.Seconds(), float64(numKeys)/addKeyDuration.Seconds()/1e6,
		finishDuration.Seconds())

	// Stop CPU profile after build phase
	if *cpuprofile != "" {
		pprof.StopCPUProfile()
	}

	// Write memory profile after build phase
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			fmt.Printf("could not create memory profile: %v\n", err)
		} else {
			runtime.GC() // Get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				fmt.Printf("could not write memory profile: %v\n", err)
			}
			_ = f.Close()
		}
	}

	close(done)

	// Final memory samples
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	if final.Alloc > peakAlloc.Load() {
		peakAlloc.Store(final.Alloc)
	}
	finalRSS := getMaxRSS()
	if finalRSS > peakRSS.Load() {
		peakRSS.Store(finalRSS)
	}

	peakHeapMem := peakAlloc.Load() - baseline.Alloc
	peakRSSMem := peakRSS.Load() - baselineRSS

	if buildErr != nil {
		fmt.Printf("Build failed: %v\n", buildErr)
		return
	}

	info, _ := os.Stat(indexPath)
	fileSize := info.Size()
	bitsPerKey := float64(fileSize*8) / float64(numKeys)
	payloadBits := float64(payloadSize * 8)
	fingerprintBits := float64(fpSize * 8)
	mphfOverhead := bitsPerKey - payloadBits - fingerprintBits

	idx, err := streamhash.Open(indexPath)
	if err != nil {
		fmt.Printf("Open failed: %v\n", err)
		return
	}
	defer func() { _ = idx.Close() }()

	// Open as PayloadIndex if payloads are present
	var pidx *streamhash.PayloadIndex
	if payloadSize > 0 {
		pidx, err = idx.WithPayload()
		if err != nil {
			fmt.Printf("WithPayload failed: %v\n", err)
			return
		}
	}

	// Randomize query order to ensure consistent access patterns
	// regardless of whether keys were sorted or not
	queryOrder := mrand.Perm(numKeys)

	fmt.Println("Warming up queries...")
	for i := range 10000 {
		if pidx != nil {
			_, _, _ = pidx.QueryPayload(keys[queryOrder[i%numKeys]][:])
		} else {
			_, _ = idx.QueryRank(keys[queryOrder[i%numKeys]][:])
		}
	}

	fmt.Println("Benchmarking queries...")
	numQueries := 100000
	queryStart := time.Now()
	for i := range numQueries {
		if pidx != nil {
			_, _, _ = pidx.QueryPayload(keys[queryOrder[i%numKeys]][:])
		} else {
			_, _ = idx.QueryRank(keys[queryOrder[i%numKeys]][:])
		}
	}
	queryDuration := time.Since(queryStart)
	avgLatency := float64(queryDuration.Nanoseconds()) / float64(numQueries) / 1000

	// Print mode header
	modeStr := "sorted"
	if !sortedMode {
		modeStr = "unsorted"
	}
	algoStr := *algoFlag

	fmt.Printf("\n")
	fmt.Printf("╔═════════════════════╦════════════════╦══════════════════╗\n")
	fmt.Printf("║ Mode: %-14s║ Algo: %-8s ║                  ║\n", modeStr, algoStr)
	fmt.Printf("╠═════════════════════╬════════════════╬══════════════════╣\n")
	fmt.Printf("║ Metric              ║ Value          ║ Target           ║\n")
	fmt.Printf("╠═════════════════════╬════════════════╬══════════════════╣\n")
	fmt.Printf("║ Bits per key        ║ %6.3f bits/key║ -                ║\n", bitsPerKey)
	fmt.Printf("║   - Payload         ║ %6.3f bits/key║ (%d bytes)        ║\n", payloadBits, payloadSize)
	fmt.Printf("║   - Fingerprint     ║ %6.3f bits/key║ (%d byte)         ║\n", fingerprintBits, fpSize)
	fmt.Printf("║   - MPHF overhead   ║ %6.3f bits/key║ -                ║\n", mphfOverhead)
	fmt.Printf("║ Query latency       ║ %6.2f μs      ║ -                ║\n", avgLatency)
	fmt.Printf("║ Build time          ║ %6.2f sec     ║ -                ║\n", buildDuration.Seconds())
	fmt.Printf("║ Build throughput    ║ %6.2f M/sec   ║ -                ║\n", float64(numKeys)/buildDuration.Seconds()/1_000_000)
	if sortedMode {
		fmt.Printf("║ Sort time           ║ %6.2f sec     ║ -                ║\n", sortDuration.Seconds())
	} else {
		fmt.Printf("║ Sort time           ║    N/A         ║ (unsorted mode)  ║\n")
	}
	fmt.Printf("║ Hash time           ║ %6.2f sec     ║ -                ║\n", hashDuration.Seconds())
	if sortedMode {
		fmt.Printf("║ Total (sort+build)  ║ %6.2f sec     ║ -                ║\n", (sortDuration + buildDuration).Seconds())
		fmt.Printf("║ Throughput w/ sort  ║ %6.2f M/sec   ║ -                ║\n", float64(numKeys)/(sortDuration+buildDuration).Seconds()/1_000_000)
		fmt.Printf("║ Total (all)         ║ %6.2f sec     ║ -                ║\n", (sortDuration + hashDuration + buildDuration).Seconds())
		fmt.Printf("║ Throughput w/ all   ║ %6.2f M/sec   ║ -                ║\n", float64(numKeys)/(sortDuration+hashDuration+buildDuration).Seconds()/1_000_000)
	} else {
		fmt.Printf("║ Total (build+hash)  ║ %6.2f sec     ║ -                ║\n", (hashDuration + buildDuration).Seconds())
		fmt.Printf("║ Throughput w/ hash  ║ %6.2f M/sec   ║ -                ║\n", float64(numKeys)/(hashDuration+buildDuration).Seconds()/1_000_000)
	}
	fmt.Printf("║ Peak heap memory    ║ %6.1f MB      ║ -                ║\n", float64(peakHeapMem)/1_000_000)
	fmt.Printf("║ Peak RSS memory     ║ %6.1f MB      ║ -                ║\n", float64(peakRSSMem)/1_000_000)
	fmt.Printf("╚═════════════════════╩════════════════╩══════════════════╝\n")

}
