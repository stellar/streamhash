package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/streamhash"
)

const (
	// Entry file format: [16-byte key][4-byte payload] = 20 bytes per entry.
	// StreamHash only uses the first 16 bytes of a key (two uint64s).
	// Files have an 8-byte LE header with the entry count.
	benchEntrySize = 20
	keySize        = 16
	payloadBytes   = 4
	maxFanIn       = 4 // max fan-in for intermediate merge levels
)

func mainBenchFiles() {
	mode := flag.String("mode", "", "sorted or unsorted")
	dir := flag.String("dir", "", "input directory of entry files (required)")
	out := flag.String("out", "", "output index file (required)")
	workers := flag.Int("workers", runtime.NumCPU()/2, "parallel workers for block building")
	tmpdir := flag.String("tmpdir", "", "temp dir for unsorted mode partition files")
	bufsize := flag.Int("bufsize", 32768, "per-file read buffer size in bytes")
	mergers := flag.Int("mergers", 32, "number of leaf merge goroutines for sorted mode")
	fp := flag.Int("fp", 2, "fingerprint size in bytes")
	cw := flag.Int("cw", 0, "number of concurrent writers for unsorted mode (0 = single-threaded AddKey)")
	algo := flag.String("algo", "bijection", "algorithm: bijection or ptrhash")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	if *mode != "sorted" && *mode != "unsorted" {
		fmt.Fprintf(os.Stderr, "Usage: bench bench-files -mode sorted|unsorted -dir <dir> -out <file>\n")
		os.Exit(1)
	}
	if *dir == "" || *out == "" {
		fmt.Fprintf(os.Stderr, "Usage: bench bench-files -mode sorted|unsorted -dir <dir> -out <file>\n")
		os.Exit(1)
	}

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cpuprofile: %v\n", err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer f.Close()
		defer pprof.StopCPUProfile()
	}

	fmt.Printf("Mode: %s, Dir: %s, Out: %s, Workers: %d, BufSize: %d\n",
		*mode, *dir, *out, *workers, *bufsize)

	files, err := filepath.Glob(filepath.Join(*dir, "*.bin"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "glob: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Found %d files\n", len(files))

	totalKeys, err := scanHeaders(files)
	if err != nil {
		fmt.Fprintf(os.Stderr, "scan headers: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Total keys: %d\n", totalKeys)

	evictFromCache(files)

	var algoID streamhash.Algorithm
	switch *algo {
	case "bijection":
		algoID = streamhash.AlgoBijection
	case "ptrhash":
		algoID = streamhash.AlgoPTRHash
	default:
		fmt.Fprintf(os.Stderr, "Unknown algorithm: %s (use 'bijection' or 'ptrhash')\n", *algo)
		os.Exit(1)
	}

	opts := []streamhash.BuildOption{
		streamhash.WithPayload(payloadBytes),
		streamhash.WithFingerprint(*fp),
		streamhash.WithWorkers(*workers),
		streamhash.WithAlgorithm(algoID),
	}

	var peakHeap atomic.Uint64
	var peakRSS atomic.Uint64
	memDone := make(chan struct{})
	go func() {
		samples := []metrics.Sample{
			{Name: "/memory/classes/heap/objects:bytes"},
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-memDone:
				return
			case <-ticker.C:
				metrics.Read(samples)
				heap := samples[0].Value.Uint64()
				for {
					old := peakHeap.Load()
					if heap <= old || peakHeap.CompareAndSwap(old, heap) {
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

	start := time.Now()
	var keysAdded uint64

	switch *mode {
	case "sorted":
		sb, err := streamhash.NewSortedBuilder(context.Background(), *out, totalKeys, opts...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewSortedBuilder: %v\n", err)
			os.Exit(1)
		}
		keysAdded, err = buildSorted(sb, files, *bufsize, *mergers)
		if err != nil {
			sb.Close()
			fmt.Fprintf(os.Stderr, "build: %v\n", err)
			os.Exit(1)
		}
		if err := sb.Finish(); err != nil {
			fmt.Fprintf(os.Stderr, "Finish: %v\n", err)
			os.Exit(1)
		}
	case "unsorted":
		if *tmpdir == "" {
			*tmpdir, err = os.MkdirTemp("", "bench-unsorted-")
			if err != nil {
				fmt.Fprintf(os.Stderr, "mkdtemp: %v\n", err)
				os.Exit(1)
			}
			defer os.RemoveAll(*tmpdir)
		} else {
			os.MkdirAll(*tmpdir, 0755)
		}
		ub, err := streamhash.NewUnsortedBuilder(context.Background(), *out, totalKeys, *tmpdir, opts...)
		if err != nil {
			fmt.Fprintf(os.Stderr, "NewUnsortedBuilder: %v\n", err)
			os.Exit(1)
		}
		if *cw > 0 {
			keysAdded, err = buildUnsortedConcurrent(ub, files, *bufsize, *cw)
			if err != nil {
				ub.Close()
				fmt.Fprintf(os.Stderr, "build: %v\n", err)
				os.Exit(1)
			}
		} else {
			keysAdded, err = buildUnsorted(ub, files, *bufsize)
			if err != nil {
				ub.Close()
				fmt.Fprintf(os.Stderr, "build: %v\n", err)
				os.Exit(1)
			}
			if err := ub.Finish(); err != nil {
				fmt.Fprintf(os.Stderr, "Finish: %v\n", err)
				os.Exit(1)
			}
		}
	}

	totalDone := time.Since(start)
	close(memDone)

	fmt.Printf("Total build: %d keys in %.1fs (%.2f M/sec)\n",
		keysAdded, totalDone.Seconds(), float64(keysAdded)/totalDone.Seconds()/1e6)

	printBenchFilesReport(*out)
	fmt.Printf("Peak heap:   %.1f MB\n", float64(peakHeap.Load())/(1<<20))
	fmt.Printf("Peak RSS:    %.1f MB\n", float64(peakRSS.Load())/(1<<20))
}

// --- Header scanning ---

func scanHeaders(files []string) (uint64, error) {
	var total atomic.Uint64
	var firstErr atomic.Value

	var wg sync.WaitGroup
	for _, path := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f, err := os.Open(path)
			if err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			defer f.Close()
			var header [8]byte
			if _, err := io.ReadFull(f, header[:]); err != nil {
				firstErr.CompareAndSwap(nil, err)
				return
			}
			total.Add(binary.LittleEndian.Uint64(header[:]))
		}()
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		return 0, v.(error)
	}
	return total.Load(), nil
}

// --- Sorted mode ---

func buildSorted(builder *streamhash.SortedBuilder, files []string, bufsize int, numMergers int) (uint64, error) {
	G := numMergers
	if G < 1 {
		G = 1
	}
	filesPerGroup := (len(files) + G - 1) / G
	var streams []*streamReader
	for i := 0; i < len(files); i += filesPerGroup {
		end := min(i+filesPerGroup, len(files))
		streams = append(streams, launchMergeStream(files[i:end], bufsize))
	}
	fmt.Printf("Merge tree: %d leaf goroutines (K=%d each)\n", len(streams), filesPerGroup)

	for len(streams) > maxFanIn {
		var nextLevel []*streamReader
		for i := 0; i < len(streams); i += maxFanIn {
			end := min(i+maxFanIn, len(streams))
			group := streams[i:end]
			if len(group) == 1 {
				nextLevel = append(nextLevel, group[0])
			} else {
				nextLevel = append(nextLevel, launchFinalMerge(group))
			}
		}
		fmt.Printf("Merge tree: %d streams at next level\n", len(nextLevel))
		streams = nextLevel
	}

	finalCh := make(chan *mergeBatch, 2)
	finalPool := make(chan *mergeBatch, 3)
	for range 3 {
		finalPool <- &mergeBatch{}
	}
	go finalMerge(streams, finalCh, finalPool)

	var keysAdded uint64
	startTime := time.Now()
	lastReport := startTime

	for batch := range finalCh {
		data := batch.data[:batch.count*benchEntrySize]
		for off := 0; off < len(data); off += benchEntrySize {
			entry := data[off : off+benchEntrySize]
			payload := uint64(binary.LittleEndian.Uint32(entry[keySize:]))
			if err := builder.AddKey(entry[:keySize], payload); err != nil {
				return keysAdded, fmt.Errorf("AddKey: %w", err)
			}
			keysAdded++
		}
		finalPool <- batch

		if now := time.Now(); now.Sub(lastReport) >= 5*time.Second {
			fmt.Printf("  sorted: %d keys added (%.2f M/sec)\n",
				keysAdded, float64(keysAdded)/now.Sub(startTime).Seconds()/1e6)
			lastReport = now
		}
	}

	return keysAdded, nil
}

// --- Unsorted mode: parallel file readers ---

func unsortedReader(files []string, bufsize int, out chan<- *mergeBatch, pool chan *mergeBatch) {
	for _, path := range files {
		r, err := newFileReader(path, bufsize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "unsortedReader: open %s: %v\n", path, err)
			continue
		}
		if !r.prepareFirst() {
			r.close()
			continue
		}

		batch := <-pool
		pos := 0
		for {
			off := pos * benchEntrySize
			copy(batch.data[off:off+benchEntrySize], r.entry())
			pos++

			if pos == mergeBatchSize {
				batch.count = pos
				out <- batch
				batch = <-pool
				pos = 0
			}

			if !r.advance() {
				break
			}
		}
		if pos > 0 {
			batch.count = pos
			out <- batch
		}
		r.close()
	}
}

func buildUnsorted(builder *streamhash.UnsortedBuilder, files []string, bufsize int) (uint64, error) {
	var keysAdded uint64
	startTime := time.Now()
	lastReport := startTime

	bufsize = (bufsize / benchEntrySize) * benchEntrySize

	numReaders := 8
	ch := make(chan *mergeBatch, numReaders*2)
	pool := make(chan *mergeBatch, numReaders*3)
	for range numReaders * 3 {
		pool <- &mergeBatch{}
	}

	var wg sync.WaitGroup
	filesPerReader := (len(files) + numReaders - 1) / numReaders
	for i := 0; i < len(files); i += filesPerReader {
		end := min(i+filesPerReader, len(files))
		wg.Add(1)
		go func(subset []string) {
			defer wg.Done()
			unsortedReader(subset, bufsize, ch, pool)
		}(files[i:end])
	}
	go func() {
		wg.Wait()
		close(ch)
	}()

	for batch := range ch {
		data := batch.data[:batch.count*benchEntrySize]
		for off := 0; off < len(data); off += benchEntrySize {
			entry := data[off : off+benchEntrySize]
			payload := uint64(binary.LittleEndian.Uint32(entry[keySize:]))
			if err := builder.AddKey(entry[:keySize], payload); err != nil {
				return keysAdded, fmt.Errorf("AddKey: %w", err)
			}
			keysAdded++
		}
		pool <- batch

		if now := time.Now(); now.Sub(lastReport) >= 5*time.Second {
			fmt.Printf("  unsorted: %d keys (%.2f M/sec)\n",
				keysAdded, float64(keysAdded)/now.Sub(startTime).Seconds()/1e6)
			lastReport = now
		}
	}

	return keysAdded, nil
}

// --- Unsorted mode: concurrent writers ---

func buildUnsortedConcurrent(builder *streamhash.UnsortedBuilder, files []string, bufsize int, numWriters int) (uint64, error) {
	bufsize = (bufsize / benchEntrySize) * benchEntrySize

	filesPerWriter := (len(files) + numWriters - 1) / numWriters

	var totalKeys atomic.Uint64
	startTime := time.Now()

	var progressKeys atomic.Uint64
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				k := progressKeys.Load()
				elapsed := time.Since(startTime)
				fmt.Printf("  unsorted-cw: %d keys (%.2f M/sec)\n",
					k, float64(k)/elapsed.Seconds()/1e6)
			}
		}
	}()

	err := builder.AddKeys(numWriters, func(writerID int, addKey func([]byte, uint64) error) error {
		start := writerID * filesPerWriter
		end := min(start+filesPerWriter, len(files))
		if start >= len(files) {
			return nil
		}
		var localKeys uint64
		for _, path := range files[start:end] {
			r, err := newFileReader(path, bufsize)
			if err != nil {
				return err
			}
			if !r.prepareFirst() {
				r.close()
				continue
			}
			for {
				entry := r.entry()
				payload := uint64(binary.LittleEndian.Uint32(entry[keySize:]))
				if err := addKey(entry[:keySize], payload); err != nil {
					r.close()
					return err
				}
				localKeys++
				if localKeys&0xFFFF == 0 {
					progressKeys.Add(0x10000)
				}
				if !r.advance() {
					break
				}
			}
			r.close()
		}
		progressKeys.Add(localKeys & 0xFFFF)
		totalKeys.Add(localKeys)
		return nil
	})
	close(progressDone)

	return totalKeys.Load(), err
}

// --- Utilities ---

func printBenchFilesReport(indexPath string) {
	idx, err := streamhash.Open(indexPath)
	if err != nil {
		fmt.Printf("Warning: could not open index for stats: %v\n", err)
		return
	}
	defer idx.Close()

	stats := idx.Stats()
	fi, _ := os.Stat(indexPath)

	fmt.Printf("\n--- Index Stats ---\n")
	fmt.Printf("NumKeys:     %d\n", stats.NumKeys)
	fmt.Printf("NumBlocks:   %d\n", stats.NumBlocks)
	fmt.Printf("BitsPerKey:  %.3f\n", stats.BitsPerKey)
	fmt.Printf("PayloadSize: %d bytes\n", stats.PayloadSize)
	fmt.Printf("Fingerprint: %v\n", stats.FingerprintSize > 0)
	if fi != nil {
		fmt.Printf("FileSize:    %.2f GB\n", float64(fi.Size())/(1<<30))
	}
}
