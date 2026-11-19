package streamhash

import (
	"context"
	"errors"
	"fmt"
	"os"
)

const (
	// mergeBatchRecords is the records per batch handed to the goroutine that writes.
	mergeBatchRecords = 4096
	// mergeReadBuffer is each input's read buffer.
	mergeReadBuffer = 128 << 10
	// mergePoolDepth and mergeChanDepth size the batch pool and the hand-off channel.
	mergePoolDepth = 3
	mergeChanDepth = 2
)

// MergeSortedFiles merges the sorted files at inputs, which must all have
// cfg.PayloadSize, into one at output, written with cfg. It is for a writer
// that spilled sorted pieces. It keeps every input open, with a 128 KiB read
// buffer each. An input whose keys are out of order fails the merge, and a
// failed or canceled merge removes the output.
func MergeSortedFiles(ctx context.Context, output string, inputs []string, cfg SortedFileConfig) error {
	return mergeSortedFiles(ctx, output, inputs, cfg, mergeReadBuffer)
}

func mergeSortedFiles(ctx context.Context, output string, inputs []string, cfg SortedFileConfig, bufBytes int) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	runs, payloadSize, total, err := readSortedRuns(inputs)
	if err != nil {
		return err
	}
	if len(inputs) > 0 && payloadSize != cfg.PayloadSize {
		return fmt.Errorf("the sorted files have %d-byte payloads, the merge's are %d bytes", payloadSize, cfg.PayloadSize)
	}
	if err := checkOutputNotInput(output, inputs); err != nil {
		return err
	}
	w, err := NewSortedFileWriter(output, cfg)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := w.Close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	if total > 0 {
		if err := mergeRuns(ctx, runs, w, bufBytes); err != nil {
			return err
		}
	}
	if w.count != total {
		return fmt.Errorf("%w: merged %d records, the inputs count %d", ErrKeyCountMismatch, w.count, total)
	}
	return w.Finish()
}

// mergeRuns merges runs into w. One goroutine merges them through a heap and
// hands pooled batches of records to this one, which writes them. The first
// error cancels both.
func mergeRuns(ctx context.Context, runs []sortedRun, w *SortedFileWriter, bufBytes int) error {
	ctx, fail := context.WithCancelCause(ctx)
	defer fail(nil)
	out, pool := make(chan *[]byte, mergeChanDepth), newBatchPool(w.recordSize)
	go func() {
		defer close(out)
		if err := mergeFiles(ctx, runs, w.recordSize, bufBytes, out, pool); err != nil {
			fail(err)
		}
	}()
	// Drain out until it closes, so the merge goroutine has closed its files
	// by the time this returns.
	for batch := range out {
		if ctx.Err() == nil {
			if err := w.appendEncoded(*batch); err != nil {
				fail(err)
			}
		}
		pool <- batch // never blocks: the pool has room for every batch
	}
	return context.Cause(ctx)
}

// newBatchPool returns a pool of empty batches. They travel as pointers:
// []byte channels put the heap loop's slice temporaries on the stack.
func newBatchPool(recordSize int) chan *[]byte {
	pool := make(chan *[]byte, mergePoolDepth)
	for range mergePoolDepth {
		b := make([]byte, mergeBatchRecords*recordSize)
		pool <- &b
	}
	return pool
}

// mergeFiles merges runs through one heap into batches of records on out,
// taking empty batches from pool. It stops at the next batch once ctx is
// canceled.
func mergeFiles(ctx context.Context, runs []sortedRun, recordSize, bufBytes int, out chan<- *[]byte, pool <-chan *[]byte) error {
	readers := make([]*runReader, 0, len(runs))
	defer func() {
		for _, r := range readers {
			r.f.Close()
		}
	}()

	h := make([]mergeEntry, 0, len(runs))
	for _, run := range runs {
		r, err := newRunReader(run, recordSize, bufBytes)
		if err != nil {
			return err
		}
		readers = append(readers, r)
		h = append(h, mergeEntry{key: r.key(), idx: len(readers) - 1})
	}
	n := len(h)
	for i := n/2 - 1; i >= 0; i-- {
		siftDown(h, i, n)
	}

	bp := <-pool
	batch, pos := *bp, 0
	for n > 0 {
		r := readers[h[0].idx]
		pos += copy(batch[pos:], r.record())
		if pos == len(batch) {
			out <- bp
			if err := ctx.Err(); err != nil {
				return err
			}
			bp = <-pool
			batch, pos = *bp, 0
		}

		if r.advance() {
			k := r.key()
			if k.less(h[0].key) {
				return fmt.Errorf("sorted file %s: %w", r.f.Name(), ErrUnsortedInput)
			}
			h[0].key = k
			siftDown(h, 0, n)
		} else {
			if r.err != nil {
				return r.err
			}
			n--
			if n > 0 {
				h[0] = h[n]
				siftDown(h, 0, n)
			}
		}
	}
	if pos > 0 {
		*bp = batch[:pos]
		out <- bp
	}
	return nil
}

// mergeEntry is one input's heap slot: its current key and its index in the readers.
type mergeEntry struct {
	key sortKey
	idx int
}

// siftDown restores the min-heap rooted at i over h[:n].
func siftDown(h []mergeEntry, i, n int) {
	for {
		left := 2*i + 1
		if left >= n {
			break
		}
		j := left
		if right := left + 1; right < n && h[right].key.less(h[j].key) {
			j = right
		}
		if !h[j].key.less(h[i].key) {
			break
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

// runReader reads one input's records directly into an aligned buffer and
// slices them out of it, with no per-record parse or allocation.
type runReader struct {
	run        sortedRun
	f          *os.File
	buf        []byte // aligned for direct reads
	recordSize int
	cursor     int   // offset in buf of the current record
	valid      int   // bytes of buf that hold records
	fileOff    int64 // file offset of buf[0], a multiple of directAlign
	err        error
}

// newRunReader opens run, which must have records, and reads its first.
func newRunReader(run sortedRun, recordSize, bufBytes int) (*runReader, error) {
	// A refill rereads up to directAlign-1 bytes, so the buffer holds at
	// least two aligned blocks.
	bufBytes = max(bufBytes, 2*directAlign)
	bufBytes += (directAlign - bufBytes%directAlign) % directAlign
	f, err := openDirect(run.path)
	if err != nil {
		return nil, err
	}
	r := &runReader{run: run, f: f, buf: alignedBuffer(bufBytes), recordSize: recordSize}
	if !r.fill(run.start) {
		f.Close()
		return nil, r.err
	}
	return r, nil
}

func (r *runReader) record() []byte { return r.buf[r.cursor : r.cursor+r.recordSize] }
func (r *runReader) key() sortKey   { return sortKeyOf(r.buf[r.cursor:]) }

// advance moves to the next record and reports whether there is one. At an
// error it sets r.err.
func (r *runReader) advance() bool {
	r.cursor += r.recordSize
	return r.cursor+r.recordSize <= r.valid || r.fill(r.fileOff+int64(r.cursor))
}

// fill reads the buffer from the aligned offset at or below off, the file
// offset of the next record, and reports whether that record is there.
func (r *runReader) fill(off int64) bool {
	if off == r.run.end {
		return false
	}
	alignedOff := off &^ (directAlign - 1)
	if r.valid, r.err = r.run.readAt(r.f, r.buf, alignedOff); r.err != nil {
		return false
	}
	r.fileOff, r.cursor = alignedOff, int(off-alignedOff)
	return true
}
