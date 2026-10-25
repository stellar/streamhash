package streamhash

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sync"

	intbits "github.com/stellar/streamhash/internal/bits"
)

const (
	// sortedBuildReadBudget is the memory for read buffers, and
	// sortedBuildStripeBudget for the stripes in flight.
	sortedBuildReadBudget   = 128 << 20
	sortedBuildStripeBudget = 128 << 20

	// A read buffer is between these sizes.
	sortedBuildMinBuffer = 64 << 10
	sortedBuildMaxBuffer = 4 << 20

	// sortedBuildReadGoroutines fill the read buffers.
	sortedBuildReadGoroutines = 16

	// A stripe holds about this many keys, within these bounds.
	sortedBuildMinStripeKeys = 32 << 10
	sortedBuildMaxStripeKeys = 256 << 10

	// sortedBuildStripesWaiting is how many gathered stripes may wait for a
	// worker, beyond the one each worker is solving.
	sortedBuildStripesWaiting = 2
)

// BuildFromSortedFiles builds the index at output from the sorted files at
// inputs. The key count is the sum of their record counts, and their payload
// size is the index's. The options are NewSortedBuilder's; WithPayload is not
// needed, and must agree with the files if given. The index is byte-identical
// to one built by adding the same keys to a SortedBuilder in block order.
//
// The files are not merged. Each is checked, then read once, sequentially and
// directly where the filesystem allows it. A file whose keys are out of order
// fails the build, and so does a duplicate key.
//
// Speed: about 6M keys/s per worker (WithWorkers), up to what the disk reads
// at 16+PayloadSize bytes per key. Memory: about 256 MiB, plus each worker's
// block state (about 0.5 MB with bijection, 5 MB with PTRHash), whatever the
// number of keys or files. The build keeps one file open per input.
func BuildFromSortedFiles(ctx context.Context, output string, inputs []string, opts ...BuildOption) error {
	return buildFromSortedFiles(ctx, output, inputs, sortedBuildTuning{}, opts...)
}

// sortedBuildTuning overrides the build's sizing; zero values mean the
// defaults. Tests use it to force stripe and buffer sizes, and to act once a
// stripe is gathered.
type sortedBuildTuning struct {
	stripeKeys  int
	bufBytes    int
	afterGather func(s int)
}

func buildFromSortedFiles(ctx context.Context, output string, inputs []string, tune sortedBuildTuning, opts ...BuildOption) error {
	if len(inputs) == 0 {
		return errors.New("no sorted files to build from")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	runs, payloadSize, total, err := readSortedRuns(inputs)
	if err != nil {
		return err
	}
	if err := checkOutputNotInput(output, inputs); err != nil {
		return err
	}
	cfg := newBuildConfig(opts)
	if cfg.payloadSet && cfg.payloadSize != payloadSize {
		return fmt.Errorf("the sorted files have %d-byte payloads, WithPayload asks for %d", payloadSize, cfg.payloadSize)
	}
	cfg.payloadSize = payloadSize

	// The build's own context lets close stop the reads at once after a failure.
	ctx, cancel := context.WithCancel(ctx)
	b, err := newBuilder(ctx, output, total, cfg)
	if err != nil {
		cancel()
		return err
	}
	g := &sortedBuild{b: b, recordSize: MinKeySize + payloadSize, tune: tune}
	defer func() {
		cancel()
		g.close()
	}()
	if err := g.start(ctx, runs); err != nil {
		return errors.Join(err, b.cleanup())
	}
	if b.workers > 1 {
		return g.buildParallel()
	}
	return g.buildInline(ctx)
}

// sortedBuild is the state of one BuildFromSortedFiles call.
type sortedBuild struct {
	b          *builder
	tune       sortedBuildTuning
	recordSize int
	geo        stripeGeometry
	inFlight   int // stripes gathered at a time: one per worker, plus those waiting
	stripeCap  int // records a stripe buffer is sized for
	readers    []*fileReader
	reqs       chan readRequest
	readWg     sync.WaitGroup
	gathered   uint64
}

// start sizes the stripes and buffers, opens the runs and starts reading
// them, with reads served until ctx is done.
func (g *sortedBuild) start(ctx context.Context, runs []sortedRun) error {
	b := g.b
	var err error
	g.inFlight = b.workers + sortedBuildStripesWaiting
	stripeKeys := sortedBuildStripeKeys(g.inFlight, g.recordSize, g.tune.stripeKeys)
	if g.geo, err = newStripeGeometry(b.numBlocks, b.cfg.totalKeys, stripeKeys); err != nil {
		return err
	}
	perStripe := int(b.cfg.totalKeys/uint64(b.numBlocks)) * int(g.geo.blocksPerStripe)
	g.stripeCap = perStripe + perStripe/16 + 1024
	if len(runs) == 0 {
		return nil
	}

	bufSize := sortedBuildBufferSize(len(runs), g.recordSize, g.tune.bufBytes)
	nbuf := 2
	if 3*len(runs)*bufSize <= sortedBuildReadBudget {
		nbuf = 3
	}
	g.reqs = make(chan readRequest, len(runs)*nbuf)
	for _, run := range runs {
		f, err := openDirect(run.path)
		if err != nil {
			return err
		}
		r := &fileReader{run: run, f: f, reqs: g.reqs, next: run.start, valid: -1}
		for range nbuf {
			r.bufs = append(r.bufs, alignedBuffer(bufSize))
			r.done = append(r.done, make(chan readResult, 1))
		}
		g.readers = append(g.readers, r)
	}
	for range min(sortedBuildReadGoroutines, len(runs)*nbuf) {
		g.readWg.Go(func() { serveReads(ctx, g.reqs) })
	}
	for _, r := range g.readers {
		for i := range r.bufs {
			r.post(i)
		}
	}
	return nil
}

// close stops the reads and closes the files. The workers and the writer are
// already stopped: the build drained them or cleaned up.
func (g *sortedBuild) close() {
	if g.reqs != nil {
		close(g.reqs)
	}
	g.readWg.Wait()
	for _, r := range g.readers {
		r.f.Close()
	}
}

// sortedBuildStripeKeys returns how many keys a stripe holds: as many as let
// inFlight stripes, a gathered record and a routed entry per key, fit in the
// budget, within the bounds.
func sortedBuildStripeKeys(inFlight, recordSize, stripeKeys int) int {
	if stripeKeys > 0 {
		return stripeKeys
	}
	perKey := recordSize + readEntrySize
	return min(max(sortedBuildStripeBudget/(inFlight*perKey), sortedBuildMinStripeKeys), sortedBuildMaxStripeKeys)
}

// sortedBuildBufferSize returns the read buffer size for files of
// recordSize-byte records: the budget shared between two buffers per file,
// within the limits, and a multiple of both directAlign and recordSize, so
// that every buffer starts and ends at a record.
func sortedBuildBufferSize(files, recordSize, bufBytes int) int {
	if bufBytes <= 0 {
		bufBytes = min(max(sortedBuildReadBudget/2/files, sortedBuildMinBuffer), sortedBuildMaxBuffer)
	}
	// directAlign is a power of two, so this is lcm(directAlign, recordSize).
	unit := directAlign * (recordSize / (recordSize & -recordSize))
	return max(bufBytes/unit, 1) * unit
}

// stripeGeometry splits the index's blocks into stripes of consecutive
// blocks, each of about the same number of keys.
type stripeGeometry struct {
	numBlocks       uint32
	blocksPerStripe uint32
	numStripes      int
}

// newStripeGeometry makes stripes of about stripeKeys keys, and checks that
// every stripe's first prefix is the first FastRange32 maps to its first
// block, so that no block spans two stripes.
func newStripeGeometry(numBlocks uint32, totalKeys uint64, stripeKeys int) (stripeGeometry, error) {
	per := uint64(numBlocks)
	if totalKeys > 0 {
		per = min(max(uint64(stripeKeys)*uint64(numBlocks)/totalKeys, 1), uint64(numBlocks))
	}
	geo := stripeGeometry{numBlocks: numBlocks, blocksPerStripe: uint32(per), numStripes: int((uint64(numBlocks) + per - 1) / per)}
	for s := 1; s < geo.numStripes; s++ {
		first, _ := geo.blocks(s)
		p := lowPrefix(first, numBlocks)
		if intbits.FastRange32(p, numBlocks) != first || intbits.FastRange32(p-1, numBlocks) != first-1 {
			return geo, fmt.Errorf("stripe %d: prefix %#x is not the first of block %d of %d", s, p, first, numBlocks)
		}
	}
	return geo, nil
}

// blocks returns stripe s's first block and how many blocks it has.
func (geo stripeGeometry) blocks(s int) (first, count uint32) {
	first = uint32(s) * geo.blocksPerStripe
	return first, min(geo.blocksPerStripe, geo.numBlocks-first)
}

// bound returns the first prefix past stripe s, unless s is the last stripe.
func (geo stripeGeometry) bound(s int) (uint64, bool) {
	if s+1 >= geo.numStripes {
		return 0, false
	}
	first, _ := geo.blocks(s + 1)
	return lowPrefix(first, geo.numBlocks), true
}

// lowPrefix returns the smallest prefix FastRange32 maps to block b of n:
// ceil(b * 2^64 / n).
func lowPrefix(b, n uint32) uint64 {
	q, r := bits.Div64(uint64(b), 0, uint64(n))
	if r != 0 {
		q++
	}
	return q
}

// A stripe is the records of a run of consecutive blocks, gathered from
// every file.
type stripe struct {
	first      uint32 // its first block
	blocks     uint32 // how many blocks it has
	keysBefore uint64 // the keys of the stripes before it
	data       []byte // its records, file by file
	runs       []int  // how many records came from each file
}

func (g *sortedBuild) newStripe() *stripe {
	return &stripe{data: make([]byte, 0, g.stripeCap*g.recordSize), runs: make([]int, len(g.readers))}
}

// gather copies stripe s's run out of every file into st.
func (g *sortedBuild) gather(ctx context.Context, s int, st *stripe) error {
	bound, bounded := g.geo.bound(s)
	st.first, st.blocks = g.geo.blocks(s)
	st.keysBefore = g.gathered
	st.data = st.data[:0]
	recordSize := g.recordSize
	for i, r := range g.readers {
		count := 0
		for {
			ok, err := r.ready(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			rest := r.bufs[r.cur][r.pos:r.valid]
			n := len(rest) / recordSize
			cut := n
			if bounded {
				cut = firstAtOrPast(rest, recordSize, n, bound)
			}
			st.data = append(st.data, rest[:cut*recordSize]...)
			r.pos += cut * recordSize
			count += cut
			if cut < n {
				break
			}
			r.advance()
		}
		st.runs[i] = count
		g.gathered += uint64(count)
	}
	if g.tune.afterGather != nil {
		g.tune.afterGather(s)
	}
	return nil
}

// firstAtOrPast returns the index of the first of the n records in buf whose
// prefix is at or past bound, or n. It scans the first records in order,
// since with many files a run is a few dozen records, then gallops.
func firstAtOrPast(buf []byte, recordSize, n int, bound uint64) int {
	prefix := func(i int) uint64 { return binary.BigEndian.Uint64(buf[i*recordSize:]) }
	i := 0
	for ; i < min(n, 64); i++ {
		if prefix(i) >= bound {
			return i
		}
	}
	if i == n {
		return n
	}
	lo, hi := i-1, i
	for hi < n && prefix(hi) < bound {
		lo, hi = hi, hi*2
	}
	hi = min(hi, n)
	for lo+1 < hi {
		mid := (lo + hi) / 2
		if prefix(mid) < bound {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi
}

// checkCount checks that the gathered records add up to the files' counts.
func (g *sortedBuild) checkCount() error {
	if g.gathered != g.b.cfg.totalKeys {
		return fmt.Errorf("%w: read %d records, the sorted files count %d", ErrKeyCountMismatch, g.gathered, g.b.cfg.totalKeys)
	}
	return nil
}

// buildInline gathers, routes, solves and commits each stripe in turn on the
// calling goroutine.
func (g *sortedBuild) buildInline(ctx context.Context) error {
	b := g.b
	st, router := g.newStripe(), g.newStripeRouter()
	for s := range g.geo.numStripes {
		if err := g.buildStripeInline(ctx, s, st, router); err != nil {
			return errors.Join(err, b.cleanup())
		}
	}
	if err := g.checkCount(); err != nil {
		return errors.Join(err, b.cleanup())
	}
	return b.finalizeIndex()
}

// buildStripeInline gathers stripe s into st, routes it, and solves and
// commits its blocks, the empty ones included.
func (g *sortedBuild) buildStripeInline(ctx context.Context, s int, st *stripe, router *stripeRouter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := g.gather(ctx, s, st); err != nil {
		return err
	}
	flat, ends, err := router.route(st)
	if err != nil {
		return err
	}
	b, start := g.b, 0
	for _, end := range ends {
		b.loadBlock(b.builder, flat[start:end])
		if err := b.buildBlock(); err != nil {
			return err
		}
		start = end
	}
	return nil
}

// buildParallel gathers stripes for the workers, which route and solve them
// and hand their blocks to the writer.
func (g *sortedBuild) buildParallel() error {
	b := g.b
	free := make(chan *stripe, g.inFlight)
	for range g.inFlight {
		free <- g.newStripe()
	}
	stripes := make(chan *stripe, g.inFlight)
	b.startPipeline(func() error { return g.runStripeWorker(stripes, free) })
	err := g.gatherAll(b.workerCtx, stripes, free)
	if err != nil && b.workerCtx.Err() == nil {
		// The gather failed on its own, so stop the pipeline.
		return errors.Join(err, b.cleanup())
	}
	// All gathered, or the pipeline failed or the build was canceled: the
	// drain reports the cause, and never finalizes a canceled pipeline.
	return b.drainParallelPipeline()
}

// gatherAll gathers every stripe into a buffer from free and queues it on
// stripes, which it closes when it returns: at the end, at the first error,
// or once ctx is done.
func (g *sortedBuild) gatherAll(ctx context.Context, stripes chan<- *stripe, free <-chan *stripe) error {
	defer close(stripes)
	for s := range g.geo.numStripes {
		var st *stripe
		select {
		case st = <-free:
		case <-ctx.Done():
			return ctx.Err()
		}
		if err := g.gather(ctx, s, st); err != nil {
			return err
		}
		stripes <- st // never blocks: inFlight stripes exist and stripes holds inFlight
	}
	return g.checkCount()
}

// runStripeWorker routes and solves stripes until stripes closes or the
// workers' context is done.
func (g *sortedBuild) runStripeWorker(stripes <-chan *stripe, free chan<- *stripe) error {
	b := g.b
	solver, err := b.newBlockSolver()
	if err != nil {
		return err
	}
	router := g.newStripeRouter()
	for {
		var st *stripe
		select {
		case s, ok := <-stripes:
			if !ok {
				return nil
			}
			st = s
		case <-b.workerCtx.Done():
			return b.workerCtx.Err()
		}
		flat, ends, err := router.route(st)
		if err != nil {
			return err
		}
		first, keysBefore := st.first, st.keysBefore
		free <- st // never blocks: the pool has room for every stripe
		start := 0
		for i, end := range ends {
			b.loadBlock(solver.blk, flat[start:end])
			if err := b.sendResult(solver.build(first+uint32(i), keysBefore+uint64(start))); err != nil {
				return err
			}
			start = end
		}
	}
}

// stripeRouter sorts a stripe's records by block.
type stripeRouter struct {
	g    *sortedBuild
	ends []int
	flat []routedEntry
}

func (g *sortedBuild) newStripeRouter() *stripeRouter {
	return &stripeRouter{g: g, ends: make([]int, g.geo.blocksPerStripe+1), flat: make([]routedEntry, 0, g.stripeCap)}
}

// route sorts st's records by block, by a counting sort, into flat, and
// returns flat and where each block ends in it: block i of the stripe is
// flat[ends[i-1]:ends[i]], with ends[-1] taken as 0. A record out of order
// in its file's run, or outside the stripe, fails with ErrUnsortedInput.
func (r *stripeRouter) route(st *stripe) ([]routedEntry, []int, error) {
	g := r.g
	recordSize, numBlocks := g.recordSize, g.geo.numBlocks
	first, blocks := st.first, st.blocks
	data := st.data
	ends := r.ends[:blocks+1]
	clear(ends)
	off := 0
	for i, n := range st.runs {
		var last sortKey
		for end := off + n*recordSize; off < end; off += recordSize {
			k := sortKeyOf(data[off : off+MinKeySize : off+MinKeySize])
			if k.less(last) {
				return nil, nil, fmt.Errorf("sorted file %s: %w", g.readers[i].run.path, ErrUnsortedInput)
			}
			last = k
			blk := intbits.FastRange32(k.hi, numBlocks) - first
			if blk >= blocks {
				return nil, nil, fmt.Errorf("sorted file %s: %w: a record is outside its stripe", g.readers[i].run.path, ErrUnsortedInput)
			}
			ends[blk+1]++
		}
	}
	for i := 1; i < len(ends); i++ {
		ends[i] += ends[i-1]
	}

	n := len(data) / recordSize
	if cap(r.flat) < n {
		r.flat = make([]routedEntry, n)
	}
	flat := r.flat[:n]
	place := func(k0, k1, payload uint64) {
		blk := intbits.FastRange32(bits.ReverseBytes64(k0), numBlocks) - first
		flat[ends[blk]] = routedEntry{k0: k0, k1: k1, payload: payload}
		ends[blk]++
	}
	// Every record but the last has 8 readable bytes past its key, so its
	// payload is one masked load; the last record decodes its own bytes.
	payloadSize := g.b.cfg.payloadSize
	mask := ^uint64(0) >> (64 - 8*payloadSize)
	off = 0
	for ; off+MinKeySize+8 <= len(data); off += recordSize {
		rec := data[off : off+MinKeySize+8 : off+MinKeySize+8]
		place(binary.LittleEndian.Uint64(rec[0:8]), binary.LittleEndian.Uint64(rec[8:16]), binary.LittleEndian.Uint64(rec[16:24])&mask)
	}
	for ; off < len(data); off += recordSize {
		rec := data[off : off+recordSize : off+recordSize]
		place(binary.LittleEndian.Uint64(rec[0:8]), binary.LittleEndian.Uint64(rec[8:16]), unpackPayloadFromBytes(rec[MinKeySize:], payloadSize))
	}
	return flat, ends[:blocks], nil
}

// fileReader reads one file's records into aligned buffers, which the read
// goroutines fill ahead of the gatherer, the one consumer.
type fileReader struct {
	run   sortedRun
	f     *os.File
	reqs  chan<- readRequest
	bufs  [][]byte
	done  []chan readResult // each buffer's read result; never full
	next  int64             // the file offset of the next read to post
	cur   int               // the buffer being consumed
	pos   int               // bytes of bufs[cur] consumed
	valid int               // bytes of records in bufs[cur]: 0 past the run's end, -1 until its result is in
}

type readRequest struct {
	r   *fileReader
	buf int
	off int64
}

type readResult struct {
	n   int
	err error
}

// serveReads fills buffers as requested until reqs closes. Once ctx is done
// it answers the requests without reading.
func serveReads(ctx context.Context, reqs <-chan readRequest) {
	for q := range reqs {
		var res readResult
		if res.err = ctx.Err(); res.err == nil {
			res.n, res.err = q.r.run.readAt(q.r.f, q.r.bufs[q.buf], q.off)
		}
		q.r.done[q.buf] <- res
	}
}

// post asks for buffer i to be filled from the file's next unread part, or
// marks it empty once the run is all posted.
func (r *fileReader) post(i int) {
	if r.next >= r.run.end {
		r.done[i] <- readResult{}
		return
	}
	r.reqs <- readRequest{r: r, buf: i, off: r.next}
	r.next += int64(len(r.bufs[i]))
}

// ready waits for the current buffer's read if its result is not in yet, and
// reports whether the buffer holds records left to consume.
func (r *fileReader) ready(ctx context.Context) (bool, error) {
	if r.valid < 0 {
		select {
		case res := <-r.done[r.cur]:
			if res.err != nil {
				return false, res.err
			}
			r.valid = res.n
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return r.valid > 0, nil
}

// advance posts a refill of the consumed buffer and moves on to the next.
func (r *fileReader) advance() {
	r.post(r.cur)
	r.cur = (r.cur + 1) % len(r.bufs)
	r.pos, r.valid = 0, -1
}
