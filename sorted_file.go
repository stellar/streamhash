package streamhash

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/cespare/xxhash/v2"
)

// Sorted file header (spec §3.9):
//
//	Offset  Size  Field        Type
//	0       4     Magic        0x53545346 ("STSF")
//	4       2     Version      0x0002
//	6       1     PayloadSize  uint8 (0-8)
//	7       1     Reserved     zero
//	8       8     Count        uint64_le
//	16      8     RecordsHash  uint64_le (xxHash64 of the records)
//	24      8     HeaderHash   uint64_le (xxHash64 of the header's other fields and the metadata)
//	32      4     MetadataLen  uint32_le
//	36      ML    Metadata
//
// Zero bytes pad the header to a multiple of 4096, and the records follow.
const (
	sortedFileMagic   = uint32(0x53545346)
	sortedFileVersion = uint16(0x0002)

	sortedFileFixedSize = 36
	// Finish writes Count, RecordsHash and HeaderHash, which are contiguous.
	sortedFileCountOffset       = 8
	sortedFileRecordsHashOffset = 16
	sortedFileHeaderHashOffset  = 24
	sortedFileMetadataLenOffset = 32

	// sortedFileAlign aligns the records, so direct reads can start at them.
	// It is a multiple of directAlign.
	sortedFileAlign = 4096

	// maxSortedFileMetadata bounds what a corrupt header can make a reader allocate.
	maxSortedFileMetadata = 64 << 10
)

// SortedFileConfig configures a sorted file.
type SortedFileConfig struct {
	PayloadSize  int    // bytes per payload, 0 to 8
	Metadata     []byte // stored in the header for ReadSortedFileHeader, at most 64 KiB
	BytesPerSync int    // start writeback every this many bytes, as WithBytesPerSync; 0 leaves it to the kernel
}

// SortedFileHeader is a sorted file's header.
type SortedFileHeader struct {
	PayloadSize int    // bytes per payload, 0 to 8
	Count       uint64 // records in the file
	Metadata    []byte // the SortedFileConfig.Metadata the file was written with
}

// sortedFileHeaderHash is the HeaderHash of a header whose fixed fields are
// fixed: xxHash64 of the fields before HeaderHash, then MetadataLen and the
// metadata.
func sortedFileHeaderHash(fixed, metadata []byte) uint64 {
	d := xxhash.New()
	_, _ = d.Write(fixed[:sortedFileHeaderHashOffset])
	_, _ = d.Write(fixed[sortedFileMetadataLenOffset:sortedFileFixedSize])
	_, _ = d.Write(metadata)
	return d.Sum64()
}

// recordsOffset is where the file's records start.
func (h SortedFileHeader) recordsOffset() int64 {
	return sortedFileRecordsOffset(len(h.Metadata))
}

// sortedFileRecordsOffset is the header's size: its fixed fields and the
// metadata, padded to sortedFileAlign.
func sortedFileRecordsOffset(metadataLen int) int64 {
	return (sortedFileFixedSize + int64(metadataLen) + sortedFileAlign - 1) &^ (sortedFileAlign - 1)
}

// sortKey is a key's first 16 bytes as two big-endian uint64s, which compare
// as the bytes do.
type sortKey struct{ hi, lo uint64 }

func sortKeyOf(key []byte) sortKey {
	return sortKey{binary.BigEndian.Uint64(key[0:8]), binary.BigEndian.Uint64(key[8:16])}
}

func (k sortKey) less(o sortKey) bool {
	if k.hi != o.hi {
		return k.hi < o.hi
	}
	return k.lo < o.lo
}

// SortedFileWriter writes a sorted file: keys with their payloads in key
// order, in fixed-width records of the key's first 16 bytes, then the payload
// as PayloadSize little-endian bytes. MergeSortedFiles merges several into one.
type SortedFileWriter struct {
	f          *os.File
	out        *writebackWriter
	bw         *bufio.Writer
	header     []byte // the fixed fields and the metadata
	recordSize int
	maxPayload uint64
	last       sortKey
	count      uint64
	// record lives here rather than in Add: bufio may hand the slice to the
	// file, which would move a local array to the heap per record.
	record [MinKeySize + maxPayloadSize]byte
	closed bool
}

// NewSortedFileWriter creates the sorted file at path. Keys must be added in
// non-decreasing order of their first 16 bytes; a key out of order fails Add.
// Finish writes the record count and syncs. Close without Finish removes the file.
func NewSortedFileWriter(path string, cfg SortedFileConfig) (*SortedFileWriter, error) {
	if cfg.PayloadSize < 0 || cfg.PayloadSize > maxPayloadSize {
		return nil, ErrPayloadTooLarge
	}
	if len(cfg.Metadata) > maxSortedFileMetadata {
		return nil, fmt.Errorf("sorted file metadata is %d bytes, over the maximum of %d", len(cfg.Metadata), maxSortedFileMetadata)
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	header := encodeSortedFileHeader(cfg)
	out := &writebackWriter{f: f, bytesPerSync: cfg.BytesPerSync, skip: len(header), records: xxhash.New()}
	w := &SortedFileWriter{
		f:          f,
		out:        out,
		bw:         bufio.NewWriterSize(out, 1<<20),
		header:     header[:sortedFileFixedSize+len(cfg.Metadata)],
		recordSize: MinKeySize + cfg.PayloadSize,
		maxPayload: ^uint64(0) >> (64 - 8*cfg.PayloadSize),
	}
	// The header fits in the empty buffer, so this write cannot fail.
	_, _ = w.bw.Write(header)
	return w, nil
}

// Add adds a record of key's first 16 bytes and payload, which must fit in
// PayloadSize bytes.
func (w *SortedFileWriter) Add(key []byte, payload uint64) error {
	if w.closed {
		return os.ErrClosed
	}
	if len(key) < MinKeySize {
		return ErrKeyTooShort
	}
	if payload > w.maxPayload {
		return ErrPayloadOverflow
	}
	k := sortKeyOf(key)
	if k.less(w.last) {
		return ErrUnsortedInput
	}
	w.last = k
	copy(w.record[:MinKeySize], key)
	binary.LittleEndian.PutUint64(w.record[MinKeySize:], payload)
	if _, err := w.bw.Write(w.record[:w.recordSize]); err != nil {
		return err
	}
	w.count++
	return nil
}

// appendEncoded appends records already in the file's encoding, whose order
// the caller has checked.
func (w *SortedFileWriter) appendEncoded(records []byte) error {
	if _, err := w.bw.Write(records); err != nil {
		return err
	}
	w.count += uint64(len(records) / w.recordSize)
	return nil
}

// Finish writes the record count and the hashes, syncs and closes the file.
// If it fails, it removes the file.
func (w *SortedFileWriter) Finish() error {
	if w.closed {
		return os.ErrClosed
	}
	w.closed = true
	if err := w.commit(); err != nil {
		return errors.Join(err, os.Remove(w.f.Name()))
	}
	return nil
}

// commit flushes and syncs the records, patches the count and the hashes,
// syncs again and closes. The header goes last, so a header that passes its
// check means the records are on disk. A short file's header is still in the
// buffer until the flush, which also completes the records' hash, and ENOSPC
// and EIO can surface only at close.
func (w *SortedFileWriter) commit() error {
	err := w.bw.Flush()
	if err == nil {
		err = w.f.Sync()
	}
	if err == nil {
		fixed := w.header[:sortedFileFixedSize]
		binary.LittleEndian.PutUint64(fixed[sortedFileCountOffset:], w.count)
		binary.LittleEndian.PutUint64(fixed[sortedFileRecordsHashOffset:], w.out.records.Sum64())
		binary.LittleEndian.PutUint64(fixed[sortedFileHeaderHashOffset:], sortedFileHeaderHash(fixed, w.header[sortedFileFixedSize:]))
		_, err = w.f.WriteAt(fixed[sortedFileCountOffset:sortedFileMetadataLenOffset], sortedFileCountOffset)
	}
	if err == nil {
		err = w.f.Sync()
	}
	return errors.Join(err, w.f.Close())
}

// Close removes the file unless Finish has run. It is safe to call after Finish.
func (w *SortedFileWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return errors.Join(w.f.Close(), os.Remove(w.f.Name()))
}

func encodeSortedFileHeader(cfg SortedFileConfig) []byte {
	buf := make([]byte, sortedFileRecordsOffset(len(cfg.Metadata)))
	binary.LittleEndian.PutUint32(buf[0:4], sortedFileMagic)
	binary.LittleEndian.PutUint16(buf[4:6], sortedFileVersion)
	buf[6] = byte(cfg.PayloadSize)
	binary.LittleEndian.PutUint32(buf[sortedFileMetadataLenOffset:], uint32(len(cfg.Metadata)))
	copy(buf[sortedFileFixedSize:], cfg.Metadata)
	return buf
}

// writebackWriter writes to f, starting writeback every bytesPerSync bytes.
// It hashes what it writes past the first skip bytes, the header, into
// records: the buffer's flushes hash the records in bulk, where hashing in
// Add would cost several times more.
type writebackWriter struct {
	f            *os.File
	bytesPerSync int
	unsynced     int
	skip         int
	records      *xxhash.Digest
}

func (w *writebackWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	skipped := min(w.skip, n)
	w.skip -= skipped
	_, _ = w.records.Write(p[skipped:n])
	if w.bytesPerSync > 0 {
		w.unsynced += n
		if w.unsynced >= w.bytesPerSync {
			w.unsynced = 0
			startWriteback(w.f)
		}
	}
	return n, err
}

// readSortedFileHeaderAt is readSortedFileHeader for the file at path.
func readSortedFileHeaderAt(path string) (SortedFileHeader, uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return SortedFileHeader{}, 0, err
	}
	defer f.Close()
	return readSortedFileHeader(f)
}

// ReadSortedFileHeader reads and checks the header at path: magic, version,
// payload size, its hash, and that the record count matches the file's length.
// It does not read the records, so it checks their hash only when there are none.
func ReadSortedFileHeader(path string) (SortedFileHeader, error) {
	h, _, err := readSortedFileHeaderAt(path)
	return h, err
}

// readSortedFileHeader reads the header of f, which must not be open for
// direct reads: these reads are not aligned. It also returns the RecordsHash.
func readSortedFileHeader(f *os.File) (SortedFileHeader, uint64, error) {
	info, err := f.Stat()
	if err != nil {
		return SortedFileHeader{}, 0, err
	}
	size := info.Size()
	if size < sortedFileFixedSize {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w", f.Name(), ErrTruncatedFile)
	}
	var fixed [sortedFileFixedSize]byte
	if _, err := f.ReadAt(fixed[:], 0); err != nil {
		return SortedFileHeader{}, 0, err
	}
	if binary.LittleEndian.Uint32(fixed[0:4]) != sortedFileMagic {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w", f.Name(), ErrInvalidMagic)
	}
	if binary.LittleEndian.Uint16(fixed[4:6]) != sortedFileVersion {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w", f.Name(), ErrInvalidVersion)
	}
	h := SortedFileHeader{PayloadSize: int(fixed[6]), Count: binary.LittleEndian.Uint64(fixed[sortedFileCountOffset:])}
	if h.PayloadSize > maxPayloadSize {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w: payload size %d", f.Name(), ErrCorruptedIndex, h.PayloadSize)
	}
	metadataLen := binary.LittleEndian.Uint32(fixed[sortedFileMetadataLenOffset:])
	if metadataLen > maxSortedFileMetadata {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w: metadata length %d is over the maximum of %d", f.Name(), ErrCorruptedIndex, metadataLen, maxSortedFileMetadata)
	}
	recordsOffset := sortedFileRecordsOffset(int(metadataLen))
	if size < recordsOffset {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w", f.Name(), ErrTruncatedFile)
	}
	if metadataLen > 0 {
		h.Metadata = make([]byte, metadataLen)
		if _, err := f.ReadAt(h.Metadata, sortedFileFixedSize); err != nil {
			return SortedFileHeader{}, 0, err
		}
	}
	if sortedFileHeaderHash(fixed[:], h.Metadata) != binary.LittleEndian.Uint64(fixed[sortedFileHeaderHashOffset:]) {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w: header", f.Name(), ErrChecksumFailed)
	}
	// Divide the trusted size rather than multiply the untrusted count, so a
	// corrupt count cannot overflow.
	body, recordSize := size-recordsOffset, int64(MinKeySize+h.PayloadSize)
	if holds := uint64(body / recordSize); holds != h.Count || body%recordSize != 0 {
		sentinel := ErrKeyCountMismatch
		if holds < h.Count {
			sentinel = ErrTruncatedFile
		}
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w: the header counts %d records of %d bytes, the file holds %d bytes of records",
			f.Name(), sentinel, h.Count, recordSize, body)
	}
	recordsHash := binary.LittleEndian.Uint64(fixed[sortedFileRecordsHashOffset:])
	if h.Count == 0 && recordsHash != xxhash.Sum64(nil) {
		return SortedFileHeader{}, 0, fmt.Errorf("sorted file %s: %w: records", f.Name(), ErrChecksumFailed)
	}
	return h, recordsHash, nil
}

// sortedRun is a sorted file's records: the file offsets where they start
// and end, and their hash.
type sortedRun struct {
	path       string
	start, end int64
	hash       uint64
}

// check fails if the records read from the run hash to sum, not to its hash.
func (r sortedRun) check(sum uint64) error {
	if sum != r.hash {
		return fmt.Errorf("sorted file %s: %w: records", r.path, ErrChecksumFailed)
	}
	return nil
}

// readAt reads the run's file f at off into buf, and returns how many bytes
// of buf are the run's records. A file shorter than its records fails with
// ErrTruncatedFile.
func (r sortedRun) readAt(f *os.File, buf []byte, off int64) (int, error) {
	want := int(min(int64(len(buf)), r.end-off))
	n, err := f.ReadAt(buf, off)
	if n >= want {
		return want, nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	return 0, fmt.Errorf("sorted file %s: %w: it ends before its last record", f.Name(), ErrTruncatedFile)
}

// readSortedRuns checks the headers of the sorted files at paths, which must
// share a payload size, and returns the runs of those with records, the
// payload size, and the total record count.
func readSortedRuns(paths []string) ([]sortedRun, int, uint64, error) {
	var runs []sortedRun
	var payloadSize int
	var total uint64
	for i, path := range paths {
		h, recordsHash, err := readSortedFileHeaderAt(path)
		if err != nil {
			return nil, 0, 0, err
		}
		if i == 0 {
			payloadSize = h.PayloadSize
		} else if h.PayloadSize != payloadSize {
			return nil, 0, 0, fmt.Errorf("sorted file %s has %d-byte payloads, %s has %d-byte payloads", path, h.PayloadSize, paths[0], payloadSize)
		}
		if h.Count > 0 {
			start := h.recordsOffset()
			runs = append(runs, sortedRun{path: path, start: start, end: start + int64(h.Count)*int64(MinKeySize+h.PayloadSize), hash: recordsHash})
		}
		total += h.Count
	}
	return runs, payloadSize, total, nil
}

// checkOutputNotInput fails if output is one of the inputs, which creating
// the output would truncate.
func checkOutputNotInput(output string, inputs []string) error {
	out, err := os.Stat(output)
	if err != nil {
		return nil
	}
	for _, in := range inputs {
		if info, err := os.Stat(in); err == nil && os.SameFile(out, info) {
			return fmt.Errorf("the output %s is one of the inputs", output)
		}
	}
	return nil
}

// SortedFileReader reads a sorted file's records in order.
type SortedFileReader struct {
	f           *os.File
	hash        *xxhash.Digest
	br          *bufio.Reader
	run         sortedRun
	payloadSize int
	left        uint64
	last        sortKey
	record      [MinKeySize + maxPayloadSize]byte
	err         error
}

// OpenSortedFile opens the sorted file at path to read its records in order.
// It checks the header as ReadSortedFileHeader does. The records' hash is
// checked when the last record is read: only once Next has returned it are
// the records known to be the ones written.
func OpenSortedFile(path string) (*SortedFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h, recordsHash, err := readSortedFileHeader(f)
	if err == nil {
		_, err = f.Seek(h.recordsOffset(), io.SeekStart)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	hash := xxhash.New()
	return &SortedFileReader{
		f: f, hash: hash, br: bufio.NewReaderSize(io.TeeReader(f, hash), 1<<20),
		run:         sortedRun{path: path, hash: recordsHash},
		payloadSize: h.PayloadSize, left: h.Count,
	}, nil
}

// Next returns the next record's key and payload, and io.EOF after the last
// record. A record out of order fails with ErrUnsortedInput. If the records
// do not match the file's hash, the last one fails with ErrChecksumFailed.
// After an error, every later call returns it.
func (r *SortedFileReader) Next() (key [MinKeySize]byte, payload uint64, err error) {
	if r.err != nil {
		return key, 0, r.err
	}
	defer func() {
		if err != nil {
			r.err = err
		}
	}()
	if r.left == 0 {
		return key, 0, io.EOF
	}
	rec := r.record[:MinKeySize+r.payloadSize]
	if _, err = io.ReadFull(r.br, rec); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			err = fmt.Errorf("sorted file %s: %w: it ends before its last record", r.f.Name(), ErrTruncatedFile)
		}
		return key, 0, err
	}
	r.left--
	k := sortKeyOf(rec)
	if k.less(r.last) {
		return key, 0, fmt.Errorf("sorted file %s: %w", r.f.Name(), ErrUnsortedInput)
	}
	r.last = k
	if r.left == 0 {
		// The buffered reader has read every record by now.
		if err := r.run.check(r.hash.Sum64()); err != nil {
			return key, 0, err
		}
	}
	copy(key[:], rec[:MinKeySize])
	return key, unpackPayloadFromBytes(rec[MinKeySize:], r.payloadSize), nil
}

// Close closes the file.
func (r *SortedFileReader) Close() error {
	return r.f.Close()
}
