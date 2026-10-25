package streamhash

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// Sorted file header (spec §3.9):
//
//	Offset  Size  Field        Type
//	0       4     Magic        0x53545346 ("STSF")
//	4       2     Version      0x0001
//	6       1     PayloadSize  uint8 (0-8)
//	7       1     Reserved     zero
//	8       8     Count        uint64_le
//	16      4     MetadataLen  uint32_le
//	20      ML    Metadata
//
// Zero bytes pad the header to a multiple of 4096, and the records follow.
const (
	sortedFileMagic   = uint32(0x53545346)
	sortedFileVersion = uint16(0x0001)

	sortedFileFixedSize   = 20
	sortedFileCountOffset = 8

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
	bw         *bufio.Writer
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
	w := &SortedFileWriter{
		f:          f,
		bw:         bufio.NewWriterSize(&writebackWriter{f: f, bytesPerSync: cfg.BytesPerSync}, 1<<20),
		recordSize: MinKeySize + cfg.PayloadSize,
		maxPayload: ^uint64(0) >> (64 - 8*cfg.PayloadSize),
	}
	// The header fits in the empty buffer, so this write cannot fail.
	_, _ = w.bw.Write(encodeSortedFileHeader(cfg))
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

// Finish writes the record count, syncs and closes the file. If it fails, it
// removes the file.
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

// commit flushes, patches the count, syncs and closes, in that order: a short
// file's header is still in the buffer until the flush, and ENOSPC and EIO
// can surface only at close.
func (w *SortedFileWriter) commit() error {
	err := w.bw.Flush()
	if err == nil {
		var count [8]byte
		binary.LittleEndian.PutUint64(count[:], w.count)
		_, err = w.f.WriteAt(count[:], sortedFileCountOffset)
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
	binary.LittleEndian.PutUint32(buf[16:20], uint32(len(cfg.Metadata)))
	copy(buf[sortedFileFixedSize:], cfg.Metadata)
	return buf
}

// writebackWriter writes to f, starting writeback every bytesPerSync bytes.
type writebackWriter struct {
	f            *os.File
	bytesPerSync int
	unsynced     int
}

func (w *writebackWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if w.bytesPerSync > 0 {
		w.unsynced += n
		if w.unsynced >= w.bytesPerSync {
			w.unsynced = 0
			startWriteback(w.f)
		}
	}
	return n, err
}

// ReadSortedFileHeader reads and checks the header at path: magic, version,
// payload size, and that the record count matches the file's length.
func ReadSortedFileHeader(path string) (SortedFileHeader, error) {
	f, err := os.Open(path)
	if err != nil {
		return SortedFileHeader{}, err
	}
	defer f.Close()
	return readSortedFileHeader(f)
}

// readSortedFileHeader reads the header of f, which must not be open for
// direct reads: these reads are not aligned.
func readSortedFileHeader(f *os.File) (SortedFileHeader, error) {
	info, err := f.Stat()
	if err != nil {
		return SortedFileHeader{}, err
	}
	size := info.Size()
	if size < sortedFileFixedSize {
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w", f.Name(), ErrTruncatedFile)
	}
	var fixed [sortedFileFixedSize]byte
	if _, err := f.ReadAt(fixed[:], 0); err != nil {
		return SortedFileHeader{}, err
	}
	if binary.LittleEndian.Uint32(fixed[0:4]) != sortedFileMagic {
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w", f.Name(), ErrInvalidMagic)
	}
	if binary.LittleEndian.Uint16(fixed[4:6]) != sortedFileVersion {
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w", f.Name(), ErrInvalidVersion)
	}
	h := SortedFileHeader{PayloadSize: int(fixed[6]), Count: binary.LittleEndian.Uint64(fixed[8:16])}
	if h.PayloadSize > maxPayloadSize {
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w: payload size %d", f.Name(), ErrCorruptedIndex, h.PayloadSize)
	}
	metadataLen := binary.LittleEndian.Uint32(fixed[16:20])
	if metadataLen > maxSortedFileMetadata {
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w: metadata length %d is over the maximum of %d", f.Name(), ErrCorruptedIndex, metadataLen, maxSortedFileMetadata)
	}
	recordsOffset := sortedFileRecordsOffset(int(metadataLen))
	if size < recordsOffset {
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w", f.Name(), ErrTruncatedFile)
	}
	if metadataLen > 0 {
		h.Metadata = make([]byte, metadataLen)
		if _, err := f.ReadAt(h.Metadata, sortedFileFixedSize); err != nil {
			return SortedFileHeader{}, err
		}
	}
	// Divide the trusted size rather than multiply the untrusted count, so a
	// corrupt count cannot overflow.
	body, recordSize := size-recordsOffset, int64(MinKeySize+h.PayloadSize)
	if holds := uint64(body / recordSize); holds != h.Count || body%recordSize != 0 {
		sentinel := ErrKeyCountMismatch
		if holds < h.Count {
			sentinel = ErrTruncatedFile
		}
		return SortedFileHeader{}, fmt.Errorf("sorted file %s: %w: the header counts %d records of %d bytes, the file holds %d bytes of records",
			f.Name(), sentinel, h.Count, recordSize, body)
	}
	return h, nil
}

// sortedRun is a sorted file's records: the file offsets where they start
// and end.
type sortedRun struct {
	path       string
	start, end int64
}

// readAt reads the run's file f at off into buf, and returns how many bytes
// of buf are the run's records. A file shorter than its records fails with
// ErrTruncatedFile.
func (r sortedRun) readAt(f *os.File, buf []byte, off int64) (int, error) {
	n, err := f.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	want := int(min(int64(len(buf)), r.end-off))
	if n < want {
		return 0, fmt.Errorf("sorted file %s: %w: it ends before its last record", f.Name(), ErrTruncatedFile)
	}
	return want, nil
}

// readSortedRuns checks the headers of the sorted files at paths, which must
// share a payload size, and returns the runs of those with records, the
// payload size, and the total record count.
func readSortedRuns(paths []string) ([]sortedRun, int, uint64, error) {
	var runs []sortedRun
	var payloadSize int
	var total uint64
	for i, path := range paths {
		h, err := ReadSortedFileHeader(path)
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
			runs = append(runs, sortedRun{path: path, start: start, end: start + int64(h.Count)*int64(MinKeySize+h.PayloadSize)})
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
	br          *bufio.Reader
	payloadSize int
	left        uint64
	last        sortKey
	record      [MinKeySize + maxPayloadSize]byte
}

// OpenSortedFile opens the sorted file at path to read its records in order.
// It checks the header as ReadSortedFileHeader does.
func OpenSortedFile(path string) (*SortedFileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	h, err := readSortedFileHeader(f)
	if err == nil {
		_, err = f.Seek(h.recordsOffset(), io.SeekStart)
	}
	if err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &SortedFileReader{f: f, br: bufio.NewReaderSize(f, 1<<20), payloadSize: h.PayloadSize, left: h.Count}, nil
}

// Next returns the next record's key and payload, and io.EOF after the last
// record. A record out of order fails with ErrUnsortedInput.
func (r *SortedFileReader) Next() (key [MinKeySize]byte, payload uint64, err error) {
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
	copy(key[:], rec[:MinKeySize])
	return key, unpackPayloadFromBytes(rec[MinKeySize:], r.payloadSize), nil
}

// Close closes the file.
func (r *SortedFileReader) Close() error {
	return r.f.Close()
}
