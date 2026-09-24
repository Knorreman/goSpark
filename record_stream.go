package spark

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

type recordStream interface {
	Next() (any, error)
	Close() error
}

type bucketReader struct {
	source    io.ReadCloser
	reader    *bufio.Reader
	codec     RecordCodec
	remaining uint32
	max       int
	closed    bool
	checksum  uint32
}

func openBucket(source io.ReadCloser, codec RecordCodec, max int) (*bucketReader, error) {
	r := bufio.NewReaderSize(source, 32<<10)
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		source.Close()
		return nil, err
	}
	if string(header[:4]) != string(shuffleMagic[:]) {
		source.Close()
		return nil, fmt.Errorf("invalid shuffle magic")
	}
	return &bucketReader{source: source, reader: r, codec: codec, remaining: binary.BigEndian.Uint32(header[4:]), max: max}, nil
}
func openBucketFile(path string, codec RecordCodec, max int) (*bucketReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return openBucket(f, codec, max)
}
func (r *bucketReader) Next() (any, error) {
	if r.closed {
		return nil, io.EOF
	}
	if r.remaining == 0 {
		_, err := r.reader.ReadByte()
		r.Close()
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("trailing shuffle bytes")
	}
	var hdr [8]byte
	if _, err := io.ReadFull(r.reader, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:4])
	if uint64(n) > uint64(r.max) {
		return nil, fmt.Errorf("shuffle record %d exceeds limit %d", n, r.max)
	}
	p := make([]byte, int(n))
	if _, err := io.ReadFull(r.reader, p); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(p) != binary.BigEndian.Uint32(hdr[4:]) {
		return nil, fmt.Errorf("shuffle record checksum mismatch")
	}
	r.remaining--
	r.checksum = crc32.Update(r.checksum, crc32.IEEETable, p)
	return r.codec.Decode(p)
}
func (r *bucketReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.source.Close()
}

type bucketWriter struct {
	path     string
	file     *os.File
	buffer   *bufio.Writer
	count    uint32
	size     int64
	checksum uint32
}

func newBucketWriter(path string) (*bucketWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if _, err = f.Write(append(shuffleMagic[:], 0, 0, 0, 0)); err != nil {
		f.Close()
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	return &bucketWriter{path: path, size: 8}, nil
}
func (w *bucketWriter) append(payload []byte) error {
	if w.count == ^uint32(0) {
		return fmt.Errorf("shuffle bucket record count exceeds GSH1 limit")
	}
	if w.file == nil {
		f, err := os.OpenFile(w.path, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		w.file = f
		w.buffer = bufio.NewWriterSize(f, 32<<10)
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE(payload))
	if _, err := w.buffer.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.buffer.Write(payload); err != nil {
		return err
	}
	w.count++
	w.size += int64(len(payload) + 8)
	w.checksum = crc32.Update(w.checksum, crc32.IEEETable, payload)
	return nil
}
func (w *bucketWriter) suspend() error {
	if w.file == nil {
		return nil
	}
	err := w.buffer.Flush()
	closeErr := w.file.Close()
	w.file = nil
	w.buffer = nil
	if err != nil {
		return err
	}
	return closeErr
}
func (w *bucketWriter) finish() (ShuffleBucketMeta, error) {
	if err := w.suspend(); err != nil {
		return ShuffleBucketMeta{}, err
	}
	f, err := os.OpenFile(w.path, os.O_WRONLY, 0644)
	if err != nil {
		return ShuffleBucketMeta{}, err
	}
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], w.count)
	_, err = f.WriteAt(n[:], 4)
	closeErr := f.Close()
	if err != nil {
		return ShuffleBucketMeta{}, err
	}
	if closeErr != nil {
		return ShuffleBucketMeta{}, closeErr
	}
	return ShuffleBucketMeta{Bytes: w.size, Records: int(w.count), Checksum: w.checksum}, nil
}

func iteratorFromStream[T any](ctx *Context, s recordStream) Iterator[T] {
	ctx.onClose(func() { _ = s.Close() })
	return func() (T, bool) {
		ctx.checkCanceled()
		v, err := s.Next()
		if err == io.EOF {
			var zero T
			return zero, false
		}
		must(err)
		value, ok := v.(T)
		if !ok {
			must(fmt.Errorf("shuffle type mismatch: %T", v))
		}
		return value, true
	}
}
