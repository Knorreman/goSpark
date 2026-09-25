package spark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const multipartPartBytes = 8 << 20

// One part at a time provides backpressure without an unbounded producer queue.
// The maximum upload is 10,000 parts (about 78 GiB at this part size).
type multipartWriter struct {
	ctx      context.Context
	store    *awsS3Store
	key      string
	buffer   []byte
	uploadID *string
	parts    []types.CompletedPart
	err      error
	closed   bool
}

func (s *awsS3Store) writer(ctx context.Context, key string) io.WriteCloser {
	return &multipartWriter{ctx: ctx, store: s, key: key, buffer: make([]byte, 0, multipartPartBytes)}
}
func (w *multipartWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("S3 writer closed")
	}
	if w.err != nil {
		return 0, w.err
	}
	total := 0
	for len(p) > 0 {
		if err := w.ctx.Err(); err != nil {
			return total, w.fail(err)
		}
		if len(w.buffer) == multipartPartBytes {
			if err := w.flush(); err != nil {
				return total, w.fail(err)
			}
		}
		n := min(len(p), multipartPartBytes-len(w.buffer))
		w.buffer = append(w.buffer, p[:n]...)
		total += n
		p = p[n:]
	}
	return total, nil
}
func (w *multipartWriter) flush() error {
	if len(w.parts) >= 10000 {
		return fmt.Errorf("multipart upload exceeds 10000 parts")
	}
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()
	if w.uploadID == nil {
		r, err := w.store.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(w.store.bucket), Key: aws.String(w.key)})
		if err != nil {
			return err
		}
		w.uploadID = r.UploadId
	}
	n := int32(len(w.parts) + 1)
	r, err := w.store.client.UploadPart(ctx, &s3.UploadPartInput{Bucket: aws.String(w.store.bucket), Key: aws.String(w.key), UploadId: w.uploadID, PartNumber: aws.Int32(n), Body: bytes.NewReader(w.buffer), ContentLength: aws.Int64(int64(len(w.buffer)))})
	if err != nil {
		return err
	}
	w.parts = append(w.parts, types.CompletedPart{ETag: r.ETag, PartNumber: aws.Int32(n), ChecksumCRC32: r.ChecksumCRC32, ChecksumCRC32C: r.ChecksumCRC32C, ChecksumSHA1: r.ChecksumSHA1, ChecksumSHA256: r.ChecksumSHA256})
	w.buffer = w.buffer[:0]
	return nil
}
func (w *multipartWriter) fail(err error) error {
	w.err = errors.Join(err, w.abortUpload())
	w.buffer = nil
	return w.err
}
func (w *multipartWriter) abortUpload() error {
	if w.uploadID == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := w.store.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(w.store.bucket), Key: aws.String(w.key), UploadId: w.uploadID})
	if err == nil {
		w.uploadID = nil
	}
	return err
}
func (w *multipartWriter) Abort() error { w.closed = true; w.buffer = nil; return w.abortUpload() }
func (w *multipartWriter) Close() error {
	if w.closed {
		return fmt.Errorf("S3 writer closed")
	}
	w.closed = true
	defer func() { w.buffer = nil }()
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		return w.fail(err)
	}
	if w.uploadID == nil {
		ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
		defer cancel()
		_, err := w.store.client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(w.store.bucket), Key: aws.String(w.key), Body: bytes.NewReader(w.buffer)})
		return err
	}
	if len(w.buffer) > 0 {
		if err := w.flush(); err != nil {
			return w.fail(err)
		}
	}
	ctx, cancel := context.WithTimeout(w.ctx, 30*time.Second)
	defer cancel()
	_, err := w.store.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{Bucket: aws.String(w.store.bucket), Key: aws.String(w.key), UploadId: w.uploadID, MultipartUpload: &types.CompletedMultipartUpload{Parts: w.parts}})
	if err != nil {
		return w.fail(err)
	}
	w.uploadID = nil
	return nil
}
