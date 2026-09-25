package spark

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type repeatedByte byte

func (r repeatedByte) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte(r)
	}
	return len(p), nil
}

func TestMinIOMultipartStreaming(t *testing.T) {
	endpoint := os.Getenv("GOSPARK_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("GOSPARK_TEST_S3_ENDPOINT not configured")
	}
	client, err := newS3Client(S3Config{Region: "us-east-1", Endpoint: endpoint, PathStyle: true, AccessKeyID: "gosparktest", SecretAccessKey: "gosparktest-secret"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	bucket := fmt.Sprintf("multipart-%d", time.Now().UnixNano())
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	store := &awsS3Store{bucket: bucket, client: client}
	defer func() {
		keys, _ := store.List("")
		for _, key := range keys {
			_ = store.Delete(key)
		}
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()
	const total = 20<<20 + 17
	w := store.writer(ctx, "large").(*multipartWriter)
	if _, err := io.CopyN(w, repeatedByte('x'), total); err != nil {
		t.Fatal(err)
	}
	if len(w.buffer) > multipartPartBytes || len(w.parts) != 2 || w.uploadID == nil {
		t.Fatalf("not bounded multipart: buffer=%d parts=%d", len(w.buffer), len(w.parts))
	}
	if _, err := store.Head("large"); err == nil {
		t.Fatal("object visible before complete")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := store.GetRange("large", 0)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	n, err := io.Copy(h, r)
	r.Close()
	if err != nil || n != total {
		t.Fatalf("size=%d err=%v", n, err)
	}
	want := sha256.New()
	_, _ = io.CopyN(want, repeatedByte('x'), total)
	if fmt.Sprintf("%x", h.Sum(nil)) != fmt.Sprintf("%x", want.Sum(nil)) {
		t.Fatal("multipart data changed")
	}
	for _, tc := range []string{"abort", "cancel", "failed-part"} {
		t.Run(tc, func(t *testing.T) {
			c, cancel := context.WithCancel(ctx)
			defer cancel()
			w := store.writer(c, tc).(*multipartWriter)
			if _, err := io.CopyN(w, repeatedByte('a'), multipartPartBytes+1); err != nil {
				t.Fatal(err)
			}
			switch tc {
			case "abort":
				if err := w.Abort(); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
				if err := w.Close(); err == nil {
					t.Fatal("canceled upload succeeded")
				}
			case "failed-part":
				// Remove the upload on the server, then exercise UploadPart failure.
				_, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(tc), UploadId: w.uploadID})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.CopyN(w, repeatedByte('a'), multipartPartBytes+1); err == nil {
					t.Fatal("failed part accepted")
				}
				_ = w.Abort()
			}
			if _, err := store.Head(tc); err == nil {
				t.Fatal("failed upload visible")
			}
		})
	}
	uploads, err := client.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{Bucket: aws.String(bucket)})
	if err != nil {
		t.Fatal(err)
	}
	if len(uploads.Uploads) != 0 {
		t.Fatalf("leaked %d multipart uploads", len(uploads.Uploads))
	}
}
