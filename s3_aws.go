package spark

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type awsS3Store struct {
	bucket string
	client *s3.Client
}

func newAWSS3Store(bucket string, cfg S3Config) (*awsS3Store, error) {
	client, err := newS3Client(cfg)
	if err != nil {
		return nil, err
	}
	return &awsS3Store{bucket: bucket, client: client}, nil
}

// CreateS3Bucket is primarily useful when bootstrapping a new S3-compatible
// test store. Production buckets are normally provisioned outside goSpark.
func CreateS3Bucket(bucket string) error {
	if bucket == "" {
		return fmt.Errorf("bucket is required")
	}
	client, err := newS3Client(currentS3Config())
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	return err
}

func newS3Client(cfg S3Config) (*s3.Client, error) {
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}
	if cfg.AccessKeyID != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.PathStyle || looksLikeMinIO(cfg.Endpoint)
	}), nil
}

func looksLikeMinIO(endpoint string) bool {
	if endpoint == "" {
		return false
	}
	return strings.Contains(endpoint, "minio") || strings.Contains(endpoint, ":9000")
}

func (s *awsS3Store) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

func (s *awsS3Store) Put(key string, data []byte) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (s *awsS3Store) PutIfAbsent(key string, data []byte) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
		Body: bytes.NewReader(data), IfNoneMatch: aws.String("*"),
	})
	return err
}

func (s *awsS3Store) Get(key string) ([]byte, error) {
	r, err := s.GetRange(key, 0)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (s *awsS3Store) GetRange(key string, offset int64) (io.ReadCloser, error) {
	ctx, cancel := s.ctx()
	in := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}
	if offset > 0 {
		in.Range = aws.String(fmt.Sprintf("bytes=%d-", offset))
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		cancel()
		return nil, err
	}
	return &cancelCloser{ReadCloser: out.Body, cancel: cancel}, nil
}

func (s *awsS3Store) Head(key string) (int64, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, err
	}
	if out.ContentLength != nil {
		return *out.ContentLength, nil
	}
	return 0, nil
}

func (s *awsS3Store) List(prefix string) ([]string, error) {
	ctx, cancel := s.ctx()
	defer cancel()
	var keys []string
	var token *string
	for {
		out, err := s.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(s.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}
	return keys, nil
}

func (s *awsS3Store) Delete(key string) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	return err
}

func (s *awsS3Store) Copy(src, dst string) error {
	ctx, cancel := s.ctx()
	defer cancel()
	_, err := s.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(s.bucket),
		Key:        aws.String(dst),
		CopySource: aws.String(s.bucket + "/" + src),
	})
	return err
}

type cancelCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelCloser) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}
