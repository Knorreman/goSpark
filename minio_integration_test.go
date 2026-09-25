package spark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestMinIODistributedSave(t *testing.T) {
	endpoint := os.Getenv("GOSPARK_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("GOSPARK_TEST_S3_ENDPOINT not configured")
	}
	access, secret := "gosparktest", "gosparktest-secret"
	t.Setenv("AWS_ACCESS_KEY_ID", access)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ENDPOINT_URL", endpoint)
	t.Setenv("AWS_S3_PATH_STYLE", "true")
	client, err := newS3Client(S3Config{Region: "us-east-1", Endpoint: endpoint, PathStyle: true, AccessKeyID: access, SecretAccessKey: secret})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	bucket := fmt.Sprintf("gospark-%d", time.Now().UnixNano())
	if _, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		st := &awsS3Store{bucket: bucket, client: client}
		keys, _ := st.List("")
		for _, key := range keys {
			_ = st.Delete(key)
		}
		_, _ = client.DeleteBucket(context.Background(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	}()
	uri := "s3://" + bucket + "/committed"
	spec := JobSpec{TaskName: "save-wordcount", Action: ActionSave, NumPartitions: 2, Params: map[string]string{"path": uri}}
	r := &retryAfterOutput{TaskRunner: startTestWorker(t)}
	m, err := ScheduleSave(spec, []TaskRunner{r, startTestWorker(t)})
	if err != nil {
		t.Fatal(err)
	}
	if !r.failed {
		t.Fatal("retry not exercised")
	}
	if len(m.Partitions) != 2 {
		t.Fatal(m)
	}
	checkSavedCounts(t, uri, m)
	stored, err := ReadCommittedOutput(uri)
	if err != nil {
		t.Fatal(err)
	}
	if stored.JobID != m.JobID {
		t.Fatal(stored)
	}
	fs := ResolvePath(uri).FS
	files, err := fs.List("committed/")
	if err != nil {
		t.Fatal(err)
	}
	var seenMarker bool
	for _, f := range files {
		if f.Name == "committed/_SUCCESS" {
			seenMarker = true
		}
		if strings.HasPrefix(f.Name, "committed/part-") {
			t.Fatalf("visible uncommitted part: %s", f.Name)
		}
	}
	if !seenMarker {
		t.Fatal("missing S3 success marker")
	}
	if _, err := ScheduleSave(spec, []TaskRunner{startTestWorker(t)}); !errors.Is(err, ErrOutputCommitted) {
		t.Fatalf("duplicate job: %v", err)
	}
	corruptURI := "s3://" + bucket + "/corrupt"
	resolved := ResolvePath(corruptURI)
	out := fixtureOutput(t, resolved.FS, resolved.BasePath, "corrupt-job", 1, 0, 0, "original\n")
	w, err := resolved.FS.Create(out.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write([]byte("different\n")); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = CommitDistributedOutput(corruptURI, out.JobID, "test", 1, 1, []PartitionOutput{out}); err == nil {
		t.Fatal("corrupt S3 output committed")
	}
	if _, err = ReadCommittedOutput(corruptURI); err == nil {
		t.Fatal("published corrupt S3 output")
	}
	raceURI := "s3://" + bucket + "/race"
	resolvedRace := ResolvePath(raceURI)
	a := fixtureOutput(t, resolvedRace.FS, resolvedRace.BasePath, "race-a", 1, 0, 0, "first\n")
	b := fixtureOutput(t, resolvedRace.FS, resolvedRace.BasePath, "race-b", 1, 0, 0, "second\n")
	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i, out := range []PartitionOutput{a, b} {
		wg.Add(1)
		go func(i int, o PartitionOutput) {
			defer wg.Done()
			_, errs[i] = CommitDistributedOutput(raceURI, o.JobID, "race", 1, 1, []PartitionOutput{o})
		}(i, out)
	}
	wg.Wait()
	success, conflict := 0, 0
	for _, e := range errs {
		if e == nil {
			success++
		} else if errors.Is(e, ErrOutputCommitted) {
			conflict++
		} else {
			t.Fatal(e)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("MinIO conditional commit: %v", errs)
	}
}
