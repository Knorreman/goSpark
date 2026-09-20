package spark

import (
	"testing"
)

func TestS3ConfigEnabled(t *testing.T) {
	if (S3Config{}).enabled() {
		t.Fatal("empty config should not be enabled")
	}
	if !(S3Config{AccessKeyID: "AKIA"}).enabled() {
		t.Fatal("access key should enable")
	}
	if !(S3Config{UseDefaultChain: true}).enabled() {
		t.Fatal("default chain should enable")
	}
}

func TestMergeS3ConfigOverride(t *testing.T) {
	got := mergeS3Config(S3Config{Region: "us-east-1"}, S3Config{Region: "eu-west-1", PathStyle: true})
	if got.Region != "eu-west-1" || !got.PathStyle {
		t.Fatalf("got %+v", got)
	}
}
