package spark

import (
	"os"
	"sync"
)

type S3Config struct {
	Region          string
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	PathStyle       bool
	UseDefaultChain bool
}

func (c S3Config) enabled() bool {
	return c.AccessKeyID != "" || c.UseDefaultChain
}

func S3ConfigFromEnv() S3Config {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	return S3Config{
		Region:          region,
		Endpoint:        os.Getenv("AWS_ENDPOINT_URL"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		PathStyle:       os.Getenv("AWS_S3_PATH_STYLE") == "true" || os.Getenv("GOSPARK_S3_PATH_STYLE") == "true",
		UseDefaultChain: os.Getenv("AWS_EC2_METADATA_DISABLED") != "true" && os.Getenv("GOSPARK_S3_DEFAULT_CHAIN") == "true",
	}
}

func mergeS3Config(base, over S3Config) S3Config {
	out := base
	if over.Region != "" {
		out.Region = over.Region
	}
	if over.Endpoint != "" {
		out.Endpoint = over.Endpoint
	}
	if over.AccessKeyID != "" {
		out.AccessKeyID = over.AccessKeyID
	}
	if over.SecretAccessKey != "" {
		out.SecretAccessKey = over.SecretAccessKey
	}
	if over.SessionToken != "" {
		out.SessionToken = over.SessionToken
	}
	if over.PathStyle {
		out.PathStyle = true
	}
	if over.UseDefaultChain {
		out.UseDefaultChain = true
	}
	return out
}

var (
	s3CfgMu      sync.RWMutex
	s3CfgGlobal  S3Config
	s3CfgHasUser bool
)

func SetS3Config(cfg S3Config) {
	s3CfgMu.Lock()
	s3CfgGlobal = cfg
	s3CfgHasUser = true
	s3CfgMu.Unlock()
}

func currentS3Config() S3Config {
	env := S3ConfigFromEnv()
	s3CfgMu.RLock()
	user := s3CfgGlobal
	has := s3CfgHasUser
	s3CfgMu.RUnlock()
	if has {
		return mergeS3Config(env, user)
	}
	return env
}
