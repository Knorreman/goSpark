package spark

import (
	"fmt"
	"strings"
)

const (
	MasterLocal     = "local"
	MasterLocalAll  = "local[*]"
	MasterK8sPrefix = "k8s://"
)

type Config struct {
	AppName        string
	Master         string
	NumPartitions  int
	K8sNamespace   string
	K8sImage       string
	K8sServiceName string
	S3             S3Config
}

func DefaultConfig() *Config {
	return &Config{
		AppName:       "goSpark",
		Master:        MasterLocalAll,
		NumPartitions: 0,
	}
}

func (c *Config) IsLocal() bool {
	return strings.HasPrefix(c.Master, "local")
}

func (c *Config) IsK8s() bool {
	return strings.HasPrefix(c.Master, MasterK8sPrefix)
}

func (c *Config) NumLocalCores() (int, error) {
	if !c.IsLocal() {
		return 0, fmt.Errorf("not local mode")
	}
	suffix := strings.TrimPrefix(c.Master, "local")
	if suffix == "" || suffix == "1" {
		return 1, nil
	}
	if suffix == "*" {
		return runtimeNumCPU(), nil
	}
	var n int
	_, err := fmt.Sscanf(suffix, "%d", &n)
	if err != nil {
		return 0, fmt.Errorf("invalid local mode: %s", c.Master)
	}
	return n, nil
}
