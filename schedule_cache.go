package spark

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ScheduleCache owns reusable memory caches on a set of workers. Keep the same
// worker processes for subsequent RunPipeline calls using specs returned by Persist.
type ScheduleCache struct {
	runners []TaskRunner
	mu      sync.Mutex
	names   map[string]bool
	closed  bool
}

func NewScheduleCache(runners []TaskRunner) *ScheduleCache {
	return &ScheduleCache{runners: append([]TaskRunner(nil), runners...), names: map[string]bool{}}
}

// Persist marks a registered job output and its cached narrow-lineage inputs
// for reuse. The identity and graph fingerprint must match on subsequent runs.
func (c *ScheduleCache) Persist(name string, spec JobSpec) (JobSpec, error) {
	if !cacheIdentityPattern.MatchString(name) {
		return spec, fmt.Errorf("invalid reusable cache identity %q", name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return spec, fmt.Errorf("schedule cache stopped")
	}
	c.names[name] = true
	spec.CacheIdentity = name
	return spec, nil
}

// Unpersist releases all fingerprints of a named cache on the workers.
func (c *ScheduleCache) Unpersist(ctx context.Context, name string) error {
	if !cacheIdentityPattern.MatchString(name) {
		return fmt.Errorf("invalid reusable cache identity %q", name)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.release(ctx, name); err != nil {
		return err
	}
	delete(c.names, name)
	return nil
}

// Stop releases all cache identities registered with this driver handle.
func (c *ScheduleCache) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for name := range c.names {
		if err := c.release(ctx, name); err != nil {
			errs = append(errs, err)
		} else {
			delete(c.names, name)
		}
	}
	return errors.Join(errs...)
}

func (c *ScheduleCache) release(ctx context.Context, name string) error {
	var errs []error
	for _, runner := range c.runners {
		if client, ok := runner.(interface {
			UnpersistCacheContext(context.Context, string) error
		}); ok {
			if err := client.UnpersistCacheContext(ctx, name); err != nil {
				errs = append(errs, err)
			}
		} else {
			errs = append(errs, fmt.Errorf("runner %T does not support cache unpersist", runner))
		}
	}
	return errors.Join(errs...)
}

// UnpersistCacheContext removes a named in-process cache, waiting for active
// tasks to finish before removing its partitions.
func (c *WorkerClient) UnpersistCacheContext(ctx context.Context, name string) error {
	if !cacheIdentityPattern.MatchString(name) {
		return fmt.Errorf("invalid reusable cache identity %q", name)
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/unpersist/" + name
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
		if resp.StatusCode != http.StatusConflict {
			return fmt.Errorf("unpersist returned HTTP %d", resp.StatusCode)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
