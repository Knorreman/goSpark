package spark

import "fmt"

const (
	defaultMemoryBytes = 16 << 20
	defaultRecordBytes = 4 << 20
)

// Budgets count encoded bytes, not Go heap bytes. Runtime/decoded-object overhead
// and user callback allocations must be allowed for in the executor memory limit.
func (c *Context) memoryBytes() int {
	if c.conf.ShuffleMemoryBytes > 0 {
		return c.conf.ShuffleMemoryBytes
	}
	return defaultMemoryBytes
}
func (c *Context) recordBytes() int {
	if c.conf.MaxRecordBytes > 0 {
		return c.conf.MaxRecordBytes
	}
	return defaultRecordBytes
}
func (c *Context) groupBytes() int {
	if c.conf.MaxGroupBytes > 0 {
		return c.conf.MaxGroupBytes
	}
	return c.memoryBytes()
}

type executionError struct{ err error }

func must(err error) {
	if err != nil {
		panic(executionError{err})
	}
}
func recordLimit(size, limit int) error {
	if size > limit {
		return fmt.Errorf("encoded record size %d exceeds limit %d", size, limit)
	}
	return nil
}
