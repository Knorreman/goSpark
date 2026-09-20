package spark

import (
	"fmt"
	"io"
)

const (
	SuccessFileName = "_SUCCESS"
	tempDirName     = "_temporary"
)

type OutputCommitter struct {
	FS      FileSystem
	Base    string
	Attempt int
}

func NewOutputCommitter(fs FileSystem, base string, attempt int) *OutputCommitter {
	if attempt <= 0 {
		attempt = 1
	}
	return &OutputCommitter{FS: fs, Base: base, Attempt: attempt}
}

func (c *OutputCommitter) TempDir() string {
	return c.FS.Join(c.Base, tempDirName, fmt.Sprintf("attempt_%05d", c.Attempt))
}

func (c *OutputCommitter) TempPath(partitionIndex int) string {
	return c.FS.Join(c.TempDir(), PartitionFileName(partitionIndex))
}

func (c *OutputCommitter) FinalPath(partitionIndex int) string {
	return c.FS.Join(c.Base, PartitionFileName(partitionIndex))
}

func (c *OutputCommitter) SuccessPath() string {
	return c.FS.Join(c.Base, SuccessFileName)
}

func (c *OutputCommitter) Setup() error {
	return c.FS.MkdirAll(c.TempDir(), 0755)
}

func (c *OutputCommitter) CommitPartition(partitionIndex int) error {
	return c.FS.Rename(c.TempPath(partitionIndex), c.FinalPath(partitionIndex))
}

func (c *OutputCommitter) CommitJob() error {
	w, err := c.FS.Create(c.SuccessPath())
	if err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	return nil
}

func (c *OutputCommitter) HasSuccess() bool {
	_, err := c.FS.Stat(c.SuccessPath())
	return err == nil
}

func writeCloserChecked(w io.WriteCloser, writeErr error) error {
	closeErr := w.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
