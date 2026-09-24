package spark

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func cacheShuffleBucket(ctx *Context, store *DiskShuffleStore, m MapOutputManifest, meta ShuffleBucketMeta) (string, error) {
	var src io.ReadCloser
	if m.BaseURL != "" {
		url := fmt.Sprintf("%s/shuffle/%s/%d/%d/%d/%d", strings.TrimRight(m.BaseURL, "/"), m.JobID, m.ShuffleID, m.MapID, m.Attempt, meta.ReduceID)
		req, err := http.NewRequestWithContext(ctx.TaskContext(), http.MethodGet, url, nil)
		if err != nil {
			return "", err
		}
		resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return "", fmt.Errorf("shuffle fetch HTTP %d", resp.StatusCode)
		}
		src = resp.Body
	} else {
		f, err := os.Open(filepath.Join(m.Location, bucketName(meta.ReduceID)))
		if err != nil {
			return "", err
		}
		src = f
	}
	defer src.Close()
	f, err := os.CreateTemp(ctx.disk.dir, "fetch-")
	if err != nil {
		return "", err
	}
	name := f.Name()
	success := false
	defer func() {
		f.Close()
		if !success {
			os.Remove(name)
		}
	}()
	n, err := io.CopyBuffer(f, src, make([]byte, 32<<10))
	if err != nil {
		return "", err
	}
	if n != meta.Bytes {
		return "", fmt.Errorf("shuffle size mismatch: %d != %d", n, meta.Bytes)
	}
	if err = f.Close(); err != nil {
		return "", err
	}
	r, err := openBucketFile(name, store.codec, ctx.recordBytes())
	if err != nil {
		return "", err
	}
	defer r.Close()
	count := 0
	for {
		_, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		count++
	}
	if count != meta.Records || r.checksum != meta.Checksum {
		return "", fmt.Errorf("shuffle manifest checksum/count mismatch")
	}
	success = true
	return name, nil
}
