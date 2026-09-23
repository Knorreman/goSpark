package spark

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func ServeShuffle(root, addr string) (*http.Server, string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/shuffle/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/shuffle/")
		parts := strings.Split(rel, "/")
		if len(parts) != 5 {
			http.Error(w, "want /shuffle/{job}/{shuffle}/{map}/{attempt}/{reduce}", http.StatusBadRequest)
			return
		}
		jobID := parts[0]
		sid, err1 := strconv.Atoi(parts[1])
		mid, err2 := strconv.Atoi(parts[2])
		att, err3 := strconv.Atoi(parts[3])
		rid, err4 := strconv.Atoi(parts[4])
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			http.Error(w, "invalid shuffle path", http.StatusBadRequest)
			return
		}
		path := filepath.Join(root, jobID, fmt.Sprintf("s%d", sid), fmt.Sprintf("m%d-a%d", mid, att), bucketName(rid))
		f, err := os.Open(path)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		io.Copy(w, f)
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	return srv, ln.Addr().String(), nil
}

func FetchShuffleBucket(baseURL string, m MapOutputManifest, reduceID int, codec RecordCodec) ([]any, error) {
	return FetchShuffleBucketContext(context.Background(), baseURL, m, reduceID, codec)
}

func FetchShuffleBucketContext(ctx context.Context, baseURL string, m MapOutputManifest, reduceID int, codec RecordCodec) ([]any, error) {
	if codec == nil {
		codec = DefaultCodec()
	}
	url := fmt.Sprintf("%s/shuffle/%s/%d/%d/%d/%d", strings.TrimRight(baseURL, "/"), m.JobID, m.ShuffleID, m.MapID, m.Attempt, reduceID)
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("fetch shuffle %s: %s %s", url, resp.Status, body)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp("", "gospark-shuf-*")
	if err != nil {
		return nil, err
	}
	name := tmp.Name()
	defer os.Remove(name)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return nil, err
	}
	tmp.Close()
	return readBucketFile(name, codec)
}
