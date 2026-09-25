package spark

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type execTaskResponse struct {
	JobID       string             `json:"job_id"`
	StageID     int                `json:"stage_id"`
	Attempt     int                `json:"attempt"`
	PartitionID int                `json:"partition_id"`
	Kind        StageKind          `json:"kind"`
	Manifest    *MapOutputManifest `json:"manifest,omitempty"`
	Output      *PartitionOutput   `json:"output,omitempty"`
	RecordBlob  []byte             `json:"record_blob,omitempty"`
	SampleBlob  []byte             `json:"sample_blob,omitempty"`
	Cached      []CachePartition   `json:"cached,omitempty"`
	Dropped     []CachePartition   `json:"dropped,omitempty"`
	Error       string             `json:"error,omitempty"`
	FetchError  *FetchError        `json:"fetch_error,omitempty"`
}

var jobIDPattern = regexp.MustCompile(`^job-[0-9a-f]{32}$`)

func validJobID(id string) bool { return jobIDPattern.MatchString(id) }

func ServeWorker(storeDir, addr string) (*http.Server, string, error) {
	budget, err := newDiskBudget(storeDir)
	if err != nil {
		return nil, "", err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	var incarnation [16]byte
	if _, err := rand.Read(incarnation[:]); err != nil {
		ln.Close()
		return nil, "", err
	}
	workerID := fmt.Sprintf("%x", incarnation)
	baseURL := os.Getenv("GOSPARK_ADVERTISE_URL")
	if baseURL == "" {
		host := ln.Addr().String()
		host = strings.Replace(host, "[::]", "127.0.0.1", 1)
		baseURL = "http://" + host
	}
	mux := http.NewServeMux()
	var activeMu sync.Mutex
	active := map[string]int{}
	jobCaches := map[string]*memoryCache{}
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-GoSpark-Worker-ID", workerID)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/task", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		var task Task
		if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		activeMu.Lock()
		active[task.JobID]++
		if validJobID(task.JobID) {
			if jobCaches[task.JobID] == nil {
				jobCaches[task.JobID] = newMemoryCache()
			}
			task.jobCache = jobCaches[task.JobID]
		}
		activeMu.Unlock()
		defer func() {
			activeMu.Lock()
			active[task.JobID]--
			if active[task.JobID] == 0 {
				delete(active, task.JobID)
			}
			activeMu.Unlock()
		}()
		task.StoreDir = storeDir
		task.budget = budget
		res, err := ExecuteTaskContext(r.Context(), task)
		if r.Context().Err() != nil {
			return
		}
		out := execTaskResponse{JobID: task.JobID, StageID: task.StageID, Attempt: task.Attempt, PartitionID: task.PartitionID}
		if err == nil && res.Output != nil && task.PartitionID == 0 && task.Attempt == 0 && os.Getenv("GOSPARK_TEST_FAIL_SAVE_ONCE") == "true" {
			out.Error = "injected lost save response after attempt upload"
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(out)
			return
		}
		if err != nil {
			out.Error = err.Error()
			errors.As(err, &out.FetchError)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(out)
			return
		}
		out.Kind = res.Kind
		out.Output = res.Output
		out.Cached = res.Cached
		out.Dropped = res.Dropped
		if res.Manifest != nil {
			man := *res.Manifest
			man.BaseURL = baseURL
			out.Manifest = &man
		}
		if len(res.Records) > 0 {
			blob, err := encodeRecordSlice(res.Records)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out.RecordBlob = blob
		}
		if len(res.Samples) > 0 {
			blob, err := encodeRecordSlice(res.Samples)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			out.SampleBlob = blob
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	mux.HandleFunc("/cleanup/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		jobID := strings.TrimPrefix(r.URL.Path, "/cleanup/")
		if !validJobID(jobID) {
			http.Error(w, "invalid job ID", http.StatusBadRequest)
			return
		}
		activeMu.Lock()
		defer activeMu.Unlock()
		if active[jobID] != 0 {
			http.Error(w, "job has active tasks", http.StatusConflict)
			return
		}
		if err := budget.removeDir(NewDiskShuffleStore(storeDir).jobDir(jobID)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		delete(jobCaches, jobID)
	})
	mux.HandleFunc("/shuffle/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/shuffle/")
		parts := strings.Split(rel, "/")
		if len(parts) != 5 {
			http.Error(w, "want /shuffle/{job}/{shuffle}/{map}/{attempt}/{reduce}", http.StatusBadRequest)
			return
		}
		sid, err1 := strconv.Atoi(parts[1])
		mid, err2 := strconv.Atoi(parts[2])
		att, err3 := strconv.Atoi(parts[3])
		rid, err4 := strconv.Atoi(parts[4])
		if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
			http.Error(w, "invalid shuffle path", http.StatusBadRequest)
			return
		}
		path := NewDiskShuffleStore(storeDir).mapDir(parts[0], sid, mid, att) + "/" + bucketName(rid)
		http.ServeFile(w, r, path)
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	return srv, ln.Addr().String(), nil
}

func encodeRecordSlice(recs []any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&recs); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeRecordSlice(blob []byte) ([]any, error) {
	var recs []any
	if err := gob.NewDecoder(bytes.NewReader(blob)).Decode(&recs); err != nil {
		return nil, err
	}
	return recs, nil
}

type WorkerClient struct {
	BaseURL string
	// Timeout bounds each RPC, including shuffle fetch and computation on the worker.
	Timeout           time.Duration
	HeartbeatInterval time.Duration
	mu                sync.Mutex
	workerID          string
}

func (c *WorkerClient) WorkerID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.workerID
}

func (c *WorkerClient) CleanupJobContext(ctx context.Context, jobID string) error {
	if !validJobID(jobID) {
		return fmt.Errorf("invalid job ID")
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/cleanup/" + jobID
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
			return fmt.Errorf("cleanup returned HTTP %d", resp.StatusCode)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (c *WorkerClient) Alive() bool {
	return c.AliveContext(context.Background())
}
func (c *WorkerClient) AliveContext(ctx context.Context) bool {
	if c == nil || c.BaseURL == "" {
		return false
	}
	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.BaseURL, "/")+"/health", nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	c.mu.Lock()
	c.workerID = resp.Header.Get("X-GoSpark-Worker-ID")
	c.mu.Unlock()
	return true
}

func (r localRunner) Alive() bool { return true }

func (c *WorkerClient) Exec(task Task) (ExecResult, error) {
	return c.ExecContext(context.Background(), task)
}

func (c *WorkerClient) ExecContext(parent context.Context, task Task) (ExecResult, error) {
	body, err := json.Marshal(task)
	if err != nil {
		return ExecResult{}, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	client := &http.Client{Timeout: timeout}
	deadline, cancelDeadline := context.WithTimeout(parent, timeout)
	defer cancelDeadline()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	interval := c.HeartbeatInterval
	if interval <= 0 {
		interval = time.Second
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !c.AliveContext(ctx) {
					cancel(fmt.Errorf("worker heartbeat failed"))
					return
				}
			}
		}
	}()
	defer func() { cancel(nil); <-done }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+"/task", bytes.NewReader(body))
	if err != nil {
		return ExecResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ExecResult{}, context.Cause(ctx)
		}
		return ExecResult{}, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return ExecResult{}, err
	}
	var out execTaskResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return ExecResult{}, fmt.Errorf("decode task response: %w %s", err, data)
	}
	if out.Error != "" {
		if out.FetchError != nil {
			return ExecResult{}, out.FetchError
		}
		return ExecResult{}, fmt.Errorf("%s", out.Error)
	}
	if out.JobID != task.JobID || out.StageID != task.StageID || out.Attempt != task.Attempt || out.PartitionID != task.PartitionID {
		return ExecResult{}, fmt.Errorf("stale or mismatched task response")
	}
	if err := ctx.Err(); err != nil {
		return ExecResult{}, context.Cause(ctx)
	}
	if resp.StatusCode != http.StatusOK {
		return ExecResult{}, fmt.Errorf("worker returned HTTP %d", resp.StatusCode)
	}
	res := ExecResult{PartitionID: out.PartitionID, Kind: out.Kind, Manifest: out.Manifest, Output: out.Output, Cached: out.Cached, Dropped: out.Dropped}
	if len(out.RecordBlob) > 0 {
		recs, err := decodeRecordSlice(out.RecordBlob)
		if err != nil {
			return ExecResult{}, err
		}
		res.Records = recs
	}
	if len(out.SampleBlob) > 0 {
		samples, err := decodeRecordSlice(out.SampleBlob)
		if err != nil {
			return ExecResult{}, err
		}
		res.Samples = samples
	}
	return res, nil
}
