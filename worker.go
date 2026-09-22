package spark

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type execTaskResponse struct {
	PartitionID int                `json:"partition_id"`
	Kind        StageKind          `json:"kind"`
	Manifest    *MapOutputManifest `json:"manifest,omitempty"`
	RecordBlob  []byte             `json:"record_blob,omitempty"`
	Error       string             `json:"error,omitempty"`
}

func ServeWorker(storeDir, addr string) (*http.Server, string, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, "", err
	}
	baseURL := os.Getenv("GOSPARK_ADVERTISE_URL")
	if baseURL == "" {
		host := ln.Addr().String()
		host = strings.Replace(host, "[::]", "127.0.0.1", 1)
		baseURL = "http://" + host
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
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
		task.StoreDir = storeDir
		res, err := ExecuteTask(task)
		out := execTaskResponse{PartitionID: task.PartitionID}
		if err != nil {
			out.Error = err.Error()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(out)
			return
		}
		out.Kind = res.Kind
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
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
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
	Timeout time.Duration
}

func (c *WorkerClient) Alive() bool {
	if c == nil || c.BaseURL == "" {
		return false
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(strings.TrimRight(c.BaseURL, "/") + "/health")
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (r localRunner) Alive() bool { return true }

func (c *WorkerClient) Exec(task Task) (ExecResult, error) {
	body, err := json.Marshal(task)
	if err != nil {
		return ExecResult{}, err
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Post(strings.TrimRight(c.BaseURL, "/")+"/task", "application/json", bytes.NewReader(body))
	if err != nil {
		return ExecResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ExecResult{}, fmt.Errorf("worker returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return ExecResult{}, err
	}
	var out execTaskResponse
	if err := json.Unmarshal(data, &out); err != nil {
		return ExecResult{}, fmt.Errorf("decode task response: %w %s", err, data)
	}
	if out.Error != "" {
		return ExecResult{}, fmt.Errorf("%s", out.Error)
	}
	res := ExecResult{PartitionID: out.PartitionID, Kind: out.Kind, Manifest: out.Manifest}
	if len(out.RecordBlob) > 0 {
		recs, err := decodeRecordSlice(out.RecordBlob)
		if err != nil {
			return ExecResult{}, err
		}
		res.Records = recs
	}
	return res, nil
}
