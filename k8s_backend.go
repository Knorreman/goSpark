package spark

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"os/exec"

	"strings"
	"sync"
	"time"
)

func k8sCreateNamespace(namespace string) error {
	manifest := fmt.Sprintf(`
apiVersion: v1
kind: Namespace
metadata:
  name: %s
`, namespace)
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply failed: %v\n%s", err, string(output))
	}
	return nil
}

func k8sDeleteNamespace(namespace string) error {
	cmd := exec.Command("kubectl", "delete", "namespace", namespace, "--ignore-not-found", "--grace-period=0")
	return cmd.Run()
}

func k8sApplyManifest(manifest string) error {
	cmd := exec.Command("kubectl", "apply", "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl apply failed: %v\n%s", err, string(output))
	}
	return nil
}

func k8sGetPodLogs(namespace, podName string) (string, error) {
	cmd := exec.Command("kubectl", "logs", "-n", namespace, podName)
	output, err := cmd.CombinedOutput()
	return string(output), err
}

func k8sWaitForJob(namespace, jobName string, completions int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for job %s/%s", namespace, jobName)
		case <-ticker.C:
			cmd := exec.Command("kubectl", "get", "job", jobName, "-n", namespace,
				"-o", fmt.Sprintf("jsonpath={.status.succeeded}"))
			output, err := cmd.CombinedOutput()
			if err != nil {
				continue
			}
			s := strings.TrimSpace(string(output))
			if s == fmt.Sprintf("%d", completions) {
				return nil
			}
		}
	}
}

type K8sClusterTest struct {
	Namespace string
	Image     string
}

func NewK8sClusterTest(namespace, image string) *K8sClusterTest {
	return &K8sClusterTest{Namespace: namespace, Image: image}
}

func (t *K8sClusterTest) Setup() error {
	k8sDeleteNamespace(t.Namespace)
	time.Sleep(2 * time.Second)
	return k8sCreateNamespace(t.Namespace)
}

func (t *K8sClusterTest) Teardown() error {
	return k8sDeleteNamespace(t.Namespace)
}

func (t *K8sClusterTest) RunJob(jobName, command string, parallelism, completions int) error {
	var completionMode string
	if completions > 1 {
		completionMode = `
  completionMode: Indexed`
	} else {
		completionMode = ""
	}
	manifest := fmt.Sprintf(`
apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
spec:
  parallelism: %d
  completions: %d%s
  template:
    spec:
      containers:
      - name: worker
        image: %s
        imagePullPolicy: IfNotPresent
        command: ["./gospark-worker", "%s"]
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
        - name: OUTPUT_DIR
          value: "/tmp/gospark-output"
      restartPolicy: OnFailure
`, jobName, t.Namespace, parallelism, completions, completionMode, t.Image, command)

	if err := k8sApplyManifest(manifest); err != nil {
		return fmt.Errorf("failed to create job %s: %w", jobName, err)
	}

	if err := k8sWaitForJob(t.Namespace, jobName, completions, 120*time.Second); err != nil {
		pods, _ := k8sGetJobPods(t.Namespace, jobName)
		for _, pod := range pods {
			logs, _ := k8sGetPodLogs(t.Namespace, pod)
			fmt.Printf("=== Pod %s logs ===\n%s\n", pod, logs)
		}
		return err
	}
	return nil
}

func (t *K8sClusterTest) RunSaveJob(jobName, outputDir string, parallelism int) error {
	var pvcManifest string
	if outputDir != "" {
		pvcManifest = fmt.Sprintf(`
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: gospark-output
  namespace: %s
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 1Gi
  storageClassName: standard
`, t.Namespace)
	}

	volumeMounts := ""
	volumes := ""
	if outputDir != "" {
		volumeMounts = `
        volumeMounts:
        - name: output
          mountPath: /tmp/gospark-output`
		volumes = `
      volumes:
      - name: output
        persistentVolumeClaim:
          claimName: gospark-output`
	}

	manifest := fmt.Sprintf(`
%s
apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
spec:
  parallelism: %d
  completions: %d
  completionMode: Indexed
  template:
    spec:
      containers:
      - name: worker
        image: %s
        imagePullPolicy: IfNotPresent
        command: ["./gospark-worker", "savetest"]
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
        - name: OUTPUT_DIR
          value: "%s"
%s
      restartPolicy: OnFailure
%s
`, pvcManifest, jobName, t.Namespace, parallelism, parallelism, t.Image, outputDir, volumeMounts, volumes)

	if err := k8sApplyManifest(manifest); err != nil {
		return fmt.Errorf("failed to create save job %s: %w", jobName, err)
	}

	if err := k8sWaitForJob(t.Namespace, jobName, parallelism, 120*time.Second); err != nil {
		return err
	}
	return nil
}

func k8sGetJobPods(namespace, jobName string) ([]string, error) {
	cmd := exec.Command("kubectl", "get", "pods", "-n", namespace,
		"-l", "job-name="+jobName,
		"-o", "jsonpath={.items[*].metadata.name}")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, err
	}
	return strings.Fields(strings.TrimSpace(string(output))), nil
}

type K8sBackend struct {
	config K8sConfig
	ctx    context.Context
}

func NewK8sBackend(config K8sConfig) *K8sBackend {
	return &K8sBackend{
		config: config,
		ctx:    context.Background(),
	}
}

func (b *K8sBackend) SubmitJob(rddAny RDDAny, action func(RDDAny) error) error {
	computeShuffleStages(rddAny)
	return action(rddAny)
}

func RunOnK8s[T any](rdd *RDD[T], config K8sConfig) [][]T {
	computeShuffleStages(rdd)
	partitions := rdd.Partitions()

	results := make([][]T, len(partitions))
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for i, p := range partitions {
		go func(idx int, partition Partition) {
			defer wg.Done()
			data := CollectIterator(rdd.Compute(partition))
			mu.Lock()
			results[idx] = data
			mu.Unlock()
		}(i, p)
	}

	wg.Wait()
	return results
}

func (b *K8sBackend) CreateWorkerPod(taskSpec TaskSpec) (string, error) {
	podName := fmt.Sprintf("gospark-worker-%d", taskSpec.PartitionID)
	manifest := K8sWorkerPodManifest(b.config.Namespace, b.config.Image, podName)
	if err := k8sApplyManifest(manifest); err != nil {
		return "", err
	}
	return podName, nil
}

func (b *K8sBackend) DeleteWorkerPod(podName string) error {
	cmd := exec.Command("kubectl", "delete", "pod", podName, "-n", b.config.Namespace, "--ignore-not-found")
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("delete pod %s: %v\n%s", podName, err, string(output))
	}
	return nil
}

type TaskSpec struct {
	JobID       string `json:"job_id"`
	StageID     int    `json:"stage_id"`
	PartitionID int    `json:"partition_id"`
	RDDID       int    `json:"rdd_id"`
	DataURL     string `json:"data_url"`
	ResultURL   string `json:"result_url"`
}

type TaskResult struct {
	PartitionID int    `json:"partition_id"`
	Data        []byte `json:"data"`
	Err         string `json:"error,omitempty"`
}

type K8sConfig struct {
	Namespace      string
	Image          string
	ServiceAccount string
	Workers        int
	Timeout        time.Duration
	DriverHost     string
	DriverPort     int
}

func DefaultK8sConfig() K8sConfig {
	return K8sConfig{
		Namespace:      "gospark",
		Image:          "gospark/worker:latest",
		Workers:        3,
		Timeout:        5 * time.Minute,
		DriverPort:     38101,
		ServiceAccount: "default",
	}
}

type driverServer struct {
	mu         sync.Mutex
	partitions map[int][]byte
	results    map[int][]byte
	errors     map[int]error
	completed  chan struct{}
	port       int
	rddAny     RDDAny
	conf       K8sConfig
}

type taskRequest struct {
	PartitionID int `json:"partition_id"`
}

type taskResponse struct {
	PartitionID int    `json:"partition_id"`
	Data        []byte `json:"data,omitempty"`
	Error       string `json:"error,omitempty"`
}

type resultSubmit struct {
	PartitionID int    `json:"partition_id"`
	Data        []byte `json:"data"`
	Error       string `json:"error,omitempty"`
}

func newDriverServer(rddAny RDDAny, conf K8sConfig) *driverServer {
	return &driverServer{
		partitions: make(map[int][]byte),
		results:    make(map[int][]byte),
		errors:     make(map[int]error),
		completed:  make(chan struct{}),
		port:       conf.DriverPort,
		rddAny:     rddAny,
		conf:       conf,
	}
}

func (d *driverServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasPrefix(path, "/task/") {
		var req taskRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		d.mu.Lock()
		data, ok := d.partitions[req.PartitionID]
		d.mu.Unlock()
		if !ok {
			http.Error(w, fmt.Sprintf("partition %d not found", req.PartitionID), http.StatusNotFound)
			return
		}
		resp := taskResponse{PartitionID: req.PartitionID, Data: data}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	} else if strings.HasPrefix(path, "/result/") {
		var submit resultSubmit
		if err := json.NewDecoder(r.Body).Decode(&submit); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		d.mu.Lock()
		if submit.Error != "" {
			d.errors[submit.PartitionID] = fmt.Errorf("%s", submit.Error)
		} else {
			d.results[submit.PartitionID] = submit.Data
		}
		totalResults := len(d.results) + len(d.errors)
		d.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		if totalResults >= len(d.partitions) && len(d.partitions) > 0 {
			select {
			case d.completed <- struct{}{}:
			default:
			}
		}
	} else if path == "/health" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (b *K8sBackend) startDriverServer(port int) (*http.Server, error) {
	mux := http.NewServeMux()
	server := &http.Server{Addr: fmt.Sprintf(":%d", port), Handler: mux}
	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond)
	return server, nil
}

func (b *K8sBackend) stopDriverServer(server *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}

func K8sIntegrationTest() error {
	namespace := "gospark-test"
	image := "localhost/gospark/worker:latest"

	fmt.Println("=== goSpark K8s Integration Test ===")
	fmt.Println()

	test := NewK8sClusterTest(namespace, image)

	fmt.Println("1. Setting up namespace...")
	k8sDeleteNamespace(namespace)
	time.Sleep(2 * time.Second)
	if err := k8sCreateNamespace(namespace); err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}

	fmt.Println("2. Smoke test...")
	if err := test.RunJob("gospark-smoke", "smoke", 1, 1); err != nil {
		return fmt.Errorf("smoke test failed: %w", err)
	}
	fmt.Println("   PASSED!")

	fmt.Println("3. Word count (3 pods)...")
	if err := test.RunJob("gospark-wordcount", "wordcount", 3, 3); err != nil {
		return fmt.Errorf("word count failed: %w", err)
	}
	fmt.Println("   PASSED!")

	fmt.Println("4. Distributed computation (3 pods)...")
	if err := test.RunJob("gospark-distributed", "distributed", 3, 3); err != nil {
		return fmt.Errorf("distributed test failed: %w", err)
	}
	fmt.Println("   PASSED!")

	fmt.Println("5. SaveAsTextFile test...")
	if err := test.RunJob("gospark-save", "savetest", 1, 1); err != nil {
		return fmt.Errorf("save test failed: %w", err)
	}
	fmt.Println("   PASSED!")

	fmt.Println()
	fmt.Printf("Namespace %s left for inspection. Clean up with:\n", namespace)
	fmt.Printf("  kubectl delete namespace %s\n", namespace)
	fmt.Println()
	fmt.Println("=== All K8s integration tests PASSED! ===")
	return nil
}
