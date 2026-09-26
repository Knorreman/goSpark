package spark

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const ProtocolVersion = 1

const (
	ActionCollect = "collect"
	ActionCount   = "count"
	ActionSave    = "save"
)

type JobSpec struct {
	TaskName           string            `json:"task_name"`
	CacheIdentity      string            `json:"cache_identity,omitempty"`
	Action             string            `json:"action,omitempty"`
	Params             map[string]string `json:"params,omitempty"`
	Broadcasts         []Broadcast       `json:"broadcasts,omitempty"`
	Accumulators       []AccumulatorDef  `json:"accumulators,omitempty"`
	InputSplits        []InputSplit      `json:"input_splits,omitempty"`
	NumPartitions      int               `json:"num_partitions,omitempty"`
	ProtocolVersion    int               `json:"protocol_version"`
	ImageDigest        string            `json:"image_digest,omitempty"`
	PipelinePhase      string            `json:"pipeline_phase,omitempty"`
	PipelineNode       int               `json:"pipeline_node,omitempty"`
	accumulatorContext *Context
}

func (s JobSpec) withDefaults() JobSpec {
	if s.ProtocolVersion == 0 {
		s.ProtocolVersion = ProtocolVersion
	}
	return s
}

type JobFactory func(ctx *Context, spec JobSpec) (RDDAny, error)

type ActionFunc func(rdd RDDAny, spec JobSpec) error

var (
	taskRegistryMu sync.Mutex
	jobRegistry    = make(map[string]JobFactory)
	actionRegistry = make(map[string]ActionFunc)
	builtinsOnce   sync.Once
)

func ensureBuiltinActions() {
	builtinsOnce.Do(func() {
		RegisterAction(ActionCollect, func(rdd RDDAny, spec JobSpec) error {
			computeShuffleStages(rdd)
			for _, p := range rdd.Partitions() {
				it := rdd.ComputeAny(p)
				for {
					_, ok := it()
					if !ok {
						break
					}
				}
			}
			return nil
		})
		RegisterAction(ActionCount, func(rdd RDDAny, spec JobSpec) error {
			computeShuffleStages(rdd)
			for _, p := range rdd.Partitions() {
				it := rdd.ComputeAny(p)
				for {
					_, ok := it()
					if !ok {
						break
					}
				}
			}
			return nil
		})
		RegisterAction(ActionSave, func(rdd RDDAny, spec JobSpec) error {
			path := ""
			if spec.Params != nil {
				path = spec.Params["path"]
			}
			if path == "" {
				return fmt.Errorf("save action requires params.path")
			}
			return RunPartition(rdd, 0, path)
		})
	})
}

func registerJob(name string, fn JobFactory) {
	taskRegistryMu.Lock()
	jobRegistry[name] = fn
	taskRegistryMu.Unlock()
}

func getJob(name string) (JobFactory, bool) {
	taskRegistryMu.Lock()
	fn, ok := jobRegistry[name]
	taskRegistryMu.Unlock()
	return fn, ok
}

func RegisterAction(name string, fn ActionFunc) {
	taskRegistryMu.Lock()
	actionRegistry[name] = fn
	taskRegistryMu.Unlock()
}

func GetAction(name string) (ActionFunc, bool) {
	ensureBuiltinActions()
	taskRegistryMu.Lock()
	fn, ok := actionRegistry[name]
	taskRegistryMu.Unlock()
	return fn, ok
}

func registeredRDD(ctx *Context, name string, partitions int) (RDDAny, error) {
	factory, ok := getJob(name)
	if !ok {
		return nil, fmt.Errorf("pipeline %q not registered", name)
	}
	return factory(ctx, JobSpec{TaskName: name, NumPartitions: partitions, PipelinePhase: "final"})
}

func WorkerRunPartition(taskName string, partitionIndex int, outputPath string) error {
	ctx := NewContext(&Config{Master: MasterLocal})
	defer ctx.Stop()
	rddAny, err := registeredRDD(ctx, taskName, 0)
	if err != nil {
		return err
	}
	return RunPartition(rddAny, partitionIndex, outputPath)
}

func WorkerRunPartitionFromEnv() error {
	taskName := os.Getenv("GOSPARK_TASK")
	if taskName == "" {
		return fmt.Errorf("GOSPARK_TASK environment variable not set")
	}
	outputPath := os.Getenv("GOSPARK_OUTPUT")
	if outputPath == "" {
		return fmt.Errorf("GOSPARK_OUTPUT environment variable not set")
	}
	partitionStr := os.Getenv("GOSPARK_PARTITION_INDEX")
	if partitionStr == "" {
		partitionStr = os.Getenv("POD_INDEX")
	}
	if partitionStr == "" {
		partitionStr = "0"
	}
	partitionIndex, err := strconv.Atoi(partitionStr)
	if err != nil {
		return fmt.Errorf("invalid partition index %q: %w", partitionStr, err)
	}
	return WorkerRunPartition(taskName, partitionIndex, outputPath)
}

type K8sDistributedSaveConfig struct {
	TaskName   string
	OutputPath string
	Namespace  string
	Image      string
	Workers    int
	Timeout    time.Duration
}

func K8sDistributedSave(ctx *Context, cfg K8sDistributedSaveConfig) error {
	tempCtx := NewContext(&Config{Master: MasterLocal, NumPartitions: cfg.Workers})
	rddAny, err := registeredRDD(tempCtx, cfg.TaskName, cfg.Workers)
	if err != nil {
		tempCtx.Stop()
		return err
	}
	computeShuffleStages(rddAny)
	numPartitions := len(rddAny.Partitions())
	tempCtx.Stop()

	if cfg.Namespace == "" {
		cfg.Namespace = "default"
	}
	if cfg.Workers <= 0 {
		cfg.Workers = numPartitions
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}
	if cfg.Workers > numPartitions {
		cfg.Workers = numPartitions
	}

	manifest := fmt.Sprintf(`
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
        command: ["./gospark-worker", "distributed-save"]
        env:
        - name: GOSPARK_TASK
          value: "%s"
        - name: GOSPARK_OUTPUT
          value: "%s"
        - name: POD_INDEX
          valueFrom:
            fieldRef:
              fieldPath: metadata.annotations['batch.kubernetes.io/job-completion-index']
      restartPolicy: OnFailure
`, cfg.TaskName, cfg.Namespace, cfg.Workers, cfg.Workers, cfg.Image, cfg.TaskName, cfg.OutputPath)

	if err := k8sApplyManifest(manifest); err != nil {
		return fmt.Errorf("failed to create distributed save job: %w", err)
	}

	jobName := cfg.TaskName
	if err := k8sWaitForJob(cfg.Namespace, jobName, cfg.Workers, cfg.Timeout); err != nil {
		pods, _ := k8sGetJobPods(cfg.Namespace, jobName)
		for _, pod := range pods {
			logs, _ := k8sGetPodLogs(cfg.Namespace, pod)
			fmt.Printf("=== Pod %s logs ===\n%s\n", pod, logs)
		}
		return fmt.Errorf("distributed save job failed: %w", err)
	}

	return nil
}
