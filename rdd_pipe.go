package spark

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"strings"
)

// Pipe runs command once per partition, writing each record as a line to stdin
// and returning stdout lines. The command is interpreted by the system shell.
// Input and output for each partition are buffered until the process exits.
func Pipe[T any](rdd *RDD[T], command string) *RDD[string] {
	return MapPartitions(rdd, func(iter Iterator[T]) Iterator[string] {
		var input strings.Builder
		for value, ok := iter(); ok; value, ok = iter() {
			if _, err := fmt.Fprintln(&input, value); err != nil {
				panic(executionError{err})
			}
		}
		cmd := exec.CommandContext(rdd.ctx.TaskContext(), "sh", "-c", command)
		cmd.Stdin = strings.NewReader(input.String())
		var output, stderr bytes.Buffer
		cmd.Stdout = &output
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			panic(executionError{fmt.Errorf("pipe %q: %w: %s", command, err, strings.TrimSpace(stderr.String()))})
		}
		var lines []string
		scanner := bufio.NewScanner(&output)
		scanner.Buffer(make([]byte, 4096), output.Len()+1)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			panic(executionError{fmt.Errorf("pipe %q output: %w", command, err)})
		}
		return SliceIterator(lines)
	})
}
