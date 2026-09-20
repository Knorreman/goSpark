package spark

import (
	"runtime"
)

func runtimeNumCPU() int {
	return runtime.NumCPU()
}
