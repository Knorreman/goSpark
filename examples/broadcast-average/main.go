package main

import (
	"fmt"
	"os"

	"goSpark/examples/broadcastaverage"
)

func main() {
	result, err := broadcastaverage.Local()
	if err == nil {
		err = broadcastaverage.Check(result)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("total=%g averages=%v\n", result.Total, result.Averages)
}
