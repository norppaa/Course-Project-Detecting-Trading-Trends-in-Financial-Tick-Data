// Command bench holds the benchmarking tools. bench/run.sh uses all of them;
// see bench/README.md.
//
//	bench sink     records the analyzer's output with receive timestamps
//	bench sample   records CPU and memory of running processes
//	bench analyze  turns a run folder into a summary and time series
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "sink":
		err = runSink(os.Args[2:])
	case "sample":
		err = runSample(os.Args[2:])
	case "analyze":
		err = runAnalyze(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "bench %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: bench sink|sample|analyze [flags]")
	os.Exit(2)
}
