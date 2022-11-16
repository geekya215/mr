package main

import (
	"fmt"
	"mr/core"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so addresss:port\n")
		os.Exit(1)
	}

	mapf, reducef := core.LoadPlugin(os.Args[1])

	worker := core.MakeWorker(mapf, reducef, os.Args[2])
	worker.Run()
}
