package main

import "fmt"
import "flag"

import "github.com/prataprc/llrb.go"

var options struct {
	minblock int
	maxblock int
}

func argParse() {
	flag.IntVar(&options.minblock, "minblock", 32,
		"minimum block size")
	flag.IntVar(&options.maxblock, "maxblock", 1024*1024,
		"maximum block size")
	flag.Parse()
}

func main() {
	argParse()
	tellutilization()
}

func tellutilization() {
	sizes := llrb.Blocksizes(options.minblock, options.maxblock)
	fmt.Println(sizes, options.minblock, options.maxblock)
	for i, size := range sizes[1:] {
		u := (float64(sizes[i]+sizes[i+1]) / 2.0) / float64(size)
		fmt.Printf("size %4v, util %v\n", size, u)
	}
	fmt.Printf("total %v size pools\n", len(sizes))
}
