package main

import "fmt"
import "flag"
import "strings"
import "strconv"

import "github.com/prataprc/storage.go"
import hm "github.com/dustin/go-humanize"

var options struct {
	nodemem []int // min,max,cap
	valmem  []int // min,max,cap
}

func argParse() {
	var nodemem string
	var valmem string

	flag.StringVar(&nodemem, "nodemem", "",
		"minblock,maxblock,capacity for node memory arena")
	flag.StringVar(&valmem, "valmem", "",
		"minblock,maxblock,capacity for value memory arena")
	flag.Parse()

	options.nodemem = []int{llrb.MinKeymem, llrb.MaxKeymem, 10 * 1024 * 1024}
	if nodemem != "" {
		options.nodemem = []int{}
		for _, s := range strings.Split(nodemem, ",") {
			i, _ := strconv.Atoi(s)
			options.nodemem = append(options.nodemem, i)
		}
	}
	options.valmem = []int{llrb.MinKeymem, llrb.MaxKeymem, 10 * 1024 * 1024}
	if valmem != "" {
		options.valmem = []int{}
		for _, s := range strings.Split(valmem, ",") {
			i, _ := strconv.Atoi(s)
			options.valmem = append(options.valmem, i)
		}
	}
}

func main() {
	argParse()
	config := map[string]interface{}{
		"nodemem.minblock": options.nodemem[0],
		"nodemem.maxblock": options.nodemem[1],
		"nodemem.capacity": options.nodemem[2],
		"valmem.minblock":  options.valmem[0],
		"valmem.maxblock":  options.valmem[1],
		"valmem.capacity":  options.valmem[2],
	}
	t := llrb.NewLLRB(config)
	printutilization(t)
}

func printutilization(t *llrb.LLRB) {
	min := hm.Bytes(uint64(options.nodemem[0]))
	max := hm.Bytes(uint64(options.nodemem[1]))
	cp := hm.Bytes(uint64(options.nodemem[2]))
	fmt.Printf("nodemem{%v %v %v}\n", min, max, cp)

	min = hm.Bytes(uint64(options.valmem[1]))
	max = hm.Bytes(uint64(options.valmem[2]))
	cp = hm.Bytes(uint64(options.valmem[2]))
	fmt.Printf("valmem{%v %v %v}\n", min, max, cp)

	mem := hm.Bytes(uint64(t.Memory()))
	alloc := hm.Bytes(uint64(t.Allocated()))
	kmem := hm.Bytes(uint64(t.KeyMemory()))
	vmem := hm.Bytes(uint64(t.ValueMemory()))
	avail := hm.Bytes(uint64(t.Available()))
	nblocks, vblocks := len(t.NodeBlocks()), len(t.ValueBlocks())
	fmsg := "Memory:%v; Allocated:{%v,%v,%v}; Available:%v; Blocks:{%v,%v}\n"
	fmt.Printf(fmsg, mem, alloc, kmem, vmem, avail, nblocks, vblocks)
}
