package main

import "fmt"
import "time"
import "os"
import "runtime"
import "flag"
import "strings"
import "math/rand"
import "strconv"
import _ "net/http/pprof"
import "net/http"
import "runtime/pprof"

import "github.com/prataprc/storage.go"
import hm "github.com/dustin/go-humanize"

var options struct {
	nodearena [4]int // min,max,cap
	valarena  [4]int // min,max,cap
	klen      [2]int // min-klen, max-klen
	vlen      [2]int // min-vlen, max-vlen
	n         int
	ncpu      int
	memtick   int
	mprof     string
	pprof     string
}

func argParse() {
	var nodearena, valarena, klen, vlen string

	flag.StringVar(&nodearena, "nodearena", "",
		"minblock,maxblock,capacity for node memory arena")
	flag.StringVar(&valarena, "valarena", "",
		"minblock,maxblock,capacity for value memory arena")
	flag.StringVar(&klen, "klen", "",
		"minklen, maxklen - generate keys between [minklen,maxklen)")
	flag.StringVar(&vlen, "vlen", "",
		"minvlen, maxvlen - generate values between [minklen,maxklen)")
	flag.IntVar(&options.n, "n", 1000,
		"number of items to generate and insert")
	flag.IntVar(&options.ncpu, "ncpu", 1,
		"set number cores to use.")
	flag.IntVar(&options.memtick, "memtick", 1000,
		"log memory stats for every tick, in ms")
	flag.StringVar(&options.mprof, "mprof", "",
		"dump mem-profile to file")
	flag.StringVar(&options.pprof, "pprof", "",
		"dump cpu-profile to file")
	flag.Parse()

	options.nodearena = [4]int{
		llrb.MinKeymem, llrb.MaxKeymem, 10 * 1024 * 1024, 1024 * 1024,
	}
	if nodearena != "" {
		for i, s := range strings.Split(nodearena, ",") {
			ln, _ := strconv.Atoi(s)
			options.nodearena[i] = ln
		}
	}
	options.valarena = [4]int{
		llrb.MinKeymem, llrb.MaxKeymem, 10 * 1024 * 1024, 1024 * 1024,
	}
	if valarena != "" {
		for i, s := range strings.Split(valarena, ",") {
			ln, _ := strconv.Atoi(s)
			options.valarena[i] = ln
		}
	}
	options.klen = [2]int{64, 128}
	if klen != "" {
		for i, s := range strings.Split(klen, ",") {
			ln, _ := strconv.Atoi(s)
			options.klen[i] = ln
		}
	}
	options.vlen = [2]int{64, 128}
	if vlen != "" {
		for i, s := range strings.Split(vlen, ",") {
			ln, _ := strconv.Atoi(s)
			options.vlen[i] = ln
		}
	}
}

func main() {
	argParse()
	// set CPU
	fmt.Printf("Setting number of cpus to %v\n", options.ncpu)
	runtime.GOMAXPROCS(options.ncpu)
	// start memory statistic logger
	go MemstatLogger(int64(options.memtick))
	// start pprof
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	if options.pprof != "" {
		fd, err := os.Create(options.pprof)
		if err != nil {
			fmt.Printf("unable to create %q: %v\n", options.pprof, err)
		}
		defer fd.Close()

		pprof.StartCPUProfile(fd)
		defer pprof.StopCPUProfile()
	}

	config := map[string]interface{}{
		"nodearena.minblock": options.nodearena[0],
		"nodearena.maxblock": options.nodearena[1],
		"nodearena.capacity": options.nodearena[2],
		"nodepool.capacity":  options.nodearena[3],
		"valarena.minblock":  options.valarena[0],
		"valarena.maxblock":  options.valarena[1],
		"valarena.capacity":  options.valarena[2],
		"valpool.capacity":   options.valarena[3],
	}
	t := llrb.NewLLRB("cmdline", config, nil)
	now := time.Now()
	insertItems(t, 10 /*vbno*/, 0xABCD123456 /*vbuuid*/, 0 /*seqno*/, options.n)
	fmt.Printf("Took %v to populate %v items\n", time.Since(now), options.n)
	printutilization(t)
	t.Release()
	if takeMEMProfile(options.mprof) {
		fmt.Printf("dumped mem-profile to %v\n", options.mprof)
	}
}

func insertItems(t *llrb.LLRB, vbno uint16, vbuuid, seqno uint64, count int) {
	startseqno := seqno
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic: %v\n", r)
		}
		fmt.Printf("Inserted %v items\n", seqno-startseqno)
	}()

	maxkey, maxval := options.klen[1], options.vlen[1]
	key, value := make([]byte, maxkey), make([]byte, maxval)
	for i := 0; i < count; i++ {
		key, value := makekeyval(key, value)
		t.Upsert(key, value, vbno, vbuuid, seqno)
		seqno++
	}
}

func makekeyval(key, value []byte) (k, v []byte) {
	min, max := options.klen[0], options.klen[1]
	if max-min > 0 {
		k = key[:rand.Intn(max-min)+min]
		for i := range k {
			k[i] = byte(97 + rand.Intn(26))
		}
	}

	min, max = options.vlen[0], options.vlen[1]
	if max-min > 0 {
		v = value[:rand.Intn(max-min)+min]
		for i := range v {
			v[i] = byte(97 + rand.Intn(26))
		}
	}
	return k, v
}

func printutilization(t *llrb.LLRB) {
	min := hm.Bytes(uint64(options.nodearena[0]))
	max := hm.Bytes(uint64(options.nodearena[1]))
	cp := hm.Bytes(uint64(options.nodearena[2]))
	pcp := hm.Bytes(uint64(options.nodearena[3]))
	overhead, useful := t.NodeArena()
	overh := hm.Bytes(uint64(overhead))
	use := hm.Bytes(uint64(useful))
	alloc := hm.Bytes(uint64(t.NodeAllocated()))
	kmem := hm.Bytes(uint64(t.KeyMemory()))
	avail := hm.Bytes(uint64(t.NodeAvailable()))
	nblocks := len(t.NodeBlocks())
	fmsg := "Nodes{min:%v max:%v cap:%v,%v mem:%v,%v alloc:{%v,%v} avail:%v blks:%v}\n"
	fmt.Printf(fmsg, min, max, cp, pcp, overh, use, alloc, kmem, avail, nblocks)

	min = hm.Bytes(uint64(options.valarena[0]))
	max = hm.Bytes(uint64(options.valarena[1]))
	cp = hm.Bytes(uint64(options.valarena[2]))
	pcp = hm.Bytes(uint64(options.valarena[3]))
	overhead, useful = t.ValueArena()
	overh = hm.Bytes(uint64(overhead))
	use = hm.Bytes(uint64(useful))
	alloc = hm.Bytes(uint64(t.ValueAllocated()))
	vmem := hm.Bytes(uint64(t.ValueMemory()))
	avail = hm.Bytes(uint64(t.ValueAvailable()))
	vblocks := len(t.ValueBlocks())
	fmsg = "Value{min:%v max:%v cap:{%v,%v} mem:{%v,%v} alloc:{%v,%v} avail:%v blks:%v}\n"
	fmt.Printf(fmsg, min, max, cp, pcp, overh, use, alloc, vmem, avail, vblocks)

	t.LogNodeutilz()
	t.LogValueutilz()
}

func takeMEMProfile(filename string) bool {
	if filename == "" {
		return false
	}
	fd, err := os.Create(filename)
	if err != nil {
		fmt.Println(err)
		return false
	}
	pprof.WriteHeapProfile(fd)
	defer fd.Close()
	return true
}
