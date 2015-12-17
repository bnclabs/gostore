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
		"minblock,maxblock,arena-capacity,pool-capacity for nodes")
	flag.StringVar(&valarena, "valarena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for values")
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
		storage.MinKeymem, storage.MaxKeymem, 10 * 1024 * 1024, 1024 * 1024,
	}
	if nodearena != "" {
		for i, s := range strings.Split(nodearena, ",") {
			ln, _ := strconv.Atoi(s)
			options.nodearena[i] = ln
		}
	}
	options.valarena = [4]int{
		storage.MinKeymem, storage.MaxKeymem, 10 * 1024 * 1024, 1024 * 1024,
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
	llrb := storage.NewLLRB("cmdline", config, nil)
	now := time.Now()
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD123456), uint64(0)
	insertItems(llrb, vbno, vbuuid, seqno, options.n)
	fmt.Printf("Took %v to insert %v items\n", time.Since(now), options.n)
	printutilization(llrb)
	llrb.Release()
	if takeMEMProfile(options.mprof) {
		fmt.Printf("dumped mem-profile to %v\n", options.mprof)
	}
}

func insertItems(llrb *storage.LLRB, vbno uint16, vbuuid, seqno uint64, count int) {
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
		llrb.Upsert(key, value)
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

func printutilization(llrb *storage.LLRB) {
	min := hm.Bytes(uint64(options.nodearena[0]))
	max := hm.Bytes(uint64(options.nodearena[1]))
	cp := hm.Bytes(uint64(options.nodearena[2]))
	pcp := hm.Bytes(uint64(options.nodearena[3]))
	overhead, useful := llrb.NodeArena()
	overh := hm.Bytes(uint64(overhead))
	use := hm.Bytes(uint64(useful))
	alloc := hm.Bytes(uint64(llrb.NodeAllocated()))
	kmem := hm.Bytes(uint64(llrb.KeyMemory()))
	avail := hm.Bytes(uint64(llrb.NodeAvailable()))
	nblocks := len(llrb.NodeBlocks())
	fmsg := "Nodes blksz:{%v-%v/%v} cap:{%v/%v} mem:{%v,%v - %v,%v} avail:%v\n"
	fmt.Printf(fmsg, min, max, nblocks, cp, pcp, use, overh, alloc, kmem, avail)

	min = hm.Bytes(uint64(options.valarena[0]))
	max = hm.Bytes(uint64(options.valarena[1]))
	cp = hm.Bytes(uint64(options.valarena[2]))
	pcp = hm.Bytes(uint64(options.valarena[3]))
	overhead, useful = llrb.ValueArena()
	overh = hm.Bytes(uint64(overhead))
	use = hm.Bytes(uint64(useful))
	alloc = hm.Bytes(uint64(llrb.ValueAllocated()))
	vmem := hm.Bytes(uint64(llrb.ValueMemory()))
	avail = hm.Bytes(uint64(llrb.ValueAvailable()))
	vblocks := len(llrb.ValueBlocks())
	fmsg = "Value blksz:{%v-%v/%v} cap:{%v/%v} mem:{%v,%v - %v,%v} avail:%v\n"
	fmt.Printf(fmsg, min, max, vblocks, cp, pcp, use, overh, alloc, vmem, avail)

	llrb.LogNodeutilz()
	llrb.LogValueutilz()
	llrb.LogUpsertDepth()
	llrb.LogHeightStats()
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
