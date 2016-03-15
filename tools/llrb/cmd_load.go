package main

import "time"
import "os"
import "sync"
import "fmt"
import "strconv"
import "flag"
import "strings"
import "runtime"
import "runtime/pprof"
import "runtime/debug"

import "github.com/prataprc/storage.go"

var loadopts struct {
	nodearena [4]int // min,max,cap
	valarena  [4]int // min,max,cap
	klen      [2]int // min-klen, max-klen
	vlen      [2]int // min-vlen, max-vlen
	n         int
	ncpu      int
	mvcc      int
	memstats  int
	mprof     string
	pprof     string
	dotfile   string
	args      []string
}

func parseLoadopts(args []string) {
	f := flag.NewFlagSet("load", flag.ExitOnError)

	var nodearena, valarena, klen, vlen string

	f.StringVar(&nodearena, "nodearena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for nodes")
	f.StringVar(&valarena, "valarena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for values")
	f.StringVar(&klen, "klen", "",
		"minklen, maxklen - generate keys between [minklen,maxklen)")
	f.StringVar(&vlen, "vlen", "",
		"minvlen, maxvlen - generate values between [minklen,maxklen)")
	f.IntVar(&loadopts.n, "n", 1000,
		"number of items to generate and insert")
	f.IntVar(&loadopts.ncpu, "ncpu", runtime.NumCPU(),
		"set number cores to use.")
	f.IntVar(&loadopts.mvcc, "mvcc", 0,
		"enabled mvcc for load.")
	f.IntVar(&loadopts.memstats, "stats", 1000,
		"log llrb stats for every tick, in ms")
	f.StringVar(&loadopts.mprof, "mprof", "",
		"dump mem-profile to file")
	f.StringVar(&loadopts.pprof, "pprof", "",
		"dump cpu-profile to file")
	f.Parse(args)

	loadopts.nodearena = [4]int{
		storage.MinKeymem, storage.MaxKeymem, 10 * 1024 * 1024, 1024 * 1024,
	}
	if nodearena != "" {
		for i, s := range strings.Split(nodearena, ",") {
			ln, _ := strconv.Atoi(s)
			loadopts.nodearena[i] = ln
		}
	}
	loadopts.valarena = [4]int{
		storage.MinKeymem, storage.MaxKeymem, 10 * 1024 * 1024, 1024 * 1024,
	}
	if valarena != "" {
		for i, s := range strings.Split(valarena, ",") {
			ln, _ := strconv.Atoi(s)
			loadopts.valarena[i] = ln
		}
	}
	loadopts.klen = [2]int{64, 128}
	if klen != "" {
		for i, s := range strings.Split(klen, ",") {
			ln, _ := strconv.Atoi(s)
			loadopts.klen[i] = ln
		}
	}
	loadopts.vlen = [2]int{64, 128}
	if vlen != "" {
		for i, s := range strings.Split(vlen, ",") {
			ln, _ := strconv.Atoi(s)
			loadopts.vlen[i] = ln
		}
	}
	setCPU(loadopts.ncpu)
}

func doLoad(args []string) {
	parseLoadopts(args)

	// start memory statistic logger
	go storage.MemstatLogger(int64(loadopts.memstats))
	if loadopts.pprof != "" {
		fd, err := os.Create(loadopts.pprof)
		if err != nil {
			fmt.Printf("unable to create %q: %v\n", loadopts.pprof, err)
		}
		defer fd.Close()

		pprof.StartCPUProfile(fd)
		defer pprof.StopCPUProfile()
	}

	config := map[string]interface{}{
		"maxvb":                   1024,
		"mvcc.enable":             loadopts.mvcc > 0,
		"mvcc.snapshot.tick":      5,
		"mvcc.writer.chanbuffer":  1000,
		"nodearena.minblock":      loadopts.nodearena[0],
		"nodearena.maxblock":      loadopts.nodearena[1],
		"nodearena.capacity":      loadopts.nodearena[2],
		"nodearena.pool.capacity": loadopts.nodearena[3],
		"valarena.minblock":       loadopts.valarena[0],
		"valarena.maxblock":       loadopts.valarena[1],
		"valarena.capacity":       loadopts.valarena[2],
		"valarena.pool.capacity":  loadopts.valarena[3],
		"metadata.bornseqno":      true,
	}

	llrb := storage.NewLLRB("load", config, nil)

	now := time.Now()
	gench := make(chan [2][]byte, 100000)
	go loadgenerate(loadopts.n, gench)
	var wg sync.WaitGroup
	for i := 0; i < runtime.NumCPU()+loadopts.mvcc; i++ {
		vbno, vbuuid, seqno := uint16(i), uint64(0xABCD123456), uint64(0)
		wg.Add(1)
		go insertItems(llrb, vbno, vbuuid, seqno, gench, &wg)
	}
	wg.Wait()
	fmt.Printf("Took %v to insert %v items\n", time.Since(now), loadopts.n)
	llrb.Log(9, true)

	llrb.Validate()
	llrb.Destroy()

	if takeMEMProfile(loadopts.mprof) {
		fmt.Printf("dumped mem-profile to %v\n", loadopts.mprof)
	}
}

func insertItems(
	llrb *storage.LLRB, vbno uint16, vbuuid, seqno uint64,
	gench chan [2][]byte, wg *sync.WaitGroup) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic: %v\n", r)
			fmt.Printf("\n%s", getStackTrace(2, debug.Stack()))
		}
		wg.Done()
	}()

	for item := range gench {
		key, value := item[0], item[1]
		llrb.Upsert(
			key, value,
			func(index storage.Index, newnd, oldnd storage.Node) {
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		seqno++
	}
}

func loadgenerate(count int, gench chan [2][]byte) {
	maxkey, maxval := loadopts.klen[1], loadopts.vlen[1]
	for i := 0; i < count; i++ {
		key, value := make([]byte, maxkey), make([]byte, maxval)
		key = makekey(key, loadopts.klen[0], loadopts.klen[1])
		value = makeval(key, loadopts.vlen[0], loadopts.vlen[1])
		gench <- [2][]byte{key, value}
	}
	close(gench)
}
