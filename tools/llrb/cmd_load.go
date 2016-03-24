package main

import "time"
import "os"
import "sync"
import "fmt"
import "bytes"
import "io/ioutil"
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
	par       int
	batchsize int
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
	f.IntVar(&loadopts.par, "par", 16,
		"number of load generators")
	f.IntVar(&loadopts.memstats, "stats", 1000,
		"log llrb stats for every tick, in ms")
	f.IntVar(&loadopts.batchsize, "batchsize", 100,
		"batchsize for loading bulk-upserts")
	f.StringVar(&loadopts.mprof, "mprof", "",
		"dump mem-profile to file")
	f.StringVar(&loadopts.pprof, "pprof", "",
		"dump cpu-profile to file")
	f.StringVar(&loadopts.dotfile, "dotfile", "",
		"dump dot file output of the LLRB tree")
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
		"metadata.vbuuid":         true,
		"metadata.bornseqno":      true,
		"metadata.deadseqno":      false,
		"metadata.mvalue":         false,
	}

	llrb := storage.NewLLRB("load", config, nil)

	now := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < loadopts.par; i++ {
		vbno, vbuuid, seqno := uint16(i), uint64(0xABCD123456), uint64(0)
		wg.Add(1)
		go insertItems(llrb, vbno, vbuuid, seqno, &wg)
	}
	wg.Wait()
	fmt.Printf("Took %v to insert %v items\n", time.Since(now), llrb.Count())
	llrb.Log(9, true)

	llrb.Validate()

	if takeMEMProfile(loadopts.mprof) {
		fmt.Printf("dumped mem-profile to %v\n", loadopts.mprof)
	}
	if len(loadopts.dotfile) > 0 {
		buffer := bytes.NewBuffer(nil)
		llrb.Dotdump(buffer)
		ioutil.WriteFile(loadopts.dotfile, buffer.Bytes(), 0666)
	}

	llrb.Destroy()
}

func insertItems(
	llrb *storage.LLRB, vbno uint16, vbuuid, seqno uint64, wg *sync.WaitGroup) {

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("panic: %v\n", r)
			fmt.Printf("\n%s", getStackTrace(2, debug.Stack()))
		}
		wg.Done()
	}()

	batchsize := loadopts.batchsize
	keys, values := make([][]byte, batchsize), make([][]byte, batchsize)
	for i := range keys {
		keys[i] = make([]byte, 256)
		values[i] = make([]byte, 256)
	}
	for items := loadopts.n; items > 0; items -= batchsize {
		keys, values = keys[:batchsize], values[:batchsize]
		if items < batchsize {
			keys, values = keys[:items], values[:batchsize]
		}
		keys, values = loadgenerate(keys, values)
		llrb.UpsertMany(
			keys, values,
			func(index storage.Index, i int64, newnd, oldnd storage.Node) {
				newnd.Setvbno(vbno).SetVbuuid(vbuuid)
				newnd.SetBornseqno(seqno + uint64(i))
			})
	}
}

func loadgenerate(keys, values [][]byte) ([][]byte, [][]byte) {
	var scratch [256]byte
	ns := time.Now().UnixNano()
	for i := range keys {
		suffix := strconv.AppendInt(scratch[:0], ns+int64(i), 10)
		key, value := keys[i], values[i]
		key, value = key[:cap(key)], value[:cap(values[i])]
		key = makekey(key, loadopts.klen[0], loadopts.klen[1])
		n := copy(key[len(key):cap(key)], suffix)
		keys[i] = key[:len(key)+n]
		values[i] = makeval(value, loadopts.vlen[0], loadopts.vlen[1])
	}
	return keys, values
}
