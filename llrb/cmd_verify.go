package main

import "fmt"
import "flag"
import "strings"
import "strconv"
import "time"
import "log"
import "math/rand"

import "github.com/prataprc/storage.go"

var verifyopts struct {
	repeat int
	seed   int
	vtick  time.Duration
	stick  time.Duration
	rtick  time.Duration
	opdump bool
	bagdir string
	mvcc   int
	log    string
	// LLRB specific structures
	nodearena [4]int // min,max,cap,pcap
	valarena  [4]int // min,max,cap,pcap
	args      []string
}

func parseVerifyopts(args []string) {
	f := flag.NewFlagSet("verify", flag.ExitOnError)

	var nodearena, valarena string
	var vtick, stick, rtick int

	seed := time.Now().UTC().Second()
	f.IntVar(&verifyopts.repeat, "repeat", 1000,
		"number of times to repeat the generator")
	f.IntVar(&verifyopts.seed, "seed", seed,
		"seed value for generating inputs")
	f.IntVar(&vtick, "vtick", 1000,
		"validate tick, in milliseconds")
	f.IntVar(&stick, "stick", 1000,
		"periodically generate snapshot, period in milliseconds")
	f.IntVar(&rtick, "rtick", 1000,
		"periodically release snapshots, period in milliseconds")
	f.StringVar(&verifyopts.bagdir, "bagdir", "./",
		"bagdir for monster")
	f.IntVar(&verifyopts.mvcc, "mvcc", 0,
		"use as many as n mvcc readers on llrb")
	f.BoolVar(&verifyopts.opdump, "opdump", false,
		"dump monster generated ops")
	f.StringVar(&nodearena, "nodearena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for nodes")
	f.StringVar(&valarena, "valarena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for values")
	f.StringVar(&verifyopts.log, "log", "info", "log level")
	f.Parse(args)

	verifyopts.args = f.Args()

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

	verifyopts.vtick = time.Duration(vtick) * time.Millisecond
	verifyopts.stick = time.Duration(stick) * time.Millisecond
	verifyopts.rtick = time.Duration(rtick) * time.Millisecond
}

func doVerify(args []string) {
	parseVerifyopts(args)

	fmt.Printf("Seed: %v\n", verifyopts.seed)

	opch := make(chan [][]interface{}, 10000)
	go generate(verifyopts.repeat, "./llrb.prod", opch)

	if verifyopts.mvcc > 0 {
		go validateTick(verifyopts.vtick, opch)
		go snapshotTick(verifyopts.stick, opch)
		//go releaseTick(verifyopts.rtick, opch)
		readers := make([]chan llrbcmd, 0)
		for i := 0; i < verifyopts.mvcc; i++ {
			ropch := make(chan llrbcmd, 10000)
			readers = append(readers, ropch)
			go mvccreader(i, ropch)
		}
		verifyLLRBMvcc(uint64(verifyopts.repeat), opch, readers)
	} else {
		go validateTick(verifyopts.vtick, opch)
		verifyLLRB(uint64(verifyopts.repeat), opch)
	}
}

func verifyLLRB(count uint64, opch chan [][]interface{}) {
	opstats := newopstats()

	vbno, vbuuid, seqno := uint16(10), uint64(1234), uint64(0)

	// dict
	dict := storage.NewDict()
	// llrb
	config := newllrbconfig()
	config["log.level"] = verifyopts.log
	storage.SetLogger(nil, config)
	llrb := storage.NewLLRB("verify", config, nil)

	for seqno < count {
		seqno++
		cmds := <-opch
		for _, cmd := range cmds {
			lcmd := llrbcmd{cmd: cmd, vbno: vbno, vbuuid: vbuuid, seqno: seqno}
			if verifyopts.opdump {
				fmt.Printf("cmd %v\n", lcmd.cmd)
			}
			switch cmd[0].(string) {
			case "get":
				opstats = llrb_opGet(dict, llrb, lcmd, opstats)
			case "min":
				opstats = llrb_opMin(dict, llrb, lcmd, opstats)
			case "max":
				opstats = llrb_opMax(dict, llrb, lcmd, opstats)
			case "range":
				opstats = llrb_opRange(dict, llrb, lcmd, opstats)
			case "delmin":
				opstats = llrb_opDelmin(dict, llrb, lcmd, opstats)
			case "delmax":
				opstats = llrb_opDelmax(dict, llrb, lcmd, opstats)
			case "upsert":
				opstats = llrb_opUpsert(dict, llrb, lcmd, opstats)
			case "delete":
				opstats = llrb_opDelete(dict, llrb, lcmd, opstats)
			case "validate":
				llrb_opValidate(dict, llrb, opstats, false)
			case "snapshot":
				continue
			case "release":
				continue
			default:
				log.Fatalf("unknown command %v\n", cmd)
			}
		}
	}
	llrb_opValidate(dict, llrb, opstats, true)
	llrb.Log(9, true)
}

func verifyLLRBMvcc(
	count uint64, opch chan [][]interface{}, readers []chan llrbcmd) {

	// stats
	opstats := newopstats()
	// dict
	dict := storage.NewDict()

	// llrb
	config := newllrbconfig()
	config["log.level"] = verifyopts.log
	storage.SetLogger(nil, config)
	llrb := storage.NewLLRB("verify", config, nil)

	snapch := make(chan storage.IndexSnapshot, 2)
	makesnaps := func() (dsnap, lsnap storage.IndexSnapshot) {
		err := dict.RSnapshot(snapch)
		if err != nil {
			panic(err)
		}
		dictsnap := <-snapch
		err = llrb.RSnapshot(snapch)
		if err != nil {
			panic(err)
		}
		llrbsnap := <-snapch
		return dictsnap, llrbsnap
	}

	vbno, vbuuid, seqno := uint16(10), uint64(1234), uint64(0)
	dictsnap, llrbsnap := makesnaps()
	snaprespch := make(chan interface{}, 1)
	for _, reader := range readers {
		stats := cloneopstats(opstats)
		lcmd := llrbcmd{
			cmd: []interface{}{"snapshot", dictsnap, llrbsnap, stats, snaprespch},
		}
		reader <- lcmd
		<-snaprespch
	}

	for seqno < count {
		seqno++
		cmds := <-opch
		for _, cmd := range cmds {
			lcmd := llrbcmd{cmd: cmd, vbno: vbno, vbuuid: vbuuid, seqno: seqno}
			if verifyopts.opdump {
				fmt.Printf("cmd %v\n", lcmd.cmd)
			}
			switch cmd[0].(string) {
			case "get":
				for _, reader := range readers {
					reader <- lcmd
				}
			case "min":
				for _, reader := range readers {
					reader <- lcmd
				}
			case "max":
				for _, reader := range readers {
					reader <- lcmd
				}
			case "range":
				for _, reader := range readers {
					reader <- lcmd
				}
			case "delmin":
				opstats = llrb_opDelmin(dict, llrb, lcmd, opstats)
			case "delmax":
				opstats = llrb_opDelmax(dict, llrb, lcmd, opstats)
			case "upsert":
				opstats = llrb_opUpsert(dict, llrb, lcmd, opstats)
			case "delete":
				opstats = llrb_opDelete(dict, llrb, lcmd, opstats)
			case "validate":
				lcmd.cmd = []interface{}{cmd[0], false}
				for _, reader := range readers {
					reader <- lcmd
				}

			case "snapshot":
				dictsnap.Release()
				llrbsnap.Release()
				dictsnap, llrbsnap = makesnaps()

				for _, reader := range readers {
					if rand.Intn(10) < 7 {
						stats := cloneopstats(opstats)
						cmd := []interface{}{
							"snapshot", dictsnap, llrbsnap, stats, snaprespch}
						reader <- llrbcmd{cmd: cmd}
						<-snaprespch
					}
				}

			case "release":
				continue

			default:
				log.Fatalf("unknown command %v\n", cmd)
			}
		}
	}

	lcmd := llrbcmd{cmd: []interface{}{"validate", true}}
	for _, reader := range readers {
		reader <- lcmd
	}

	lcmd = llrbcmd{cmd: []interface{}{"release", 8}}
	for _, reader := range readers {
		reader <- lcmd
	}
	dictsnap.Release()
	llrbsnap.Release()

	dictsnap, llrbsnap = makesnaps()
	llrb_opValidate(dictsnap, llrbsnap, opstats, true)
	dictsnap.Release()
	llrbsnap.Release()
	llrb.Log(9, true)
}

func validateTick(tick time.Duration, opch chan [][]interface{}) {
	tm := time.NewTicker(tick)
	for {
		<-tm.C
		opch <- [][]interface{}{[]interface{}{"validate"}}
	}
}

func snapshotTick(tick time.Duration, opch chan [][]interface{}) {
	tm := time.NewTicker(tick)
	for {
		<-tm.C
		opch <- [][]interface{}{[]interface{}{"snapshot"}}
	}
}

func releaseTick(tick time.Duration, opch chan [][]interface{}) {
	tm := time.NewTicker(tick)
	for {
		<-tm.C
		opch <- [][]interface{}{[]interface{}{"release"}}
	}
}

//--------
// helpers
//--------

var writeOps = map[string]bool{
	"delmin": true,
	"delmax": true,
	"upsert": true,
	"insert": true,
	"delete": true,
}

func isReadOp(cmd []interface{}) bool {
	return !writeOps[cmd[0].(string)]
}

func mf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
