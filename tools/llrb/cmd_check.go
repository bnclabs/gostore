package main

import "fmt"
import "flag"
import "strings"
import "strconv"
import "time"
import "log"

import "github.com/prataprc/storage.go"

var checkopts struct {
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

func parseCheckopts(args []string) {
	f := flag.NewFlagSet("check", flag.ExitOnError)

	var nodearena, valarena string
	var vtick, stick, rtick int

	seed := time.Now().UTC().Second()
	f.IntVar(&checkopts.repeat, "repeat", 1000,
		"number of times to repeat the generator")
	f.IntVar(&checkopts.seed, "seed", seed,
		"seed value for generating inputs")
	f.IntVar(&vtick, "vtick", 1000,
		"validate tick, in milliseconds")
	f.IntVar(&stick, "stick", 1000,
		"periodically generate snapshot, period in milliseconds")
	f.IntVar(&rtick, "rtick", 1000,
		"periodically release snapshots, period in milliseconds")
	f.StringVar(&checkopts.bagdir, "bagdir", "./",
		"bagdir for monster")
	f.IntVar(&checkopts.mvcc, "mvcc", 0,
		"use as many as n mvcc readers on llrb")
	f.BoolVar(&checkopts.opdump, "opdump", false,
		"dump monster generated ops")
	f.StringVar(&nodearena, "nodearena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for nodes")
	f.StringVar(&valarena, "valarena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for values")
	f.StringVar(&checkopts.log, "log", "info", "log level")
	f.Parse(args)

	checkopts.args = f.Args()

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

	checkopts.vtick = time.Duration(vtick) * time.Millisecond
	checkopts.stick = time.Duration(stick) * time.Millisecond
	checkopts.rtick = time.Duration(rtick) * time.Millisecond
}

func doCheck(args []string) {
	parseCheckopts(args)

	fmt.Printf("Seed: %v\n", checkopts.seed)

	opch := make(chan [][]interface{}, 10000)
	go generate(checkopts.repeat, "./llrb.prod", opch)

	if checkopts.mvcc > 0 {
		go validateTick(checkopts.vtick, opch)
		go snapshotTick(checkopts.stick, opch)
		go releaseTick(checkopts.rtick, opch)
		readers := make([]chan llrbcmd, 0)
		for i := 0; i < checkopts.mvcc; i++ {
			ropch := make(chan llrbcmd, 10000)
			readers = append(readers, ropch)
			go mvccreader(i, ropch)
		}
		checkLLRBMvcc(uint64(checkopts.repeat), opch, readers)
	} else {
		go validateTick(checkopts.vtick, opch)
		checkLLRB(uint64(checkopts.repeat), opch)
	}
}

func checkLLRB(count uint64, opch chan [][]interface{}) {
	genstats := newgenstats()

	vbno, vbuuid, seqno := uint16(10), uint64(1234), uint64(0)

	// dict
	dict := storage.NewDict()

	// llrb
	config := newllrbconfig()
	config["log.level"] = checkopts.log
	llrb := storage.NewLLRB("check", config, nil)

	for seqno < count {
		seqno++
		cmds := <-opch
		for _, cmd := range cmds {
			lcmd := llrbcmd{cmd: cmd, vbno: vbno, vbuuid: vbuuid, seqno: seqno}
			if checkopts.opdump {
				fmt.Printf("cmd %v\n", lcmd.cmd)
			}
			switch cmd[0].(string) {
			case "get":
				genstats = llrb_opGet(dict, llrb, lcmd, genstats)
			case "min":
				genstats = llrb_opMin(dict, llrb, lcmd, genstats)
			case "max":
				genstats = llrb_opMax(dict, llrb, lcmd, genstats)
			case "range":
				genstats = llrb_opRange(dict, llrb, lcmd, genstats)
			case "delmin":
				genstats = llrb_opDelmin(dict, llrb, lcmd, genstats)
			case "delmax":
				genstats = llrb_opDelmax(dict, llrb, lcmd, genstats)
			case "upsert":
				genstats = llrb_opUpsert(dict, llrb, lcmd, genstats)
			case "delete":
				genstats = llrb_opDelete(dict, llrb, lcmd, genstats)
			case "validate":
				llrb_opValidate(dict, llrb, genstats, false)
			case "snapshot":
				continue
			case "release":
				continue
			default:
				log.Fatalf("unknown command %v\n", cmd)
			}
		}
	}
	llrb_opValidate(dict, llrb, genstats, true)
	llrb.Log(9, true)
}

func checkLLRBMvcc(
	count uint64, opch chan [][]interface{}, readers []chan llrbcmd) {

	// stats
	genstats := newgenstats()
	// dict
	dict := storage.NewDict()

	// llrb
	config := newllrbconfig()
	config["log.level"] = checkopts.log
	llrb := storage.NewLLRB("check", config, nil)

	commandreaders := func(cmd llrbcmd, rdrs []chan llrbcmd) {
		for _, reader := range rdrs {
			reader <- cmd
		}
	}

	makesnaps := func() (dsnap, lsnap storage.Snapshot) {
		dictsnap, err := dict.RSnapshot()
		if err != nil {
			panic(err)
		}
		llrbsnap, err := llrb.RSnapshot()
		if err != nil {
			panic(err)
		}
		return dictsnap, llrbsnap
	}

	vbno, vbuuid, seqno := uint16(10), uint64(1234), uint64(0)
	dictsnap, llrbsnap := makesnaps()
	stats := clonestats(genstats)
	lcmd := llrbcmd{cmd: []interface{}{"snapshot", dictsnap, llrbsnap, stats}}
	commandreaders(lcmd, readers)

	for seqno < count {
		seqno++
		cmds := <-opch
		for _, cmd := range cmds {
			lcmd := llrbcmd{cmd: cmd, vbno: vbno, vbuuid: vbuuid, seqno: seqno}
			if checkopts.opdump {
				fmt.Printf("cmd %v\n", lcmd.cmd)
			}
			switch cmd[0].(string) {
			case "get":
				commandreaders(lcmd, readers)
			case "min":
				commandreaders(lcmd, readers)
			case "max":
				commandreaders(lcmd, readers)
			case "range":
				commandreaders(lcmd, readers)
			case "delmin":
				genstats = llrb_opDelmin(dict, llrb, lcmd, genstats)
			case "delmax":
				genstats = llrb_opDelmax(dict, llrb, lcmd, genstats)
			case "upsert":
				genstats = llrb_opUpsert(dict, llrb, lcmd, genstats)
			case "delete":
				genstats = llrb_opDelete(dict, llrb, lcmd, genstats)
			case "validate":
				lcmd.cmd = []interface{}{cmd[0], false}
				commandreaders(lcmd, readers)
			case "snapshot":
				for _, reader := range readers {
					dictsnap, llrbsnap := makesnaps()
					stats := clonestats(genstats)
					cmd := []interface{}{"snapshot", dictsnap, llrbsnap, stats}
					lcmd := llrbcmd{cmd: cmd}
					reader <- lcmd
				}

			case "release":
				commandreaders(lcmd, readers)
			default:
				log.Fatalf("unknown command %v\n", cmd)
			}
		}
	}
	lcmd = llrbcmd{cmd: []interface{}{"validate", true}}
	for _, reader := range readers {
		commandreaders(lcmd, []chan llrbcmd{reader})
		time.Sleep(100 * time.Millisecond)
	}
	dictsnap, llrbsnap = makesnaps()
	llrb_opValidate(dictsnap, llrbsnap, genstats, true)
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
