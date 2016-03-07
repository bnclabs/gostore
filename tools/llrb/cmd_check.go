package main

import "fmt"
import "flag"
import "strings"
import "strconv"
import "time"
import "log"

import "github.com/prataprc/storage.go"

var validateopts struct {
	repeat int
	seed   int
	opdump bool
	bagdir string
	mvcc   bool
	// LLRB specific structures
	nodearena [4]int // min,max,cap,pcap
	valarena  [4]int // min,max,cap,pcap
	args      []string
}

func parseValidateopts(args []string) {
	f := flag.NewFlagSet("validate", flag.ExitOnError)

	var nodearena, valarena string

	seed := time.Now().UTC().Second()
	f.IntVar(&validateopts.repeat, "repeat", 1000,
		"number of times to repeat the generator")
	f.IntVar(&validateopts.seed, "seed", seed,
		"seed value for generating inputs")
	f.StringVar(&validateopts.bagdir, "bagdir", "./",
		"bagdir for monster")
	f.BoolVar(&validateopts.mvcc, "mvcc", false,
		"use mvcc llrb")
	f.BoolVar(&validateopts.opdump, "opdump", false,
		"dump monster generated ops")
	f.StringVar(&nodearena, "nodearena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for nodes")
	f.StringVar(&valarena, "valarena", "",
		"minblock,maxblock,arena-capacity,pool-capacity for values")
	f.Parse(args)

	validateopts.args = f.Args()

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
}

func doValidate(args []string) {
	parseValidateopts(args)

	fmt.Printf("Seed: %v\n", validateopts.seed)

	opch := make(chan [][]interface{}, 1000)
	go generate(validateopts.repeat, "./llrb.prod", opch)

	if validateopts.mvcc {
		//withLLRBMvcc(validateopts.repeat, opch)
	} else {
		withLLRB(uint64(validateopts.repeat), opch)
	}
}

func withLLRB(count uint64, opch chan [][]interface{}) {
	genstats := newgenstats()

	vbno, vbuuid, seqno := uint16(10), uint64(1234), uint64(1)

	// dict
	dict := storage.NewDict()

	// llrb
	config := newllrbconfig()
	llrb := storage.NewLLRB("validate", config, nil)

loop:
	for seqno < count {
		cmds, ok := <-opch
		if !ok {
			break loop
		}
		for _, cmd := range cmds {
			lcmd := llrbcmd{cmd: cmd, vbno: vbno, vbuuid: vbuuid, seqno: seqno}
			if validateopts.opdump {
				fmt.Printf("cmd %v\n", lcmd.cmd)
			}
			switch cmd[0].(string) {
			case "get":
				genstats = llrb_opGet(dict, llrb, lcmd, genstats)
			case "min":
				genstats = llrb_opMin(dict, llrb, lcmd, genstats)
			case "max":
				genstats = llrb_opMax(dict, llrb, lcmd, genstats)
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
	llrb.Log(9)
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
