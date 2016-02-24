package main

import "fmt"
import "flag"
import "strings"
import "strconv"
import "time"
import "log"
import "bytes"

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
		withLLRB(validateopts.repeat, opch)
	}
}

func withLLRB(count int, opch chan [][]interface{}) {
	genstats := newgenstats()

	// dict
	dict := storage.NewDict()

	// llrb
	config := newllrbconfig()
	llrb := storage.NewLLRB("validate", config, nil)

	for count > 0 {
		count--
		cmds := <-opch
		for _, cmd := range cmds {
			switch cmd[0].(string) {
			case "get":
				genstats = llrb_opGet(dict, llrb, cmd, genstats)
			case "min":
				genstats = llrb_opMin(dict, llrb, cmd, genstats)
			case "max":
				genstats = llrb_opMax(dict, llrb, cmd, genstats)
			case "delmin":
				genstats = llrb_opDelmin(dict, llrb, cmd, genstats)
			case "delmax":
				genstats = llrb_opDelmax(dict, llrb, cmd, genstats)
			case "upsert":
				genstats = llrb_opUpsert(dict, llrb, cmd, genstats)
			case "delete":
				genstats = llrb_opDelete(dict, llrb, cmd, genstats)
			case "validate":
				llrb_opValidate(dict, llrb, genstats, false)
				if validateopts.opdump {
					fmt.Printf("%v\n", cmd)
				}
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

	buffer := bytes.NewBuffer(nil)
	llrb.Logmemory(buffer)
	fmt.Printf("LLRB Memory:\n%v\n", string(buffer.Bytes()))

	llrb.LogUpsertdepth()
	llrb.LogTreeheight()
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
