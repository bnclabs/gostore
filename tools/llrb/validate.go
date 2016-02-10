package main

import "fmt"
import "flag"
import "strings"
import "strconv"
import "time"
import "log"
import "reflect"
import "io/ioutil"
import "encoding/json"

import "github.com/prataprc/storage.go"
import "github.com/prataprc/goparsec"
import "github.com/prataprc/monster"
import mcommon "github.com/prataprc/monster/common"

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

	withLLRB(validateopts.repeat, opch)
}

func withLLRB(count int, opch chan [][]interface{}) {
	dict := storage.NewDict()
	config := map[string]interface{}{
		"maxvb":                   1024,
		"mvcc.enabled":            false,
		"mvcc.snapshot.tick":      0,
		"mvcc.writer.chanbuffer":  1000,
		"nodearena.minblock":      loadopts.nodearena[0],
		"nodearena.maxblock":      loadopts.nodearena[1],
		"nodearena.capacity":      loadopts.nodearena[2],
		"nodearena.pool.capacity": loadopts.nodearena[3],
		"valarena.minblock":       loadopts.valarena[0],
		"valarena.maxblock":       loadopts.valarena[1],
		"valarena.capacity":       loadopts.valarena[2],
		"valarena.pool.capacity":  loadopts.valarena[3],
		"metadata.mvalue":         true,
	}
	llrb := storage.NewLLRB("validate", config, nil)
	stats := map[string]int{
		"total":     0,
		"get.ok":    0,
		"get.na":    0,
		"min.ok":    0,
		"min.na":    0,
		"max.ok":    0,
		"max.na":    0,
		"delmin.ok": 0,
		"delmin.na": 0,
		"delmax.ok": 0,
		"delmax.na": 0,
		"upsert":    0,
		"insert":    0,
		"delete.ok": 0,
		"delete.na": 0,
		"validate":  0,
	}
	for count > 0 {
		count--
		cmds := <-opch
		for _, cmd := range cmds {
			switch cmd[0].(string) {
			case "get":
				stats = opGet(dict, llrb, cmd, stats)
			case "min":
				stats = opMin(dict, llrb, cmd, stats)
			case "max":
				stats = opMax(dict, llrb, cmd, stats)
			case "delmin":
				stats = opDelmin(dict, llrb, cmd, stats)
			case "delmax":
				stats = opDelmax(dict, llrb, cmd, stats)
			case "upsert":
				stats = opUpsert(dict, llrb, cmd, stats)
			case "delete":
				stats = opDelete(dict, llrb, cmd, stats)
			case "validate":
				opValidate(dict, llrb, stats, false)
				if validateopts.opdump {
					fmt.Printf("%v\n", cmd)
				}
			default:
				log.Fatalf("unknown command %v\n", cmd)
			}
		}
	}
	opValidate(dict, llrb, stats, true)
	llrb.LogNodememory()
	llrb.LogNodeutilz()
	llrb.LogValuememory()
	llrb.LogValueutilz()
	llrb.LogUpsertdepth()
	llrb.LogTreeheight()
}

//--------
// monster
//--------

func generate(repeat int, prodfile string, opch chan<- [][]interface{}) {
	text, err := ioutil.ReadFile(prodfile)
	if err != nil {
		log.Fatal(err)
	}
	root := compile(parsec.NewScanner(text)).(mcommon.Scope)
	seed, bagdir, prodfile := uint64(validateopts.seed), validateopts.bagdir, prodfile
	scope := monster.BuildContext(root, seed, bagdir, prodfile)
	nterms := scope["_nonterminals"].(mcommon.NTForms)
	for i := 0; i < repeat; i++ {
		scope = scope.RebuildContext()
		val := evaluate("root", scope, nterms["s"])
		var arr [][]interface{}
		if err := json.Unmarshal([]byte(val.(string)), &arr); err != nil {
			log.Fatal(err)
		}
		opch <- arr
	}
}

func compile(s parsec.Scanner) parsec.ParsecNode {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("%v at %v", r, s.GetCursor())
		}
	}()
	root, _ := monster.Y(s)
	return root
}

func evaluate(
	name string, scope mcommon.Scope, forms []*mcommon.Form) interface{} {

	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("%v", r)
		}
	}()
	return monster.EvalForms(name, scope, forms)
}

//---------
// validate
//---------

func opGet(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	key := []byte(strconv.Itoa(int(cmd[1].(float64))))
	refval, val := dict.Get(key), llrb.Get(key).Value()
	if reflect.DeepEqual(refval, val) == false {
		fmsg := "get: expected %v, got %v\n"
		log.Fatalf(fmsg, string(refval), string(val))
	}
	if validateopts.opdump {
		fmt.Printf("%v | dict:%v llrb:%v\n", cmd, string(refval), string(val))
	}
	stats["total"] += 1
	if refval != nil {
		stats["get.ok"] += 1
	} else {
		stats["get.na"] += 1
	}
	return stats
}

func opMin(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	refkey, refval := dict.Min()
	nd := llrb.Min()
	rkey, rval := nd.Key(), nd.Value()
	if reflect.DeepEqual(refkey, rkey) == false {
		fmsg := "min: key expected %v, got %v\n"
		log.Fatalf(fmsg, string(refkey), string(rkey))
	} else if reflect.DeepEqual(refval, rval) == false {
		fmsg := "min: value expected %v, got %v\n"
		log.Fatalf(fmsg, string(refval), string(rval))
	}
	if validateopts.opdump {
		fmsg := "%v | dict:{%v,%v} llrb:{%v,%v}\n"
		fmt.Printf(
			fmsg, cmd, string(refkey), string(refval), string(rkey),
			string(rval),
		)
	}
	stats["total"] += 1
	if refkey != nil {
		stats["min.ok"] += 1
	} else {
		stats["min.na"] += 1
	}
	return stats
}

func opMax(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	refkey, refval := dict.Max()
	nd := llrb.Max()
	rkey, rval := nd.Key(), nd.Value()
	if reflect.DeepEqual(refkey, rkey) == false {
		fmsg := "max: key expected %v, got %v\n"
		log.Fatalf(fmsg, string(refkey), string(rkey))
	} else if reflect.DeepEqual(refval, rval) == false {
		fmsg := "max: value expected %v, got %v\n"
		log.Fatalf(fmsg, string(refval), string(rval))
	}
	if validateopts.opdump {
		fmsg := "%v | dict:{%v,%v} llrb:{%v,%v}\n"
		fmt.Printf(
			fmsg, cmd, string(refkey), string(refval), string(rkey),
			string(rval),
		)
	}
	stats["total"] += 1
	if refkey != nil {
		stats["max.ok"] += 1
	} else {
		stats["max.na"] += 1
	}
	return stats
}

func opDelmin(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	refkey, refval := dict.DeleteMin()
	llrb.DeleteMin(
		func(llrb *storage.LLRB, nd *storage.Llrbnode) {
			rkey, rval := nd.Key(), nd.Value()
			if reflect.DeepEqual(refkey, rkey) == false {
				fmsg := "delmin: key expected %v, got %v\n"
				log.Fatalf(fmsg, string(refkey), string(rkey))
			} else if reflect.DeepEqual(refval, rval) == false {
				fmsg := "delmin: value expected %v, got %v\n"
				log.Fatalf(fmsg, string(refval), string(rval))
			}
			if validateopts.opdump {
				fmsg := "%v | dict:{%v,%v} llrb:{%v,%v}\n"
				fmt.Printf(
					fmsg, cmd, string(refkey), string(refval),
					string(rkey), string(rval),
				)
			}
		})
	stats["total"] += 1
	if refkey != nil {
		stats["delmin.ok"] += 1
	} else {
		stats["delmin.na"] += 1
	}
	return stats
}

func opDelmax(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	refkey, refval := dict.DeleteMax()
	llrb.DeleteMax(
		func(llrb *storage.LLRB, nd *storage.Llrbnode) {
			rkey, rval := nd.Key(), nd.Value()
			if reflect.DeepEqual(refkey, rkey) == false {
				fmsg := "delmax: key expected %v, got %v\n"
				log.Fatalf(fmsg, string(refkey), string(rkey))
			} else if reflect.DeepEqual(refval, rval) == false {
				fmsg := "delmax: value expected %v, got %v\n"
				log.Fatalf(fmsg, string(refval), string(rval))
			}
			if validateopts.opdump {
				fmsg := "%v | dict:{%v,%v} llrb:{%v,%v}\n"
				fmt.Printf(
					fmsg, cmd, string(refkey), string(refval),
					string(rkey), string(rval),
				)
			}
		})
	stats["total"] += 1
	if refkey != nil {
		stats["delmax.ok"] += 1
	} else {
		stats["delmax.na"] += 1
	}
	return stats
}

func opUpsert(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	key := []byte(strconv.Itoa(int(cmd[1].(float64))))
	value := []byte(strconv.Itoa(int(time.Now().UnixNano())))
	refval, val := dict.Get(key), llrb.Get(key).Value()
	if (refval == nil && val != nil) || (refval != nil && val == nil) {
		fmsg := "upsert-get: expected %v, got %v\n"
		log.Fatalf(fmsg, string(refval), string(val))
	} else if refval != nil && val != nil {
		if reflect.DeepEqual(refval, val) == false {
			fmsg := "upsert-get: expected %v, got %v\n"
			log.Fatalf(fmsg, string(refval), string(val))
		}
		refval = dict.Upsert(key, value)
		llrb.Upsert(
			key, value,
			func(llrb *storage.LLRB, newnd, oldnd *storage.Llrbnode) {
				if reflect.DeepEqual(refval, oldnd.Value()) == false {
					fmsg := "upsert: expected %v, got %v\n"
					log.Fatalf(fmsg, string(refval), string(oldnd.Value()))
				} else if reflect.DeepEqual(newnd.Key(), key) == false {
					fmsg := "upsert: key expected %v, got %v\n"
					log.Fatalf(fmsg, string(key), string(newnd.Key()))
				} else if reflect.DeepEqual(newnd.Value(), value) == false {
					fmsg := "upsert: value expected %v, got %v\n"
					log.Fatalf(fmsg, string(value), string(newnd.Value()))
				}
				if validateopts.opdump {
					fmsg := "%v | {%v,%v} dict:%v llrb:%v\n"
					fmt.Printf(
						fmsg, cmd, string(key), string(value), string(refval),
						string(oldnd.Value()),
					)
				}
			})
		stats["upsert"] += 1
	} else {
		refval = dict.Upsert(key, value)
		if refval != nil {
			fmsg := "insert: dict old value expected nil, got %v\n"
			log.Fatalf(fmsg, string(refval))
		}
		llrb.Upsert(
			key, value,
			func(llrb *storage.LLRB, newnd, oldnd *storage.Llrbnode) {
				if oldnd != nil {
					fmsg := "insert: llrb old value expected nil, got {%v,%v}\n"
					log.Fatalf(fmsg, string(oldnd.Key()), string(oldnd.Value()))
				} else if reflect.DeepEqual(newnd.Key(), key) == false {
					fmsg := "insert: key expected %v, got %v\n"
					log.Fatalf(fmsg, string(key), string(newnd.Key()))
				} else if reflect.DeepEqual(newnd.Value(), value) == false {
					fmsg := "insert: value expected %v, got %v\n"
					log.Fatalf(fmsg, string(value), string(newnd.Value()))
				}
				if validateopts.opdump {
					fmsg := "%v i | {%v,%v}\n"
					fmt.Printf(fmsg, cmd, string(key), string(value))
				}
			})
		stats["insert"] += 1
	}
	stats["total"] += 1
	return stats
}

func opDelete(
	dict *storage.Dict, llrb *storage.LLRB,
	cmd []interface{}, stats map[string]int) map[string]int {

	key := []byte(strconv.Itoa(int(cmd[1].(float64))))
	refval := dict.Delete(key)
	llrb.Delete(
		key,
		func(llrb *storage.LLRB, nd *storage.Llrbnode) {
			val := nd.Value()
			if (refval == nil && val != nil) || (refval != nil && val == nil) {
				fmsg := "delete: mismatch with dict expected %v, got %v\n"
				log.Fatalf(fmsg, string(refval), string(val))
			} else if reflect.DeepEqual(refval, val) == false {
				fmsg := "delete: expected %v, got %v\n"
				log.Fatalf(fmsg, string(refval), string(val))
			}
			if validateopts.opdump {
				fmsg := "%v | dict:%v llrb:%v\n"
				fmt.Printf(fmsg, cmd, string(refval), string(val))
			}
		})
	stats["total"] += 1
	if refval != nil {
		stats["delete.ok"] += 1
	} else {
		stats["delete.na"] += 1
	}
	return stats
}

func opValidate(
	dict *storage.Dict, llrb *storage.LLRB, stats map[string]int, dolog bool) {

	validateEqual(dict, llrb, dolog)
	validateStats(dict, llrb, stats, dolog)
	ok := llrb.ValidateReds()
	if dolog || !ok {
		fmt.Printf("ValidateReds:   %v\n", ok)
	}
	count := llrb.ValidateBlacks()
	if dolog || !ok {
		fmt.Printf("ValidateBlacks: %v\n", count)
	}
	stats["total"] += 1
	stats["validate"] += 1
}

func validateEqual(dict *storage.Dict, llrb *storage.LLRB, dolog bool) bool {
	dictn, llrbn := dict.Count(), llrb.Count()
	if dictn != llrbn {
		log.Fatalf("count expected dict:%v, got llrb:%v", dictn, llrbn)
		return false
	}

	refkeys, refvals := make([][]byte, 0), make([][]byte, 0)
	dict.Range(nil, nil, "both", func(k, v []byte) bool {
		refkeys, refvals = append(refkeys, k), append(refvals, v)
		return true
	})
	keys, vals := make([][]byte, 0), make([][]byte, 0)
	llrb.Range(nil, nil, "both", func(nd *storage.Llrbnode) bool {
		keys, vals = append(keys, nd.Key()), append(vals, nd.Value())
		return true
	})
	if reflect.DeepEqual(refkeys, keys) == false {
		log.Fatalf("final Dict keys and LLRB keys mismatch\n")
		return false
	} else if reflect.DeepEqual(refvals, vals) == false {
		log.Fatalf("final Dict values and LLRB values mismatch\n")
		return false
	}
	if dolog {
		fmt.Printf("validateEqual: ok\n")
		fmt.Printf("  number of elements {dict: %v, llrb:%v}\n", dictn, llrbn)
	}
	return true
}

func validateStats(
	dict *storage.Dict, llrb *storage.LLRB,
	stats map[string]int, dolog bool) bool {

	insert, upsert := stats["insert"], stats["upsert"]
	validates := stats["validate"]

	dels := [2]int{stats["delete.ok"], stats["delete.na"]}
	dmax := [2]int{stats["delmax.ok"], stats["delmax.na"]}
	dmin := [2]int{stats["delmin.ok"], stats["delmin.na"]}
	gets := [2]int{stats["get.ok"], stats["get.na"]}
	maxs := [2]int{stats["max.ok"], stats["max.na"]}
	mins := [2]int{stats["min.ok"], stats["min.na"]}
	total := insert + upsert + dels[0] + dels[1]
	total += dmax[0] + dmax[1] + dmin[0] + dmin[1]
	total += gets[0] + gets[1] + maxs[0] + maxs[1] + mins[0] + mins[1] +
		validates

	if total != stats["total"] {
		log.Fatalf("total expected %v, got %v", total, stats["total"])
		return false
	}
	dictn, cnt := dict.Count(), int64(insert-(dels[0]+dmin[0]+dmax[0]))
	if dictn != cnt {
		log.Fatalf("expected counts: %v, stats: %v\n", dictn, cnt)
		return false
	}

	if dolog {
		fmt.Printf("validateStats:  ok\n")
		fmt.Printf("  total operations : %v\n", total)
		fmt.Printf("  inserts/upserts  : {%v,%v}\n", insert, upsert)
		fmsg := "  ds/dn/dx {ok/na} : {%v,%v} {%v,%v} {%v,%v}\n"
		fmt.Printf(fmsg, dels[0], dels[1], dmax[0], dmax[1], dmin[0], dmin[1])
		fmsg = "  gt/mn/mx {ok/na} : {%v,%v} {%v,%v} {%v,%v}\n"
		fmt.Printf(fmsg, gets[0], gets[1], mins[0], mins[1], maxs[0], maxs[1])
		fmt.Printf("  validates        : %v\n", validates)
	}
	return true
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
