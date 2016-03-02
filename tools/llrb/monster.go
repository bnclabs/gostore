package main

import "encoding/json"
import "io/ioutil"
import "log"
import "flag"
import "sort"
import "fmt"

import "github.com/prataprc/goparsec"
import "github.com/prataprc/monster"
import mcommon "github.com/prataprc/monster/common"

var monsteropts struct {
	n        int
	ncpu     int
	par      int
	seed     int
	bagdir   string
	prodfile string
	mprof    string
	pprof    string
}

func parseMonsteropts(args []string) {
	f := flag.NewFlagSet("monster", flag.ExitOnError)

	f.IntVar(&monsteropts.n, "n", 1000,
		"number of items to generate and insert")
	f.IntVar(&monsteropts.ncpu, "ncpu", 1,
		"set number cores to use.")
	f.IntVar(&monsteropts.par, "par", 1,
		"no. of concurrent generators")
	f.IntVar(&monsteropts.seed, "seed", 1,
		"random seed")
	f.StringVar(&monsteropts.bagdir, "bagdir", "",
		"bag directory for monster sample data.")
	f.StringVar(&monsteropts.prodfile, "prodfile", "",
		"monster production file")
	f.StringVar(&monsteropts.mprof, "mprof", "",
		"dump mem-profile to file")
	f.StringVar(&monsteropts.pprof, "pprof", "",
		"dump cpu-profile to file")
	f.Parse(args)

	if monsteropts.prodfile == "" {
		log.Fatalf("please provide production file to monster")
	}

	fmt.Printf("seed: %v\n", monsteropts.seed)
	setCPU(monsteropts.ncpu)
}

func doMonster(args []string) {
	parseMonsteropts(args)

	opch := make(chan [][]interface{}, 100000)
	for i := 0; i < monsteropts.par; i++ {
		fmt.Printf("starting monster routine %v ...\n", i+1)
		go generate(monsteropts.n, monsteropts.prodfile, opch)
	}

	stats := make(map[string]int)

	count := 0
	for cmds := range opch {
		for _, cmd := range cmds {
			name := cmd[0].(string)
			count, ok := stats[name]
			if !ok {
				count = 0
			}
			stats[name] = count + 1
		}
		count++
		if count >= (monsteropts.par * monsteropts.n) {
			break
		}
	}
	// print statistics
	keys, total := []string{}, 0
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		total += stats[key]
		fmt.Printf("%v : %v\n", key, stats[key])
	}
	fmt.Printf("total : %v\n", total)
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
	seed, bagdir := uint64(monsteropts.seed), monsteropts.bagdir
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
