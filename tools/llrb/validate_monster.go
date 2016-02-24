package main

import "encoding/json"
import "io/ioutil"
import "log"
import "fmt"

import "github.com/prataprc/goparsec"
import "github.com/prataprc/monster"
import mcommon "github.com/prataprc/monster/common"

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
