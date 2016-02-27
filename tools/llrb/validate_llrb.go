package main

import "strconv"
import "fmt"
import "bytes"
import "reflect"
import "time"

import "github.com/prataprc/storage.go"

type llrbcmd struct {
	cmd    []interface{}
	vbno   uint16
	vbuuid uint64
	seqno  uint64
}

//---------
// validate llrb without mvcc
//---------

func llrb_opGet(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	key := []byte(strconv.Itoa(int(lcmd.cmd[1].(float64))))

	nd := llrb.Get(key)
	cmpllrbdict(dict.Get(key), nd, true)

	stats["total"] += 1
	if nd != nil {
		stats["get.ok"] += 1
	} else {
		stats["get.na"] += 1
	}
	return stats
}

func llrb_opMin(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	nd := llrb.Min()
	cmpllrbdict(dict.Min(), nd, true)

	stats["total"] += 1
	if nd.Key() != nil {
		stats["min.ok"] += 1
	} else {
		stats["min.na"] += 1
	}
	return stats
}

func llrb_opMax(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	nd := llrb.Max()
	cmpllrbdict(dict.Max(), nd, true)

	stats["total"] += 1
	if nd != nil {
		stats["max.ok"] += 1
	} else {
		stats["max.na"] += 1
	}
	return stats
}

func llrb_opDelmin(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	var refnd storage.Node

	dict.DeleteMin(func(_ storage.Index, dictnd storage.Node) {
		llrb.DeleteMin(
			func(_ storage.Index, nd storage.Node) {
				refnd = nd
				cmpllrbdict(dictnd, nd, true)
				if nd != nil {
					nd.SetDeadseqno(lcmd.seqno)
				}
			})
		if dictnd != nil {
			dictnd.SetDeadseqno(lcmd.seqno)
		}
	})

	stats["total"] += 1
	if refnd != nil {
		stats["delmin.ok"] += 1
	} else {
		stats["delmin.na"] += 1
	}
	return stats
}

func llrb_opDelmax(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	var refnd storage.Node

	dict.DeleteMax(func(_ storage.Index, dictnd storage.Node) {
		llrb.DeleteMax(
			func(index storage.Index, nd storage.Node) {
				cmpllrbdict(dictnd, nd, true)
				refnd = nd
				if nd != nil {
					nd.SetDeadseqno(lcmd.seqno)
				}
			})
		if dictnd != nil {
			dictnd.SetDeadseqno(lcmd.seqno)
		}
	})

	stats["total"] += 1
	if refnd != nil {
		stats["delmax.ok"] += 1
	} else {
		stats["delmax.na"] += 1
	}
	return stats
}

func llrb_opUpsert(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	key := []byte(strconv.Itoa(int(lcmd.cmd[1].(float64))))
	value := []byte(strconv.Itoa(int(time.Now().UnixNano())))

	cmpllrbdict(dict.Get(key), llrb.Get(key), true)

	dict.Upsert(key, value, func(_ storage.Index, dnew, dold storage.Node) {
		llrb.Upsert(
			key, value, func(_ storage.Index, lnew, lold storage.Node) {
				cmpllrbdict(dold, lold, true)
				if lold == nil {
					stats["insert"] += 1
				} else {
					stats["upsert"] += 1
				}
				lnew.Setvbno(lcmd.vbno)
				lnew.SetVbuuid(lcmd.vbuuid).SetBornseqno(lcmd.seqno)
			})
		dnew.Setvbno(lcmd.vbno).SetVbuuid(lcmd.vbuuid).SetBornseqno(lcmd.seqno)
	})

	stats["total"] += 1
	return stats
}

func llrb_opDelete(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	key := []byte(strconv.Itoa(int(lcmd.cmd[1].(float64))))

	var refnd storage.Node
	dict.Delete(key, func(_ storage.Index, ddel storage.Node) {
		llrb.Delete(key, func(_ storage.Index, ldel storage.Node) {
			cmpllrbdict(ddel, ldel, true)
			refnd = ldel
			if ldel != nil {
				ldel.SetDeadseqno(lcmd.seqno)
			}
		})
		if ddel != nil {
			ddel.SetDeadseqno(lcmd.seqno)
		}
	})

	stats["total"] += 1
	if refnd != nil {
		stats["delete.ok"] += 1
	} else {
		stats["delete.na"] += 1
	}
	return stats
}

func llrb_opValidate(
	dict *storage.Dict, llrb *storage.LLRB, stats map[string]int, dolog bool) {

	llrb_validateEqual(dict, llrb, dolog)
	llrb_validateStats(dict, llrb, stats, dolog)
	llrb.Validate()
	stats["total"] += 1
	stats["validate"] += 1
}

func llrb_validateEqual(dict *storage.Dict, llrb *storage.LLRB, dolog bool) bool {
	dictn, llrbn := dict.Count(), llrb.Count()
	if dictn != llrbn {
		err := fmt.Errorf("count expected dict:%v, got llrb:%v", dictn, llrbn)
		panic(err)
	}

	refkeys, refvals := make([][]byte, 0), make([][]byte, 0)
	dict.Range(nil, nil, "both", func(nd storage.Node) bool {
		refkeys, refvals = append(refkeys, nd.Key()), append(refvals, nd.Value())
		return true
	})
	keys, vals := make([][]byte, 0), make([][]byte, 0)
	llrb.Range(nil, nil, "both", func(nd storage.Node) bool {
		keys, vals = append(keys, nd.Key()), append(vals, nd.Value())
		return true
	})
	if reflect.DeepEqual(refkeys, keys) == false {
		panic(fmt.Errorf("final Dict keys and LLRB keys mismatch\n"))
	} else if reflect.DeepEqual(refvals, vals) == false {
		panic(fmt.Errorf("final Dict values and LLRB values mismatch\n"))
	}
	if dolog {
		fmt.Printf("validateEqual: ok\n")
		fmt.Printf("  number of elements {dict: %v, llrb:%v}\n", dictn, llrbn)
	}
	return true
}

func llrb_validateStats(
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
		panic(fmt.Errorf("total expected %v, got %v", total, stats["total"]))
	}
	dictn, cnt := dict.Count(), int64(insert-(dels[0]+dmin[0]+dmax[0]))
	if dictn != cnt {
		panic(fmt.Errorf("expected counts: %v, stats: %v\n", dictn, cnt))
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

func cmpllrbdict(dictnd, llrbnd storage.Node, fail bool) {
	if llrbnd == nil && dictnd == nil {
		return
	} else if llrbnd != nil && dictnd == nil {
		panic(fmt.Errorf("expected nil %v %v\n", llrbnd, llrbnd == nil))
	} else if llrbnd == nil && dictnd != nil {
		panic(fmt.Errorf("unexpected nil %v\n", dictnd))
	}

	if x, y := llrbnd.Key(), dictnd.Key(); bytes.Compare(x, y) != 0 {
		if fail {
			panic(fmt.Errorf("expected %v, got %v", string(x), string(y)))
		} else {
			panic(fmt.Errorf("ERROR expected %v, got %v", string(x), string(y)))
		}
	} else if x, y = llrbnd.Value(), dictnd.Value(); bytes.Compare(x, y) != 0 {
		if fail {
			panic(fmt.Errorf("expected %v, got %v", string(x), string(y)))
		} else {
			panic(fmt.Errorf("ERROR expected %v, got %v", string(x), string(y)))
		}
	} else if x, y := llrbnd.Vbno(), dictnd.Vbno(); x != y {
		if fail {
			panic(fmt.Errorf("expected %v, got %v", x, y))
		} else {
			panic(fmt.Errorf("ERROR expected %v, got %v", x, y))
		}
	} else if x, y := llrbnd.Vbuuid(), dictnd.Vbuuid(); x != y {
		if fail {
			panic(fmt.Errorf("expected %v, got %v", x, y))
		} else {
			panic(fmt.Errorf("ERROR expected %v, got %v", x, y))
		}
	} else if x, y := llrbnd.Bornseqno(), dictnd.Bornseqno(); x != y {
		if fail {
			panic(fmt.Errorf("expected %v, got %v", x, y))
		} else {
			panic(fmt.Errorf("ERROR expected %v, got %v", x, y))
		}
	} else if dsq := llrbnd.Deadseqno(); dsq != 0 && dsq != 0xFFFFFFFFFFFFFFFF {
		panic(fmt.Errorf("unexpected deadseqno %v", dsq))
	}
}
