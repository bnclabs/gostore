package main

import "fmt"
import "bytes"
import "reflect"

import "github.com/prataprc/storage.go"

type llrbcmd struct {
	cmd    []interface{}
	vbno   uint16
	vbuuid uint64
	seqno  uint64
	keys   [][]byte
	values [][]byte
}

func (lcmd *llrbcmd) String() string {
	fmsg := "cmd:%v, vbno:%v, vbuuid:%v, seqno:%v"
	return fmt.Sprintf(fmsg, lcmd.cmd, lcmd.vbno, lcmd.vbuuid, lcmd.seqno)
}

func llrb_opGet(
	dictrd storage.IndexReader, llrbrd storage.IndexReader,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	if dictrd == nil {
		panic("dictrd reader is nil")
	} else if llrbrd == nil {
		panic("llrbrd reader is nil")
	}

	key := lcmd.keys[int(lcmd.cmd[1].(float64))]

	nd := llrbrd.Get(key)
	cmpllrbdict(llrbrd.(storage.IndexSnapshot).Id(), dictrd.Get(key), nd, true)

	stats["total"] += 1
	if nd != nil {
		stats["get.ok"] += 1
	} else {
		stats["get.na"] += 1
	}
	return stats
}

func llrb_opMin(
	dictrd storage.IndexReader, llrbrd storage.IndexReader,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	if dictrd == nil {
		panic("dictrd reader is nil")
	} else if llrbrd == nil {
		panic("llrbrd reader is nil")
	}

	nd := llrbrd.Min()
	cmpllrbdict(llrbrd.(storage.IndexSnapshot).Id(), dictrd.Min(), nd, true)

	stats["total"] += 1
	if nd != nil {
		stats["min.ok"] += 1
	} else {
		stats["min.na"] += 1
	}
	return stats
}

func llrb_opMax(
	dictrd storage.IndexSnapshot, llrbrd storage.IndexSnapshot,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	if dictrd == nil {
		panic("dictrd reader is nil")
	} else if llrbrd == nil {
		panic("llrbrd reader is nil")
	}

	nd := llrbrd.Max()
	cmpllrbdict(llrbrd.(storage.IndexSnapshot).Id(), dictrd.Max(), nd, true)

	stats["total"] += 1
	if nd != nil {
		stats["max.ok"] += 1
	} else {
		stats["max.na"] += 1
	}
	return stats
}

func llrb_opRange(
	dictrd storage.IndexReader, llrbrd storage.IndexReader,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	if dictrd == nil {
		panic("dictrd reader is nil")
	} else if llrbrd == nil {
		panic("llrbrd reader is nil")
	}

	dnodes := make([]storage.Node, 0)
	lnodes := make([]storage.Node, 0)
	lowkey := lcmd.keys[int(lcmd.cmd[1].(float64))]
	highkey := lcmd.keys[int(lcmd.cmd[2].(float64))]
	incl := lcmd.cmd[3].(string)

	dictrd.Range(lowkey, highkey, incl, func(nd storage.Node) bool {
		dnodes = append(dnodes, nd)
		return true
	})
	llrbrd.Range(lowkey, highkey, incl, func(nd storage.Node) bool {
		lnodes = append(lnodes, nd)
		return true
	})

	for i, dictnd := range dnodes {
		cmpllrbdict(llrbrd.(storage.IndexSnapshot).Id(), dictnd, lnodes[i], true)
	}

	stats["total"] += 1
	stats["range.ok"] += 1
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
				cmpllrbdict(llrb.Id(), dictnd, nd, true)
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
				cmpllrbdict(llrb.Id(), dictnd, nd, true)
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

	key := lcmd.keys[int(lcmd.cmd[1].(float64))]
	value := lcmd.values[int(lcmd.cmd[2].(float64))]

	dict.Upsert(
		key, value,
		func(_ storage.Index, i int64, dnew, dold storage.Node) {
			llrb.Upsert(
				key, value,
				func(_ storage.Index, j int64, lnew, lold storage.Node) {
					cmpllrbdict(llrb.Id(), dold, lold, true)
					if lold == nil {
						stats["insert"] += 1
					} else {
						stats["upsert"] += 1
					}
					lnew.Setvbno(lcmd.vbno)
					lnew.SetVbuuid(lcmd.vbuuid)
					lnew.SetBornseqno(lcmd.seqno + uint64(i))
				})
			dnew.Setvbno(lcmd.vbno).SetVbuuid(lcmd.vbuuid)
			dnew.SetBornseqno(lcmd.seqno + uint64(i))
		})

	stats["total"] += 1
	return stats
}

func llrb_opDelete(
	dict *storage.Dict, llrb *storage.LLRB,
	lcmd llrbcmd, stats map[string]int) map[string]int {

	key := lcmd.keys[int(lcmd.cmd[1].(float64))]

	var refnd storage.Node
	dict.Delete(key, func(_ storage.Index, ddel storage.Node) {
		llrb.Delete(key, func(_ storage.Index, ldel storage.Node) {
			cmpllrbdict(llrb.Id(), ddel, ldel, true)
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
	dict, llrb storage.IndexSnapshot, stats map[string]int,
	dolog, mvcc bool) {

	llrb_validateEqual(dict, llrb, dolog)
	llrb_validateStats(dict, llrb, stats, dolog, mvcc)
	llrb.Validate()
	stats["total"] += 1
	stats["validate"] += 1
}

func llrb_validateEqual(dict, llrb storage.IndexSnapshot, dolog bool) bool {
	dictn, llrbn := dict.Count(), llrb.Count()
	if dictn != llrbn {
		fmsg := "%v count expected dict:%v, got llrb:%v"
		err := fmt.Errorf(fmsg, llrb.Id(), dictn, llrbn)
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
		fmt.Println(refkeys)
		fmt.Println(keys)
		fmsg := "%v final Dict keys and LLRB keys mismatch\n"
		panic(fmt.Errorf(fmsg, llrb.Id()))
	} else if reflect.DeepEqual(refvals, vals) == false {
		fmsg := "%v final Dict values and LLRB values mismatch\n"
		panic(fmt.Errorf(fmsg, llrb.Id()))
	}
	if dolog {
		fmt.Printf("%v validateEqual: ok\n", llrb.Id())
		fmt.Printf("  number of elements {dict: %v, llrb:%v}\n", dictn, llrbn)
	}
	return true
}

func llrb_validateStats(
	dict, llrb storage.IndexSnapshot, stats map[string]int,
	dolog, mvcc bool) bool {

	insert, upsert := stats["insert"], stats["upsert"]
	validates := stats["validate"]
	ranges := stats["range.ok"]

	dels := [2]int{stats["delete.ok"], stats["delete.na"]}
	dmax := [2]int{stats["delmax.ok"], stats["delmax.na"]}
	dmin := [2]int{stats["delmin.ok"], stats["delmin.na"]}
	gets := [2]int{stats["get.ok"], stats["get.na"]}
	maxs := [2]int{stats["max.ok"], stats["max.na"]}
	mins := [2]int{stats["min.ok"], stats["min.na"]}
	total := insert + upsert + dels[0] + dels[1]
	total += dmax[0] + dmax[1] + dmin[0] + dmin[1]
	total += gets[0] + gets[1] + maxs[0] + maxs[1] + mins[0] + mins[1] +
		validates + ranges

	checknotzeros := func(notzeros []string) {
		for _, key := range notzeros {
			if stats[key] == 0 {
				panic(fmt.Errorf("%v no %v", llrb.Id(), key))
			}
		}
	}

	if mvcc == false {
		checknotzeros([]string{
			"insert", "upsert", "delete.ok", "delmax.ok",
			"delmin.ok",
		})
	}

	if total != stats["total"] {
		fmsg := "%v total expected %v, got %v"
		panic(fmt.Errorf(fmsg, llrb.Id(), total, stats["total"]))
	}
	dictn, cnt := dict.Count(), int64(insert-(dels[0]+dmin[0]+dmax[0]))
	if dictn != cnt {
		fmsg := "%v expected counts: %v, stats: %v\n"
		panic(fmt.Errorf(fmsg, llrb.Id(), dictn, cnt))
		return false
	}

	if dolog {
		fmt.Printf("%v validateStats:  ok\n", llrb.Id())
		fmt.Printf("  total operations : %v\n", total)
		fmt.Printf("  inserts/upserts/range : {%v,%v,%v}\n", insert, upsert, ranges)
		fmsg := "  ds/dn/dx {ok/na} : {%v,%v} {%v,%v} {%v,%v}\n"
		fmt.Printf(fmsg, dels[0], dels[1], dmax[0], dmax[1], dmin[0], dmin[1])
		fmsg = "  gt/mn/mx {ok/na} : {%v,%v} {%v,%v} {%v,%v}\n"
		fmt.Printf(fmsg, gets[0], gets[1], mins[0], mins[1], maxs[0], maxs[1])
		fmt.Printf("  validates        : %v\n", validates)
	}
	return true
}

func cmpllrbdict(id string, dictnd, llrbnd storage.Node, fail bool) {
	fmsg := "ERROR %v expected %v, got %v\n"
	if llrbnd == nil && dictnd == nil {
		return
	} else if llrbnd != nil && dictnd == nil {
		panic(fmt.Errorf(fmsg, id, nil, llrbnd))
	} else if llrbnd == nil && dictnd != nil {
		panic(fmt.Errorf("%v unexpected nil %v\n", id, dictnd))
	}

	if x, y := llrbnd.Key(), dictnd.Key(); bytes.Compare(x, y) != 0 {
		if fail {
			panic(fmt.Errorf(fmsg, id, string(y), string(x)))
		} else {
			panic(fmt.Errorf(fmsg, id, string(y), string(x)))
		}
	} else if x, y = llrbnd.Value(), dictnd.Value(); bytes.Compare(x, y) != 0 {
		if fail {
			panic(fmt.Errorf(fmsg, id, string(y), string(x)))
		} else {
			panic(fmt.Errorf(fmsg, id, string(y), string(x)))
		}
	} else if x, y := llrbnd.Vbno(), dictnd.Vbno(); x != y {
		if fail {
			panic(fmt.Errorf(fmsg, id, y, x))
		} else {
			panic(fmt.Errorf(fmsg, id, y, x))
		}
	} else if x, y := llrbnd.Vbuuid(), dictnd.Vbuuid(); x != y {
		if fail {
			panic(fmt.Errorf(fmsg, id, y, x))
		} else {
			panic(fmt.Errorf(fmsg, id, y, x))
		}
	} else if x, y := llrbnd.Bornseqno(), dictnd.Bornseqno(); x != y {
		if fail {
			panic(fmt.Errorf(fmsg, id, y, x))
		} else {
			panic(fmt.Errorf(fmsg, id, y, x))
		}
	}
	//TODO: enable this only if mem_pool.initnode() is enabled.
	//if dsq := llrbnd.Deadseqno(); dsq != 0 && dsq != 0xFFFFFFFFFFFFFFFF {
	//	panic(fmt.Errorf("%v unexpected deadseqno %v", id, dsq))
	//}
}
