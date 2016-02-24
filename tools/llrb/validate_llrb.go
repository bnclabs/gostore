package main

import "strconv"
import "log"
import "fmt"
import "reflect"
import "time"

import "github.com/prataprc/storage.go"

//---------
// validate llrb without mvcc
//---------

func llrb_opGet(
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

func llrb_opMin(
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

func llrb_opMax(
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

func llrb_opDelmin(
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

func llrb_opDelmax(
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

func llrb_opUpsert(
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

func llrb_opDelete(
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

func llrb_opValidate(
	dict *storage.Dict, llrb *storage.LLRB, stats map[string]int, dolog bool) {

	llrb_validateEqual(dict, llrb, dolog)
	llrb_validateStats(dict, llrb, stats, dolog)
	if ok := llrb.ValidateReds(); dolog {
		fmt.Printf("ValidateReds:   %v\n", ok)
	}
	if count := llrb.ValidateBlacks(); dolog {
		fmt.Printf("ValidateBlacks: %v\n", count)
	}
	if ok := llrb.ValidateHeight(); dolog {
		fmt.Printf("ValidateHeight: %v\n", ok)
	}
	stats["total"] += 1
	stats["validate"] += 1
}

func llrb_validateEqual(dict *storage.Dict, llrb *storage.LLRB, dolog bool) bool {
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
