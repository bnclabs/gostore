// +build ignore

package storage

import "testing"
import "fmt"

import "unsafe"
import "time"
import "math/rand"
import "bytes"

var _ = fmt.Sprintf("dummy")

func TestNewLLRBMvcc(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enabled"] = true

	llrb := NewLLRB("test", config, nil)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}
	nodavail := config["nodearena.capacity"].(int)
	valavail := config["valarena.capacity"].(int)
	mstats := llrb.StatsMem()
	if overhead := mstats["node.overhead"].(int64); overhead != 2112 {
		t.Errorf("expected %v, got %v", 0, 2112)
	} else if useful := mstats["node.useful"].(int64); useful != 0 {
		t.Errorf("expected %v, got %v", 0, useful)
	} else if overhead = mstats["value.overhead"].(int64); overhead != 2112 {
		t.Errorf("expected %v, got %v", 0, overhead)
	} else if useful = mstats["value.useful"].(int64); useful != 0 {
		t.Errorf("expected %v, got %v", 0, overhead)
	} else if x, y := int64(0), mstats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), mstats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(nodavail), mstats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(valavail), mstats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := int64(0), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(0), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

//func TestNewLLRBNode(t *testing.T) {
//	config := makellrbconfig()
//	config["metadata.bornseqno"] = true
//	config["metadata.deadseqno"] = true
//	config["metadata.mvalue"] = true
//	config["metadata.vbuuid"] = true
//	llrb := NewLLRB("test", config, nil)
//	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
//	vbno, vbuuid := uint16(10), uint64(0xABCD)
//	bnseqno, ddseqno := uint64(12345678), uint64(12345690)
//	nd := llrb.newnode(key, value)
//	fmask := metadataMask(0).enableMvalue().enableVbuuid()
//	fmask = fmask.enableBornSeqno().enableDeadSeqno()
//	nd.metadata().initMetadata(vbno, fmask).setvbuuid(vbuuid)
//	nd.metadata().setbnseq(bnseqno).setddseq(ddseqno)
//
//	if k := nd.key(); bytes.Compare(k, key) != 0 {
//		t.Errorf("expected %v, got %v", key, nd.key())
//	} else if v := nd.nodevalue().value(); bytes.Compare(v, value) != 0 {
//		t.Errorf("expected %v, got %v", value, v)
//	} else if x := nd.metadata().vbno(); x != vbno {
//		t.Errorf("expected %v, got %v", vbno, x)
//	} else if x := nd.metadata().vbuuid(); x != vbuuid {
//		t.Errorf("expected %x, got %x", vbuuid, x)
//	} else if x := nd.metadata().bnseq(); x != bnseqno {
//		t.Errorf("expected %x, got %x", bnseqno, x)
//	} else if x := nd.metadata().ddseq(); x != ddseqno {
//		t.Errorf("expected %x, got %x", ddseqno, x)
//	}
//	llrb.freenode(nd)
//}
//
//func TestNewLLRBNodePanic(t *testing.T) {
//	llrb := NewLLRB("test", makellrbconfig(), nil)
//	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
//	func() {
//		defer func() {
//			if recover() == nil {
//				t.Errorf("expected panic")
//			}
//		}()
//		llrb.newnode(key, value)
//	}()
//}
//
//func TestCloneLLRBNode(t *testing.T) {
//	config := makellrbconfig()
//	config["metadata.mvalue"] = true
//	llrb := NewLLRB("test", config, nil)
//	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
//	nd := llrb.newnode(key, value)
//	nd.metadata().setblack().setdirty()
//	nd.left = (*Llrbnode)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd)) +
//		unsafe.Sizeof(*nd)))
//	nd.right = (*Llrbnode)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd.left)) +
//		unsafe.Sizeof(*nd)))
//	nd.metadata().setaccess(uint64(time.Now().UnixNano()))
//	newnd := llrb.clone(nd)
//	if llrb.equivalent(nd, newnd) == false {
//		t.Errorf("expected %v, got %v", nd.repr(), newnd.repr())
//	}
//}
//
//func TestLLRBBasic(t *testing.T) {
//	config := makellrbconfig()
//	config["metadata.mvalue"] = true
//	config["metadata.bornseqno"] = true
//	config["metadata.vbuuid"] = true
//	llrb := NewLLRB("test", config, nil)
//	if llrb.Count() != 0 {
//		t.Fatalf("expected an empty dict")
//	}
//	// inserts
//	inserts := [][2][]byte{
//		[2][]byte{[]byte("key1"), []byte("value1")},
//		[2][]byte{[]byte("key2"), []byte("value2")},
//		[2][]byte{[]byte("key3"), []byte("value3")},
//		[2][]byte{[]byte("key4"), []byte("value4")},
//		[2][]byte{[]byte("key5"), []byte("value5")},
//	}
//	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
//	for _, kv := range inserts {
//		llrb.Upsert(
//			kv[0], kv[1],
//			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
//				if oldnd != nil {
//					t.Errorf("expected old Llrbnode as nil")
//				}
//				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
//				llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
//				llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno})
//			})
//		seqno++
//	}
//	// lookups
//	if llrb.Has(inserts[1][0]) == false {
//		t.Errorf("expected key - %v", string(inserts[0][0]))
//	}
//	nd := llrb.Get(inserts[2][0])
//	v := nd.nodevalue().value()
//	if bytes.Compare(v, inserts[2][1]) != 0 {
//		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
//	}
//	nd = llrb.Min()
//	k, v := nd.key(), nd.nodevalue().value()
//	if bytes.Compare(k, inserts[0][0]) != 0 {
//		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
//	} else if bytes.Compare(v, inserts[0][1]) != 0 {
//		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
//	}
//	nd = llrb.Max()
//	k, v = nd.key(), nd.nodevalue().value()
//	if bytes.Compare(k, []byte("key5")) != 0 {
//		t.Errorf("expected %v, got %v", "key5", string(k))
//	} else if bytes.Compare(v, []byte("value5")) != 0 {
//		t.Errorf("expected %v, got %v", "value5", string(v))
//	}
//	// update
//	newvalue := []byte("value11")
//	llrb.Upsert(
//		inserts[0][0], newvalue,
//		func(llrb *LLRB, newnd, oldnd *Llrbnode) {
//			nd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
//			vs := oldnd.nodevalue().value()
//			if bytes.Compare(vs, inserts[0][1]) != 0 {
//				fmsg := "expected %v, got %v\n"
//				t.Errorf(fmsg, string(inserts[0][1]), string(vs))
//			}
//			vs = newnd.nodevalue().value()
//			if bytes.Compare(vs, newvalue) != 0 {
//				fmsg := "expected %v, got %v\n"
//				t.Errorf(fmsg, string(newvalue), string(vs))
//			}
//		})
//	// deletes
//	llrb.DeleteMin(
//		func(llrb *LLRB, nd *Llrbnode) {
//			k, v = nd.key(), nd.nodevalue().value()
//			fmsg := "expected %v, got %v"
//			if bytes.Compare(k, inserts[0][0]) != 0 {
//				t.Errorf(fmsg, string(inserts[0][0]), string(k))
//			} else if bytes.Compare(v, []byte("value11")) != 0 {
//				t.Errorf(fmsg, string(inserts[0][1]), string(v))
//			} else if llrb.Count() != int64(len(inserts)-1) {
//				t.Errorf(fmsg, len(inserts)-1, llrb.Count())
//			}
//		})
//	llrb.DeleteMax(
//		func(llrb *LLRB, nd *Llrbnode) {
//			k, v = nd.key(), nd.nodevalue().value()
//			fmsg := "expected %v, got %v"
//			if bytes.Compare(k, []byte("key5")) != 0 {
//				t.Errorf(fmsg, "key5", string(k))
//			} else if bytes.Compare(v, []byte("value5")) != 0 {
//				t.Errorf(fmsg, "value5", string(v))
//			} else if llrb.Count() != int64(len(inserts)-2) {
//				t.Errorf(fmsg, len(inserts)-2, llrb.Count())
//			}
//		})
//	llrb.Delete(
//		[]byte("key2"),
//		func(llrb *LLRB, nd *Llrbnode) {
//			v = nd.nodevalue().value()
//			fmsg := "expected %v, got %v"
//			if bytes.Compare(v, []byte("value2")) != 0 {
//				t.Errorf(fmsg, "value2", string(v))
//			} else if llrb.Count() != int64(len(inserts)-3) {
//				t.Errorf(fmsg, len(inserts)-3, llrb.Count())
//			}
//		})
//}
//
//func TestLLRBBasicRange(t *testing.T) {
//	config := makellrbconfig()
//	config["metadata.mvalue"] = true
//	config["metadata.bornseqno"] = true
//	config["metadata.vbuuid"] = true
//	llrb := NewLLRB("test", config, nil)
//	// inserts
//	inserts := [][2][]byte{
//		[2][]byte{[]byte("key1"), []byte("value1")},
//		[2][]byte{[]byte("key2"), []byte("value2")},
//		[2][]byte{[]byte("key3"), []byte("value3")},
//		[2][]byte{[]byte("key4"), []byte("value4")},
//		[2][]byte{[]byte("key5"), []byte("value5")},
//	}
//	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
//	for _, kv := range inserts {
//		llrb.Upsert(
//			kv[0], kv[1],
//			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
//				if oldnd != nil {
//					t.Errorf("expected old Llrbnode as nil")
//				}
//				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
//			})
//		seqno++
//	}
//	// both
//	i, ln := 0, 0
//	llrb.Range(
//		inserts[0][0], inserts[4][0], "both",
//		func(nd *Llrbnode) bool {
//			k, v := nd.key(), nd.nodevalue().value()
//			if bytes.Compare(inserts[i][0], k) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
//			} else if bytes.Compare(inserts[i][1], v) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
//			}
//			i++
//			ln++
//			return true
//		})
//	if ln != len(inserts) {
//		t.Errorf("expected %v, got %v", len(inserts), ln)
//	}
//	// none
//	i, ln = 1, 0
//	llrb.Range(
//		inserts[0][0], inserts[4][0], "none",
//		func(nd *Llrbnode) bool {
//			k, v := nd.key(), nd.nodevalue().value()
//			if bytes.Compare(inserts[i][0], k) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
//			} else if bytes.Compare(inserts[i][1], v) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
//			}
//			i++
//			ln++
//			return true
//		})
//	if ln != (len(inserts) - 2) {
//		t.Errorf("expected %v, got %v", len(inserts)-2, ln)
//	}
//	// high
//	i, ln = 1, 0
//	llrb.Range(
//		inserts[0][0], inserts[4][0], "high",
//		func(nd *Llrbnode) bool {
//			k, v := nd.key(), nd.nodevalue().value()
//			if bytes.Compare(inserts[i][0], k) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
//			} else if bytes.Compare(inserts[i][1], v) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
//			}
//			i++
//			ln++
//			return true
//		})
//	if ln != (len(inserts) - 1) {
//		t.Errorf("expected %v, got %v", len(inserts)-1, ln)
//	}
//	// low
//	i, ln = 0, 0
//	llrb.Range(
//		inserts[0][0], inserts[4][0], "low",
//		func(nd *Llrbnode) bool {
//			k, v := nd.key(), nd.nodevalue().value()
//			if bytes.Compare(inserts[i][0], k) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
//			} else if bytes.Compare(inserts[i][1], v) != 0 {
//				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
//			}
//			i++
//			ln++
//			return true
//		})
//	if ln != (len(inserts) - 1) {
//		t.Errorf("expected %v, got %v", len(inserts)-1, ln)
//	}
//	// corner case on the high side.
//	i, ln = 0, 0
//	llrb.Range(
//		inserts[0][0], inserts[0][0], "high",
//		func(nd *Llrbnode) bool { return true })
//	if ln != 0 {
//		t.Errorf("expected %v, got %v", 0, ln)
//	}
//	// corner case on the low side.
//	i, ln = 0, 0
//	llrb.Range(
//		inserts[4][0], inserts[4][0], "low",
//		func(nd *Llrbnode) bool { return true })
//	if ln != 0 {
//		t.Errorf("expected %v, got %v", 0, ln)
//	}
//}
