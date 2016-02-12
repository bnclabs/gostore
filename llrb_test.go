package storage

import "testing"
import "fmt"

import "unsafe"
import "time"
import "math/rand"
import "bytes"

var _ = fmt.Sprintf("dummy")

func TestNewLLRB(t *testing.T) {
	config := makellrbconfig()

	llrb := NewLLRB("test", config, nil)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}

	mstats := llrb.StatsMem()

	ovrhd, used, allc := int64(2112), int64(0), int64(0)
	nodavail := int64(config["nodearena.capacity"].(int))
	if x := mstats["node.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := mstats["node.useful"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := mstats["node.allocated"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
	} else if x, y := nodavail, mstats["node.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	ovrhd, used, allc = int64(2112), int64(0), int64(0)
	valavail := int64(config["valarena.capacity"].(int))
	if x := mstats["value.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := mstats["value.useful"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := mstats["value.allocated"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
	} else if x, y := valavail, mstats["value.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	if x, y := int64(0), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(0), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestNewLLRBNode(t *testing.T) {
	config := makellrbconfig()
	config["metadata.bornseqno"] = true
	config["metadata.deadseqno"] = true
	config["metadata.mvalue"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	vbno, vbuuid := uint16(10), uint64(0xABCD)
	bnseqno, ddseqno := uint64(12345678), uint64(12345690)
	nd := llrb.newnode(key, value)
	fmask := metadataMask(0).enableMvalue().enableVbuuid()
	fmask = fmask.enableBornSeqno().enableDeadSeqno()
	nd.metadata().initMetadata(vbno, fmask).setvbuuid(vbuuid)
	nd.metadata().setbnseq(bnseqno).setddseq(ddseqno)

	if k := nd.key(); bytes.Compare(k, key) != 0 {
		t.Errorf("expected %v, got %v", key, nd.key())
	} else if v := nd.nodevalue().value(); bytes.Compare(v, value) != 0 {
		t.Errorf("expected %v, got %v", value, v)
	} else if x := nd.metadata().vbno(); x != vbno {
		t.Errorf("expected %v, got %v", vbno, x)
	} else if x := nd.metadata().vbuuid(); x != vbuuid {
		t.Errorf("expected %x, got %x", vbuuid, x)
	} else if x := nd.metadata().bnseq(); x != bnseqno {
		t.Errorf("expected %x, got %x", bnseqno, x)
	} else if x := nd.metadata().ddseq(); x != ddseqno {
		t.Errorf("expected %x, got %x", ddseqno, x)
	}
	llrb.freenode(nd)
}

func TestNewLLRBNodePanic(t *testing.T) {
	llrb := NewLLRB("test", makellrbconfig(), nil)
	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	func() {
		defer func() {
			if recover() == nil {
				t.Errorf("expected panic")
			}
		}()
		llrb.newnode(key, value)
	}()
}

func TestCloneLLRBNode(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	llrb := NewLLRB("test", config, nil)
	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	nd := llrb.newnode(key, value)
	nd.metadata().setblack().setdirty()
	nd.left = (*Llrbnode)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd)) +
		unsafe.Sizeof(*nd)))
	nd.right = (*Llrbnode)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd.left)) +
		unsafe.Sizeof(*nd)))
	nd.metadata().setaccess(uint64(time.Now().UnixNano()))
	newnd := llrb.clone(nd)
	if llrb.equivalent(nd, newnd) == false {
		t.Errorf("expected %v, got %v", nd.repr(), newnd.repr())
	}
}

func TestLLRBBasic(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	for _, kv := range inserts {
		llrb.Upsert(
			kv[0], kv[1],
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
				llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
				llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno})
			})
		seqno++
	}
	// lookups
	if llrb.Has(inserts[1][0]) == false {
		t.Errorf("expected key - %v", string(inserts[0][0]))
	}
	nd := llrb.Get(inserts[2][0])
	v := nd.nodevalue().value()
	if bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	nd = llrb.Min()
	k, v := nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	nd = llrb.Max()
	k, v = nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}
	// update
	newvalue := []byte("value11")
	llrb.Upsert(
		inserts[0][0], newvalue,
		func(llrb *LLRB, newnd, oldnd *Llrbnode) {
			nd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
			vs := oldnd.nodevalue().value()
			if bytes.Compare(vs, inserts[0][1]) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(inserts[0][1]), string(vs))
			}
			vs = newnd.nodevalue().value()
			if bytes.Compare(vs, newvalue) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(newvalue), string(vs))
			}
		})
	// deletes
	llrb.DeleteMin(
		func(llrb *LLRB, nd *Llrbnode) {
			k, v = nd.key(), nd.nodevalue().value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(k, inserts[0][0]) != 0 {
				t.Errorf(fmsg, string(inserts[0][0]), string(k))
			} else if bytes.Compare(v, []byte("value11")) != 0 {
				t.Errorf(fmsg, string(inserts[0][1]), string(v))
			} else if llrb.Count() != int64(len(inserts)-1) {
				t.Errorf(fmsg, len(inserts)-1, llrb.Count())
			}
		})
	llrb.DeleteMax(
		func(llrb *LLRB, nd *Llrbnode) {
			k, v = nd.key(), nd.nodevalue().value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(k, []byte("key5")) != 0 {
				t.Errorf(fmsg, "key5", string(k))
			} else if bytes.Compare(v, []byte("value5")) != 0 {
				t.Errorf(fmsg, "value5", string(v))
			} else if llrb.Count() != int64(len(inserts)-2) {
				t.Errorf(fmsg, len(inserts)-2, llrb.Count())
			}
		})
	llrb.Delete(
		[]byte("key2"),
		func(llrb *LLRB, nd *Llrbnode) {
			v = nd.nodevalue().value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(v, []byte("value2")) != 0 {
				t.Errorf(fmsg, "value2", string(v))
			} else if llrb.Count() != int64(len(inserts)-3) {
				t.Errorf(fmsg, len(inserts)-3, llrb.Count())
			}
		})
}

func TestLLRBBasicRange(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	// inserts
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	for _, kv := range inserts {
		llrb.Upsert(
			kv[0], kv[1],
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
			})
		seqno++
	}
	// both
	i, ln := 0, 0
	llrb.Range(
		inserts[0][0], inserts[4][0], "both",
		func(nd *Llrbnode) bool {
			k, v := nd.key(), nd.nodevalue().value()
			if bytes.Compare(inserts[i][0], k) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
			} else if bytes.Compare(inserts[i][1], v) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
			}
			i++
			ln++
			return true
		})
	if ln != len(inserts) {
		t.Errorf("expected %v, got %v", len(inserts), ln)
	}
	// none
	i, ln = 1, 0
	llrb.Range(
		inserts[0][0], inserts[4][0], "none",
		func(nd *Llrbnode) bool {
			k, v := nd.key(), nd.nodevalue().value()
			if bytes.Compare(inserts[i][0], k) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
			} else if bytes.Compare(inserts[i][1], v) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
			}
			i++
			ln++
			return true
		})
	if ln != (len(inserts) - 2) {
		t.Errorf("expected %v, got %v", len(inserts)-2, ln)
	}
	// high
	i, ln = 1, 0
	llrb.Range(
		inserts[0][0], inserts[4][0], "high",
		func(nd *Llrbnode) bool {
			k, v := nd.key(), nd.nodevalue().value()
			if bytes.Compare(inserts[i][0], k) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
			} else if bytes.Compare(inserts[i][1], v) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
			}
			i++
			ln++
			return true
		})
	if ln != (len(inserts) - 1) {
		t.Errorf("expected %v, got %v", len(inserts)-1, ln)
	}
	// low
	i, ln = 0, 0
	llrb.Range(
		inserts[0][0], inserts[4][0], "low",
		func(nd *Llrbnode) bool {
			k, v := nd.key(), nd.nodevalue().value()
			if bytes.Compare(inserts[i][0], k) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][0], string(k))
			} else if bytes.Compare(inserts[i][1], v) != 0 {
				t.Errorf("expected key %v, got %v", inserts[i][1], string(v))
			}
			i++
			ln++
			return true
		})
	if ln != (len(inserts) - 1) {
		t.Errorf("expected %v, got %v", len(inserts)-1, ln)
	}
	// corner case on the high side.
	i, ln = 0, 0
	llrb.Range(
		inserts[0][0], inserts[0][0], "high",
		func(nd *Llrbnode) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	i, ln = 0, 0
	llrb.Range(
		inserts[4][0], inserts[4][0], "low",
		func(nd *Llrbnode) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
}

func TestLLRBInsert(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode to be nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
			})
		seqno++
	}
	// check memory accounting
	mstats := llrb.StatsMem()
	overhead, useful := int64(3814), int64(2096640)
	allocated, avail := int64(1600000), int64(1072141824)
	if x := mstats["node.overhead"].(int64); x != overhead {
		t.Errorf("expected %v, got %v", overhead, x)
	} else if x := mstats["node.useful"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	} else if x := mstats["node.allocated"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	} else if x := mstats["node.available"].(int64); x != avail {
		t.Errorf("expected %v, got %v", avail, x)
	}
	overhead, useful = int64(22656), int64(20971520)
	allocated, avail = int64(1280000), int64(10736138240)
	if x := mstats["value.overhead"].(int64); x != overhead {
		t.Errorf("expected %v, got %v", overhead, x)
	} else if x := mstats["value.useful"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	} else if x := mstats["value.allocated"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	} else if x := mstats["value.available"].(int64); x != avail {
		t.Errorf("expected %v, got %v", avail, x)
	}
	if x, y := int64(1000000), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// Get items
	vbno, vbuuid, seqno = uint16(10), uint64(0xABCD), uint64(12345678)
	for i, key := range keys {
		nd := llrb.Get(key)
		if nd == nil {
			t.Errorf("unexpected nil")
		} else if x := nd.metadata().vbno(); x != vbno {
			t.Errorf("expected %v, got %v", vbno, x)
		} else if x := nd.metadata().vbuuid(); x != vbuuid {
			t.Errorf("expected %v, got %v", vbuuid, x)
		} else if x := nd.metadata().bnseq(); x != seqno {
			t.Errorf("expected %v, got %v", seqno, x)
		}
		x, y := values[i], nd.nodevalue().value()
		if bytes.Compare(x, y) != 0 {
			t.Errorf("expected %v, got %v", x, y)
		}
		seqno++
	}
}

func TestLLRBUpsert(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode to be nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
				keys, values = append(keys, key), append(values, value)
			})
		seqno++
	}

	// upsert same items
	newvalues := make([][]byte, 0)
	for i, key := range keys {
		value := make([]byte, 200)
		_, value = makekeyvalue(nil, value)
		newvalues = append(newvalues, value)
		llrb.Upsert(
			key, value,
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd == nil {
					t.Errorf("unexpected nil")
				} else if x := newnd.metadata().vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := newnd.metadata().vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
				x, y := values[i], oldnd.nodevalue().value()
				z := newnd.nodevalue().value()
				if bytes.Compare(x, y) != 0 {
					fmsg := "%q expected old %s, got %s"
					t.Errorf(fmsg, string(key), string(x), string(y))
				} else if bytes.Compare(value, z) != 0 {
					fmsg := "%q expected new %s, got %s"
					t.Errorf(fmsg, string(key), string(value), string(z))
				}
			})
		seqno++
	}

	// check memory accounting
	mstats := llrb.StatsMem()
	if overhead := mstats["node.overhead"].(int64); overhead != 3814 {
		t.Errorf("expected %v, got %v", 3814, overhead)
	} else if useful := mstats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	} else if overhead := mstats["value.overhead"].(int64); overhead != 34422 {
		t.Errorf("expected %v, got %v", 34422, overhead)
	} else if useful := mstats["value.useful"].(int64); useful != 41941504 {
		t.Errorf("expected %v, got %v", 41941504, useful)
	} else if x, y := int64(1600000), mstats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(2240000), mstats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(1072141824), mstats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(10735178240), mstats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2000000), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestLLRBDelete(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				keys, values = append(keys, key), append(values, value)
				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
			})
		seqno++
	}
	// Delete items
	vbno, vbuuid, seqno = uint16(10), uint64(0xABCD), uint64(12345678)
	for i, key := range keys {
		llrb.Delete(
			key,
			func(llrb *LLRB, nd *Llrbnode) {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.metadata().vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.metadata().vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				} else if x := nd.metadata().bnseq(); x != seqno {
					t.Errorf("expected %v, got %v", seqno, x)
				}
				x, y := values[i], nd.nodevalue().value()
				if bytes.Compare(x, y) != 0 {
					t.Errorf("expected %s, got %s", x, y)
				}
			})
		seqno++
	}
	// check memory accounting
	mstats := llrb.StatsMem()
	if overhead := mstats["node.overhead"].(int64); overhead != 3814 {
		t.Errorf("expected %v, got %v", 3814, overhead)
	} else if useful := mstats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	} else if overhead := mstats["value.overhead"].(int64); overhead != 22656 {
		t.Errorf("expected %v, got %v", 22656, overhead)
	} else if useful := mstats["value.useful"].(int64); useful != 20971520 {
		t.Errorf("expected %v, got %v", 20971520, useful)
	} else if x, y := int64(0), mstats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), mstats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1073741824), mstats["node.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(10737418240), mstats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestLLRBRange(t *testing.T) {
	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := NewLLRB("test", config, nil)
	d := NewDict()
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
			})
		d.Upsert(key, value)
		keys, values = append(keys, key), append(values, value)
		seqno++
	}
	// random ranges
	repeat := 1000
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		llrb.Range(lowkey, highkey, incl, func(nd *Llrbnode) bool {
			llrbks = append(llrbks, nd.key())
			llrbvs = append(llrbvs, nd.nodevalue().value())
			return true
		})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(lowkey, highkey, incl, func(k, v []byte) bool {
			dks, dvs = append(dks, k), append(dvs, v)
			return true
		})
		if len(llrbks) != len(dks) {
			t.Errorf("expected %v, got %v", len(llrbks), len(dks))
		} else {
			for j, llrbk := range llrbks {
				if bytes.Compare(llrbk, dks[j]) != 0 {
					t.Errorf("expected %v, got %v", llrbk, dks[j])
				}
			}
		}
	}
}

func makekeyvalue(key, value []byte) ([]byte, []byte) {
	if key != nil {
		for i := 0; i < len(key); i++ {
			x := rand.Intn(26)
			key[i] = byte(97 + (x % 26))
		}
	}
	if value != nil {
		for i := 0; i < len(value); i++ {
			x := rand.Intn(26)
			value[i] = byte(97 + (x % 26))
		}
	}
	return key, value
}

func makellrbconfig() map[string]interface{} {
	config := map[string]interface{}{
		"maxvb":                   1024,
		"mvcc.enabled":            false,
		"mvcc.snapshot.tick":      5, // 5 millisecond
		"mvcc.writer.chanbuffer":  1000,
		"nodearena.minblock":      96,
		"nodearena.maxblock":      1024,
		"nodearena.capacity":      1024 * 1024 * 1024,
		"nodearena.pool.capacity": 2 * 1024 * 1024,
		"valarena.minblock":       96,
		"valarena.maxblock":       1024 * 1024,
		"valarena.capacity":       10 * 1024 * 1024 * 1024,
		"valarena.pool.capacity":  10 * 2 * 1024 * 1024,
	}
	return config
}
