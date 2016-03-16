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

	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(err)
	}

	ovrhd, used, allc := int64(2112), int64(0), int64(0)
	nodavail := int64(config["nodearena.capacity"].(int))
	if x := stats["node.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := stats["node.useful"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := stats["node.allocated"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
	} else if x, y := nodavail, stats["node.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	ovrhd, used, allc = int64(2112), int64(0), int64(0)
	valavail := int64(config["valarena.capacity"].(int))
	if x := stats["value.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := stats["value.useful"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := stats["value.allocated"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
	} else if x, y := valavail, stats["value.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	if x, y := int64(0), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(0), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	llrb.Destroy()
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

	llrb.Destroy()
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

	llrb.Destroy()
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

	llrb.Destroy()
}

func TestLLRBBasicLookup(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrb(t, "basiclookup", inserts, config)

	// get
	if llrb.Has(inserts[1][0]) == false {
		t.Errorf("expected key - %v", string(inserts[0][0]))
	}
	nd := llrb.Get(inserts[2][0])
	if v := nd.Value(); bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	if nd := llrb.Get([]byte("key10")); nd != nil {
		t.Errorf("expected nil when Get() on missing key")
	}
	// min
	nd = llrb.Min()
	if k, v := nd.Key(), nd.Value(); bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	// max
	nd = llrb.Max()
	if k, v := nd.Key(), nd.Value(); bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}

	llrb.Destroy()
}

func TestLLRBBasicUpdates(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrb(t, "basicupdates", inserts, config)

	// update
	newvalue := []byte("value11")
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(1234567890)
	countref := llrb.Count()
	llrb.Upsert(
		inserts[0][0], newvalue,
		func(index Index, _ int64, newnd, oldnd Node) {
			newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			vs := oldnd.Value()
			if bytes.Compare(vs, inserts[0][1]) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(inserts[0][1]), string(vs))
			}
			vs = newnd.Value()
			if bytes.Compare(vs, newvalue) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(newvalue), string(vs))
			}
		})
	// check
	if countref != llrb.Count() {
		t.Errorf("expected %v, got %v", countref, llrb.Count())
	} else if nd := llrb.Get(inserts[0][0]); nd == nil {
		t.Errorf("expeced valid node")
	} else if bytes.Compare(newvalue, nd.Value()) != 0 {
		t.Errorf("expected %v, got %v", newvalue, nd.Value())
	}

	llrb.Validate()

	// delete
	countref, key, value := llrb.Count(), []byte(nil), []byte(nil)
	llrb.DeleteMin(
		func(index Index, nd Node) {
			key, value = nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(key, inserts[0][0]) != 0 {
				t.Errorf(fmsg, string(inserts[0][0]), string(key))
			} else if bytes.Compare(value, []byte("value11")) != 0 {
				t.Errorf(fmsg, string(inserts[0][1]), string(value))
			} else if index.Count() != int64(len(inserts)-1) {
				t.Errorf(fmsg, len(inserts)-1, index.Count())
			}
		})
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := llrb.Get(key); nd != nil {
		t.Errorf("expeced node as nil")
	}

	llrb.Validate()

	// delete-max
	countref, key, value = llrb.Count(), nil, nil
	llrb.DeleteMax(
		func(index Index, nd Node) {
			k, v := nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(k, []byte("key5")) != 0 {
				t.Errorf(fmsg, "key5", string(k))
			} else if bytes.Compare(v, []byte("value5")) != 0 {
				t.Errorf(fmsg, "value5", string(v))
			} else if index.Count() != int64(len(inserts)-2) {
				t.Errorf(fmsg, len(inserts)-2, index.Count())
			}
		})
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := llrb.Get(key); nd != nil {
		t.Errorf("expeced node as nil")
	}

	llrb.Validate()

	// delete-min
	countref, key, value = llrb.Count(), nil, nil
	llrb.Delete(
		[]byte("key2"),
		func(index Index, nd Node) {
			v := nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(v, []byte("value2")) != 0 {
				t.Errorf(fmsg, "value2", string(v))
			} else if index.Count() != int64(len(inserts)-3) {
				t.Errorf(fmsg, len(inserts)-3, index.Count())
			}
		})
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := llrb.Get(key); nd != nil {
		t.Errorf("expeced node as nil")
	}

	llrb.Validate()
	llrb.Destroy()
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
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		seqno++
	}
	// both
	i, ln := 0, 0
	llrb.Range(
		inserts[0][0], inserts[4][0], "both",
		func(nd Node) bool {
			k, v := nd.Key(), nd.Value()
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
		func(nd Node) bool {
			k, v := nd.Key(), nd.Value()
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
		func(nd Node) bool {
			k, v := nd.Key(), nd.Value()
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
		func(nd Node) bool {
			k, v := nd.Key(), nd.Value()
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
		func(nd Node) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	i, ln = 0, 0
	llrb.Range(
		inserts[4][0], inserts[4][0], "low",
		func(nd Node) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}

	llrb.Validate()
	llrb.Destroy()
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
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode to be nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		seqno++
	}

	llrb.Validate()

	// check memory accounting
	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(err)
	}

	useful := int64(2096640)
	allocated, avail := int64(1600000), int64(1072141824)
	if x := stats["node.useful"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	}
	if x := stats["node.allocated"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	}
	if x := stats["node.available"].(int64); x != avail {
		t.Errorf("expected %v, got %v", avail, x)
	}
	useful = int64(20971520)
	allocated, avail = int64(1280000), int64(10736138240)
	if x := stats["value.useful"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	}
	if x := stats["value.allocated"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	}
	if x := stats["value.available"].(int64); x != avail {
		t.Errorf("expected %v, got %v", avail, x)
	}
	if x, y := int64(1000000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := int64(1000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	// Get items
	vbno, vbuuid, seqno = uint16(10), uint64(0xABCD), uint64(12345678)
	for i, key := range keys {
		nd := llrb.Get(key)
		if nd == nil {
			t.Errorf("unexpected nil")
		} else if x := nd.Vbno(); x != vbno {
			t.Errorf("expected %v, got %v", vbno, x)
		} else if x := nd.Vbuuid(); x != vbuuid {
			t.Errorf("expected %v, got %v", vbuuid, x)
		} else if x := nd.Bornseqno(); x != seqno {
			t.Errorf("expected %v, got %v", seqno, x)
		}
		x, y := values[i], nd.Value()
		if bytes.Compare(x, y) != 0 {
			t.Errorf("expected %v, got %v", x, y)
		}
		seqno++
	}

	llrb.Destroy()
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
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode to be nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				keys, values = append(keys, key), append(values, value)
			})
		seqno++
	}

	llrb.Validate()

	// upsert same items
	newvalues := make([][]byte, 0)
	for i, key := range keys {
		value := make([]byte, 200)
		_, value = makekeyvalue(nil, value)
		newvalues = append(newvalues, value)
		llrb.Upsert(
			key, value,
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd == nil {
					t.Errorf("unexpected nil")
				} else if x := newnd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := newnd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
				x, y, z := values[i], oldnd.Value(), newnd.Value()
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

	llrb.Validate()

	// check memory accounting
	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(err)
	}

	if useful := stats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	}
	if useful := stats["value.useful"].(int64); useful != 41941504 {
		t.Errorf("expected %v, got %v", 41941504, useful)
	}
	x, y := int64(1600000), stats["node.allocated"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(2240000), stats["value.allocated"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(1072141824), stats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(10735178240), stats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	llrb.Destroy()
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
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				keys, values = append(keys, key), append(values, value)
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		seqno++
	}

	llrb.Validate()

	// Delete items
	seqno = uint64(12345678)
	for i, key := range keys[:count/2] {
		llrb.Delete(
			key,
			func(index Index, nd Node) {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				} else if x := nd.Bornseqno(); x != seqno {
					t.Errorf("expected %v, got %v", seqno, x)
				}
				x, y := values[i], nd.Value()
				if bytes.Compare(x, y) != 0 {
					t.Errorf("expected %s, got %s", x, y)
				}
			})
		seqno++
	}

	llrb.Validate()

	// delete minimums
	for i := 0; i < len(keys[count/2:(3*count)/4]); i++ {
		llrb.DeleteMin(
			func(index Index, nd Node) {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
			})
	}

	llrb.Validate()

	// delete maximums
	for i := 0; i < len(keys[(3*count)/4:]); i++ {
		llrb.DeleteMax(
			func(index Index, nd Node) {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
			})
	}

	llrb.Validate()

	// check memory accounting
	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(err)
	}

	if useful := stats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	}
	if useful := stats["value.useful"].(int64); useful != 20971520 {
		t.Errorf("expected %v, got %v", 20971520, useful)
	} else if x, y := int64(0), stats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(1073741824), stats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(10737418240), stats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	llrb.Destroy()
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
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		d.Upsert(key, value, nil)
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
		llrb.Range(lowkey, highkey, incl, func(nd Node) bool {
			llrbks = append(llrbks, nd.Key())
			llrbvs = append(llrbvs, nd.Value())
			return true
		})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(lowkey, highkey, incl, func(nd Node) bool {
			dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
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

	llrb.Validate()
	llrb.Destroy()
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

func makellrb(
	t *testing.T, nm string, inserts [][2][]byte,
	config map[string]interface{}) *LLRB {

	llrb := NewLLRB(nm, config, nil)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	for _, kv := range inserts {
		llrb.Upsert(
			kv[0], kv[1],
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				llrb := index.(*LLRB)
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
				llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno})
			})
		seqno++
	}
	return llrb
}
