package storage

import "testing"
import "fmt"
import "bytes"
import "math/rand"

import "time"

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

func TestLLRBMvcclBasicSnapshot(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccsnapshot", inserts, config)
	writer := llrb.mvcc.writer

	waitch := make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot := <-waitch
	if len(snapshot.reclaim) == 0 {
		t.Errorf("expected reclaim nodes to be greater than zero")
	}
	snapshot.Release()

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	llrb.Destroy()
}

func TestLLRBMvcclBasicLookup(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvcclookup", inserts, config)
	writer := llrb.mvcc.writer

	waitch := make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot := <-waitch
	if len(snapshot.reclaim) == 0 {
		t.Errorf("expected reclaim nodes to be greater than zero")
	}
	snapshot.Release()

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// get
	if snapshot.Has(inserts[1][0]) == false {
		t.Errorf("expected key - %v", string(inserts[0][0]))
	}
	nd := snapshot.Get(inserts[2][0])
	v := nd.nodevalue().value()
	if bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	if nd := snapshot.Get([]byte("key10")); nd != nil {
		t.Errorf("expected nil when Get() on missing key")
	}
	// min
	nd = snapshot.Min()
	k, v := nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	// max
	nd = snapshot.Max()
	k, v = nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}

	snapshot.Release()
	llrb.Destroy()
}

func TestLLRBMvccBasicUpdates(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccupdates", inserts, config)
	writer := llrb.mvcc.writer

	// update
	newvalue := []byte("value11")
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(1234567890)
	countref := llrb.Count()
	writer.Upsert(
		inserts[0][0], newvalue,
		func(llrb *LLRB, newnd, oldnd *Llrbnode) {
			newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
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

	// check
	if countref != llrb.Count() {
		t.Errorf("expected %v, got %v", countref, llrb.Count())
	} else if nd := llrb.Get(inserts[0][0]); nd == nil {
		t.Errorf("expeced valid node")
	} else if x := nd.nodevalue().value(); bytes.Compare(newvalue, x) != 0 {
		t.Errorf("expected %v, got %v", newvalue, nd.nodevalue().value())
	}

	// snapshot
	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	//-- delete
	countref, key, value := llrb.Count(), []byte(nil), []byte(nil)
	writer.DeleteMin(
		func(llrb *LLRB, nd *Llrbnode) {
			key, value = nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(key, inserts[0][0]) != 0 {
				t.Errorf(fmsg, string(inserts[0][0]), string(key))
			} else if bytes.Compare(value, []byte("value11")) != 0 {
				t.Errorf(fmsg, string(inserts[0][1]), string(value))
			} else if llrb.Count() != int64(len(inserts)-1) {
				t.Errorf(fmsg, len(inserts)-1, llrb.Count())
			}
		})
	time.Sleep(100 * time.Millisecond)
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := snapshot.Get(key); bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}
	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// delete-max
	countref, key, value = llrb.Count(), nil, nil
	writer.DeleteMax(
		func(llrb *LLRB, nd *Llrbnode) {
			key, value := nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(key, []byte("key5")) != 0 {
				t.Errorf(fmsg, "key5", string(key))
			} else if bytes.Compare(value, []byte("value5")) != 0 {
				t.Errorf(fmsg, "value5", string(value))
			} else if llrb.Count() != int64(len(inserts)-2) {
				t.Errorf(fmsg, len(inserts)-2, llrb.Count())
			}
		})
	time.Sleep(100 * time.Millisecond)
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := snapshot.Get(key); bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}
	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// delete-min
	countref, key, value = llrb.Count(), []byte("key2"), nil
	writer.Delete(
		key,
		func(llrb *LLRB, nd *Llrbnode) {
			value = nd.nodevalue().value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(value, []byte("value2")) != 0 {
				t.Errorf(fmsg, "value2", string(value))
			} else if llrb.Count() != int64(len(inserts)-3) {
				t.Errorf(fmsg, len(inserts)-3, llrb.Count())
			}
		})
	time.Sleep(100 * time.Millisecond)
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := snapshot.Get(key); bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}
	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()
	llrb.Destroy()
}

func TestLLRBMvccBasicRange(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccrange", inserts, config)
	writer := llrb.mvcc.writer

	// snapshot
	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// both
	i, ln := 0, 0
	snapshot.Range(
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
	snapshot.Range(
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
	snapshot.Range(
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
	snapshot.Range(
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
	snapshot.Range(
		inserts[0][0], inserts[0][0], "high",
		func(nd *Llrbnode) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	i, ln = 0, 0
	snapshot.Range(
		inserts[4][0], inserts[4][0], "low",
		func(nd *Llrbnode) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	snapshot.Release()
	llrb.Destroy()
}

func TestLLRBMvccInsert(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true

	inserts := make([][2][]byte, 0)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccinsert", inserts, config)
	writer := llrb.mvcc.writer

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// check memory accounting
	mstats, err := writer.StatsMem()
	if err != nil {
		t.Error(err)
	}
	overhead, useful := int64(4906), int64(2096640)
	allocated, avail := int64(960000), int64(1072781824)
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
	if x, y := int64(100000), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// Get items
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	for i, key := range keys {
		nd := snapshot.Get(key)
		if nd == nil {
			t.Fatalf("unexpected nil")
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

	snapshot.Release()
	llrb.Destroy()
}

func TestLLRBMvccUpsert(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true

	inserts := make([][2][]byte, 0)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccupsert", inserts, config)
	writer := llrb.mvcc.writer

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// upsert same items
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	newvalues := make([][]byte, 0)
	for i, key := range keys {
		_, value := makekeyvalue(nil, make([]byte, 200))
		newvalues = append(newvalues, value)
		writer.Upsert(
			key, value,
			func(llrb *LLRB, newnd, oldnd *Llrbnode) {
				if oldnd == nil {
					t.Errorf("unexpected nil")
				} else if x := oldnd.metadata().vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := oldnd.metadata().vbuuid(); x != vbuuid {
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

				newnd.metadata().setvbno(vbno).setvbuuid(vbuuid).setbnseq(seqno)
				llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
				llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno})
			})
		seqno++
	}

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// check memory accounting
	mstats, err := writer.StatsMem()
	if err != nil {
		t.Error(err)
	}
	if overhead := mstats["node.overhead"].(int64); overhead != 4906 {
		t.Errorf("expected %v, got %v", 4906, overhead)
	} else if useful := mstats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	} else if overhead := mstats["value.overhead"].(int64); overhead != 34422 {
		t.Errorf("expected %v, got %v", 34422, overhead)
	} else if useful := mstats["value.useful"].(int64); useful != 41941504 {
		t.Errorf("expected %v, got %v", 41941504, useful)
	} else if x, y := int64(960000), mstats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(2240000), mstats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(1072781824), mstats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(10735178240), mstats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(100000), mstats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2000000), mstats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	llrb.Destroy()
}

func TestLLRBMvccDelete(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true

	inserts := make([][2][]byte, 0)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccdelete", inserts, config)
	writer := llrb.mvcc.writer

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// Delete items
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	for i, key := range keys[:count/2] {
		writer.Delete(
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

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// delete minimums
	for i := 0; i < len(keys[count/2:(2*count)/3]); i++ {
		writer.DeleteMin(
			func(llrb *LLRB, nd *Llrbnode) {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.metadata().vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.metadata().vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
			})
	}

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// delete maximums
	for i := 0; i < len(keys[(2*count)/3:]); i++ {
		writer.DeleteMax(
			func(llrb *LLRB, nd *Llrbnode) {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.metadata().vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.metadata().vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
			})
	}

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	time.Sleep(100 * time.Millisecond)

	if x := llrb.Count(); x > 0 {
		t.Errorf("expected 0, got %v", x)
	}
	// check memory accounting
	mstats, err := writer.StatsMem()
	if err != nil {
		t.Error(err)
	}
	if overhead := mstats["node.overhead"].(int64); overhead != 4906 {
		t.Errorf("expected %v, got %v", 4906, overhead)
	} else if useful := mstats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	} else if x, y := int64(0), mstats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1073741824), mstats["node.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if overhead := mstats["value.overhead"].(int64); overhead != 22656 {
		t.Errorf("expected %v, got %v", 22656, overhead)
	} else if useful := mstats["value.useful"].(int64); useful != 20971520 {
		t.Errorf("expected %v, got %v", 20971520, useful)
	} else if x, y = int64(0), mstats["value.allocated"].(int64); x != y {
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

	llrb.Destroy()
}

func TestLLRBMvccRange(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enabled"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true

	llrb := makellrbmvcc(t, "mvccrange", nil, config)
	writer := llrb.mvcc.writer
	d := NewDict()
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 10), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		writer.Upsert(
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

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
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
		snapshot.Range(lowkey, highkey, incl, func(nd *Llrbnode) bool {
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

	snapshot.Release()
	llrb.Destroy()
}

func makellrbmvcc(
	t *testing.T, nm string, inserts [][2][]byte,
	config map[string]interface{}) *LLRB {

	llrb := NewLLRB(nm, config, nil)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	writer := llrb.MVCCWriter()

	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	for _, kv := range inserts {
		writer.Upsert(
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
	return llrb
}

func validatesnapshot(sleep int, writer *LLRBWriter) (*LLRBSnapshot, error) {
	time.Sleep(time.Duration(sleep) * time.Millisecond)

	waitch := make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		return nil, err
	}
	snapshot := <-waitch
	if len(snapshot.reclaim) != 0 {
		return nil, fmt.Errorf("expected reclaim nodes to be zero")
	}

	// validate
	if snapshot.ValidateReds() == false {
		return nil, fmt.Errorf("validate-reds failed")
	} else if snapshot.ValidateDirty() == false {
		return nil, fmt.Errorf("validate-dirty failed")
	}
	snapshot.ValidateBlacks()
	return snapshot, nil
}
