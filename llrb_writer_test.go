package storage

import "testing"
import "fmt"
import "bytes"
import "math/rand"

import "time"

var _ = fmt.Sprintf("dummy")

func TestNewLLRBMvcc(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enable"] = true

	llrb := NewLLRB("test", config, nil)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}
	nodavail := config["nodearena.capacity"].(int)
	valavail := config["valarena.capacity"].(int)

	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(stats)
	}

	if overhead := stats["node.overhead"].(int64); overhead != 2112 {
		t.Errorf("expected %v, got %v", 0, 2112)
	} else if useful := stats["node.useful"].(int64); useful != 0 {
		t.Errorf("expected %v, got %v", 0, useful)
	} else if overhead = stats["value.overhead"].(int64); overhead != 2112 {
		t.Errorf("expected %v, got %v", 0, overhead)
	} else if useful = stats["value.useful"].(int64); useful != 0 {
		t.Errorf("expected %v, got %v", 0, overhead)
	} else if x, y := int64(0), stats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(nodavail), stats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(valavail), stats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := int64(0), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(0), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestLLRBMvccBasicSnapshot(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	config := makellrbconfig()
	config["mvcc.enable"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccsnapshot", inserts, config)
	writer := llrb.mvcc.writer

	snapch := make(chan Snapshot, 1)
	err := llrb.RSnapshot(snapch)
	if err != nil {
		t.Error(err)
	}
	snapshot := <-snapch
	if len(snapshot.(*LLRBSnapshot).reclaim) > 0 {
		t.Errorf("expected reclaim nodes to be zero")
	}
	snapshot.Release()

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
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
	config["mvcc.enable"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvcclookup", inserts, config)
	writer := llrb.mvcc.writer

	snapch := make(chan Snapshot, 1)
	err := llrb.RSnapshot(snapch)
	if err != nil {
		t.Error(err)
	}
	snapshot := <-snapch

	if len(snapshot.(*LLRBSnapshot).reclaim) > 0 {
		t.Errorf("expected reclaim nodes to be zero")
	}
	snapshot.Release()

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// get
	if snapshot.Has(inserts[1][0]) == false {
		t.Errorf("expected key - %v", string(inserts[0][0]))
	}
	nd := snapshot.Get(inserts[2][0])
	v := nd.Value()
	if bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	if nd := snapshot.Get([]byte("key10")); nd != nil {
		t.Errorf("expected nil when Get() on missing key")
	}
	// min
	nd = snapshot.Min()
	k, v := nd.Key(), nd.Value()
	if bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	// max
	nd = snapshot.Max()
	k, v = nd.Key(), nd.Value()
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
	config["mvcc.enable"] = true
	config["metadata.mvalue"] = true
	config["metadata.bornseqno"] = true
	config["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccupdates", inserts, config)
	writer := llrb.mvcc.writer

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

	snapch := make(chan Snapshot, 1)
	err := llrb.RSnapshot(snapch)
	if err != nil {
		t.Error(err)
	}
	snapshot := <-snapch

	// check
	if countref != llrb.Count() {
		t.Errorf("expected %v, got %v", countref, llrb.Count())
	} else if nd := snapshot.Get(inserts[0][0]); nd == nil {
		t.Errorf("expeced valid node")
	} else if x := nd.Value(); bytes.Compare(newvalue, x) != 0 {
		t.Errorf("expected %v, got %v", newvalue, nd.Value())
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	//-- delete-min
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

	time.Sleep(100 * time.Millisecond)

	// check with old snapshot
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := snapshot.Get(key); bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %q, got %q", string(value), string(nd.Value()))
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// and then with new snapshot
	if nd := snapshot.Get(key); nd != nil {
		t.Errorf("expected nil")
	}

	// delete-max
	countref, key, value = llrb.Count(), nil, nil
	llrb.DeleteMax(
		func(index Index, nd Node) {
			key, value = nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(key, []byte("key5")) != 0 {
				t.Errorf(fmsg, "key5", string(key))
			} else if bytes.Compare(value, []byte("value5")) != 0 {
				t.Errorf(fmsg, "value5", string(value))
			} else if index.Count() != int64(len(inserts)-2) {
				t.Errorf(fmsg, len(inserts)-2, index.Count())
			}
		})

	time.Sleep(100 * time.Millisecond)

	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := snapshot.Get(key); bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	if nd := snapshot.Get(key); nd != nil {
		t.Errorf("expected nil")
	}

	// delete
	countref, key, value = llrb.Count(), []byte("key2"), nil
	llrb.Delete(
		key,
		func(index Index, nd Node) {
			value = nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(value, []byte("value2")) != 0 {
				t.Errorf(fmsg, "value2", string(value))
			} else if index.Count() != int64(len(inserts)-3) {
				t.Errorf(fmsg, len(inserts)-3, index.Count())
			}
		})

	time.Sleep(100 * time.Millisecond)

	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if nd := snapshot.Get(key); bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	if nd := snapshot.Get(key); nd != nil {
		t.Errorf("expected nil")
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
	config["mvcc.enable"] = true
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
	snapshot.Range(
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
	snapshot.Range(
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
	snapshot.Range(
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
	snapshot.Range(
		inserts[0][0], inserts[0][0], "high",
		func(nd Node) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	i, ln = 0, 0
	snapshot.Range(
		inserts[4][0], inserts[4][0], "low",
		func(nd Node) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	snapshot.Release()
	llrb.Destroy()
}

func TestLLRBMvccInsert(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enable"] = true
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
	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(err)
	}
	useful := int64(2096640)
	allocated, avail := int64(960000), int64(1072781824)
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
	if x, y := int64(100000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// Get items
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	for i, key := range keys {
		nd := snapshot.Get(key)
		if nd == nil {
			t.Fatalf("unexpected nil")
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

	snapshot.Release()
	llrb.Destroy()
}

func TestLLRBMvccUpsert(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enable"] = true
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
		llrb.Upsert(
			key, value,
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd == nil {
					t.Errorf("unexpected nil")
				} else if x := oldnd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := oldnd.Vbuuid(); x != vbuuid {
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

				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				llrb := index.(*LLRB)
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

	// TODO: these number keep changing for every run,
	//       figure a way to validate them.
	//x, y := int64(987936), stats["node.allocated"].(int64)
	//if x != y {
	//	t.Errorf("expected %v, got %v", x, y)
	//}
	//x, y = int64(2283968), stats["value.allocated"].(int64)
	//if x != y {
	//	t.Errorf("expected %v, got %v", x, y)
	//}
	//x, y = int64(1072753888), stats["node.available"].(int64)
	//if x != y {
	//	t.Errorf("expected %v, got %v", x, y)
	//}
	//x, y = int64(10735134272), stats["value.available"].(int64)
	//if x != y {
	//	t.Errorf("expected %v, got %v", x, y)
	//}

	if x, y := int64(100000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := int64(2000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	llrb.Destroy()
}

func TestLLRBMvccDelete(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enable"] = true
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

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// delete minimums
	for i := 0; i < len(keys[count/2:(2*count)/3]); i++ {
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

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// delete maximums
	for i := 0; i < len(keys[(2*count)/3:]); i++ {
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
	stats, err := llrb.Stats(9)
	if err != nil {
		t.Error(err)
	}
	if useful := stats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	} else if x, y := int64(0), stats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(1073741824), stats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if useful := stats["value.useful"].(int64); useful != 20971520 {
		t.Errorf("expected %v, got %v", 20971520, useful)
	} else if x, y = int64(0), stats["value.allocated"].(int64); x != y {
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

func TestLLRBMvccRange(t *testing.T) {
	config := makellrbconfig()
	config["mvcc.enable"] = true
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
		llrb.Upsert(
			key, value,
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		d.Upsert(key, value, nil)
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
		snapshot.Range(lowkey, highkey, incl, func(nd Node) bool {
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
	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	for _, kv := range inserts {
		llrb.Upsert(
			kv[0], kv[1],
			func(index Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
				llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno})
			})
		seqno++
	}
	return llrb
}

func validatesnapshot(sleep int, writer *LLRBWriter) (Snapshot, error) {
	time.Sleep(time.Duration(sleep) * time.Millisecond)

	snapch := make(chan Snapshot, 1)
	err := writer.llrb.RSnapshot(snapch)
	if err != nil {
		return nil, err
	}
	snapshot := <-snapch

	// validate
	snapshot.Validate()
	return snapshot, nil
}
