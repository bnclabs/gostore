package storage

import "testing"
import "fmt"
import "bytes"
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

	time.Sleep(100 * time.Millisecond)
	waitch = make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot = <-waitch
	if len(snapshot.reclaim) != 0 {
		t.Errorf("expected reclaim nodes to be zero")
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

	time.Sleep(100 * time.Millisecond)
	waitch = make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot = <-waitch
	if len(snapshot.reclaim) != 0 {
		t.Errorf("expected reclaim nodes to be zero")
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
	time.Sleep(100 * time.Millisecond)
	// check
	if countref != llrb.Count() {
		t.Errorf("expected %v, got %v", countref, llrb.Count())
	} else if nd := llrb.Get(inserts[0][0]); nd == nil {
		t.Errorf("expeced valid node")
	} else if x := nd.nodevalue().value(); bytes.Compare(newvalue, x) != 0 {
		t.Errorf("expected %v, got %v", newvalue, nd.nodevalue().value())
	}
	// snapshot
	waitch := make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot := <-waitch
	if len(snapshot.reclaim) != 0 {
		t.Errorf("expected reclaim nodes to be zero")
	}
	// validate
	if snapshot.ValidateReds() == false {
		t.Errorf("validate-reds failed")
	}
	snapshot.ValidateBlacks()
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
	waitch = make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot = <-waitch
	if len(snapshot.reclaim) != 0 {
		t.Errorf("expected reclaim nodes to be zero")
	}
	// validate
	if snapshot.ValidateReds() == false {
		t.Errorf("validate-reds failed")
	}
	snapshot.ValidateBlacks()
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
	waitch = make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot = <-waitch
	if len(snapshot.reclaim) != 0 {
		t.Errorf("expected reclaim nodes to be zero")
	}
	// validate
	if snapshot.ValidateReds() == false {
		t.Errorf("validate-reds failed")
	}
	snapshot.ValidateBlacks()
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
	waitch = make(chan *LLRBSnapshot, 1)
	if err := writer.GetSnapshot(waitch); err != nil {
		t.Error(err)
	}
	snapshot = <-waitch
	if len(snapshot.reclaim) != 0 {
		t.Errorf("expected reclaim nodes to be zero")
	}
	// validate
	if snapshot.ValidateReds() == false {
		t.Errorf("validate-reds failed")
	}
	snapshot.ValidateBlacks()
	snapshot.Release()

	llrb.Destroy()
}

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

func makellrbmvcc(
	t *testing.T, nm string, inserts [][2][]byte,
	config map[string]interface{}) *LLRB {

	llrb := NewLLRB(nm, config, nil)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	writer := llrb.MVCCWriter()

	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
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
