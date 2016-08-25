package llrb

import "testing"
import "fmt"
import "reflect"
import "bytes"
import "math/rand"
import "time"

import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/dict"

var _ = fmt.Sprintf("dummy")

func TestNewLLRBMvcc(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true

	llrb := NewLLRB("test", setts)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}
	nodavail := setts.Int64("nodearena.capacity")
	valavail := setts.Int64("valarena.capacity")

	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(stats)
	}

	if overhead := stats["node.overhead"].(int64); overhead != 616 {
		t.Errorf("expected %v, got %v", 616, overhead)
	} else if useful := stats["node.useful"].(int64); useful != 0 {
		t.Errorf("expected %v, got %v", 0, useful)
	} else if overhead = stats["value.overhead"].(int64); overhead != 1128 {
		t.Errorf("expected %v, got %v", 1128, overhead)
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

	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.fpos"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccsnapshot", inserts, setts)
	writer := llrb.mvcc.writer

	snapch := make(chan api.IndexSnapshot, 1)
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvcclBasicLookup(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvcclookup", inserts, setts)
	writer := llrb.mvcc.writer

	snapch := make(chan api.IndexSnapshot, 1)
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
	var nd api.Node
	rc := snapshot.Get(
		inserts[2][0],
		func(_ api.Index, _ int64, _, x api.Node) bool {
			nd = x
			return true
		})
	if rc == false {
		t.Errorf("missing key")
	}
	v := nd.Value()
	if bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	if rc := snapshot.Get([]byte("key10"), nil); rc == true {
		t.Errorf("expected nil when Get() on missing key")
	}
	// min
	nd = nil
	rc = snapshot.Min(func(_ api.Index, _ int64, _, x api.Node) bool {
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missing minimum key")
	}
	k, v := nd.Key(), nd.Value()
	if bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	// max
	nd = nil
	rc = snapshot.Max(func(_ api.Index, _ int64, _, x api.Node) bool {
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missing maximum key")
	}
	k, v = nd.Key(), nd.Value()
	if bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}

	snapshot.Release()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccBasicUpdates(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccupdates", inserts, setts)
	writer := llrb.mvcc.writer

	// update
	newvalue := []byte("value11")
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(1234567890)
	countref := llrb.Count()
	llrb.Upsert(
		inserts[0][0], newvalue,
		func(index api.Index, _ int64, newnd, oldnd api.Node) bool {
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
			return false
		})

	snapch := make(chan api.IndexSnapshot, 1)
	err := llrb.RSnapshot(snapch)
	if err != nil {
		t.Error(err)
	}
	snapshot := <-snapch

	// check
	if countref != llrb.Count() {
		t.Errorf("expected %v, got %v", countref, llrb.Count())
	}
	var nd api.Node
	rc := snapshot.Get(
		inserts[0][0],
		func(_ api.Index, _ int64, _, x api.Node) bool {
			nd = x
			return true
		})
	if rc == false {
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
		func(index api.Index, _ int64, _, nd api.Node) bool {
			key, value = nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(key, inserts[0][0]) != 0 {
				t.Errorf(fmsg, string(inserts[0][0]), string(key))
			} else if bytes.Compare(value, []byte("value11")) != 0 {
				t.Errorf(fmsg, string(inserts[0][1]), string(value))
			} else if index.Count() != int64(len(inserts)-1) {
				t.Errorf(fmsg, len(inserts)-1, index.Count())
			}
			return false
		})

	time.Sleep(100 * time.Millisecond)

	// check with old snapshot
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	}
	nd = nil
	rc = snapshot.Get(
		key,
		func(_ api.Index, _ int64, _, x api.Node) bool {
			nd = x
			return true
		})
	if rc == false {
		t.Errorf("missing key %v", string(key))
	} else if bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %q, got %q", string(value), string(nd.Value()))
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// and then with new snapshot
	if rc := snapshot.Get(key, nil); rc == true {
		t.Errorf("expected false")
	}

	// delete-max
	countref, key, value = llrb.Count(), nil, nil
	llrb.DeleteMax(
		func(index api.Index, _ int64, _, nd api.Node) bool {
			key, value = nd.Key(), nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(key, []byte("key5")) != 0 {
				t.Errorf(fmsg, "key5", string(key))
			} else if bytes.Compare(value, []byte("value5")) != 0 {
				t.Errorf(fmsg, "value5", string(value))
			} else if index.Count() != int64(len(inserts)-2) {
				t.Errorf(fmsg, len(inserts)-2, index.Count())
			}
			return false
		})

	time.Sleep(100 * time.Millisecond)

	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	}
	nd = nil
	rc = snapshot.Get(
		key,
		func(_ api.Index, _ int64, _, x api.Node) bool {
			nd = x
			return true
		})
	if rc == false {
		t.Errorf("missing key %v", string(key))
	} else if bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	if rc := snapshot.Get(key, nil); rc == true {
		t.Errorf("expected false")
	}

	// delete
	countref, key, value = llrb.Count(), []byte("key2"), nil
	llrb.Delete(
		key,
		func(index api.Index, _ int64, _, nd api.Node) bool {
			value = nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(value, []byte("value2")) != 0 {
				t.Errorf(fmsg, "value2", string(value))
			} else if index.Count() != int64(len(inserts)-3) {
				t.Errorf(fmsg, len(inserts)-3, index.Count())
			}
			return false
		})

	time.Sleep(100 * time.Millisecond)

	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	}
	nd = nil
	rc = snapshot.Get(
		key,
		func(_ api.Index, _ int64, _, x api.Node) bool {
			nd = x
			return true
		})
	if rc == false {
		t.Errorf("missing key %v", string(key))
	} else if bytes.Compare(nd.Value(), value) != 0 {
		t.Errorf("expected %v, got %v", value, nd.Value())
	}

	snapshot.Release()

	// snapshot
	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	if rc := snapshot.Get(key, nil); rc == true {
		t.Errorf("expected false")
	}
	snapshot.Release()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccBasicRange(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccrange", inserts, setts)
	writer := llrb.mvcc.writer

	// snapshot
	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	testcases := [][]interface{}{
		[]interface{}{nil, nil, "none", inserts[:]},
		[]interface{}{nil, nil, "low", inserts[:]},
		[]interface{}{nil, nil, "high", inserts[:]},
		[]interface{}{nil, nil, "both", inserts[:]},
		[]interface{}{inserts[0][0], nil, "none", inserts[1:]},
		[]interface{}{inserts[0][0], nil, "low", inserts[0:]},
		[]interface{}{inserts[0][0], nil, "high", inserts[1:]},
		[]interface{}{inserts[0][0], nil, "both", inserts[0:]},
		[]interface{}{nil, inserts[4][0], "none", inserts[:4]},
		[]interface{}{nil, inserts[4][0], "low", inserts[:4]},
		[]interface{}{nil, inserts[4][0], "high", inserts[:5]},
		[]interface{}{nil, inserts[4][0], "both", inserts[:5]},
		[]interface{}{inserts[0][0], inserts[4][0], "none", inserts[1:4]},
		[]interface{}{inserts[0][0], inserts[4][0], "low", inserts[0:4]},
		[]interface{}{inserts[0][0], inserts[4][0], "high", inserts[1:5]},
		[]interface{}{inserts[0][0], inserts[4][0], "both", inserts[0:5]},
		[]interface{}{inserts[0][0], inserts[0][0], "none", inserts[:0]},
		[]interface{}{inserts[0][0], inserts[0][0], "low", inserts[:1]},
		[]interface{}{inserts[0][0], inserts[0][0], "high", inserts[:1]},
		[]interface{}{inserts[0][0], inserts[0][0], "both", inserts[:1]},
	}

	var lowkey, highkey []byte
	for casenum, tcase := range testcases {
		lowkey, highkey = nil, nil
		incl := tcase[2].(string)
		if tcase[0] != nil {
			lowkey = tcase[0].([]byte)
		}
		if tcase[1] != nil {
			highkey = tcase[1].([]byte)
		}

		// forward range, return true
		outs := make([][2][]byte, 0)
		snapshot.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		if reflect.DeepEqual(outs, tcase[3]) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// forward range, return false
		outs = make([][2][]byte, 0)
		snapshot.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return false
			})
		ref := tcase[3].([][2][]byte)
		if len(ref) > 0 {
			ref = ref[:1]
		}
		if reflect.DeepEqual(outs, ref) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
	}

	snapshot.Release()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialMvccRange(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	llrb := makellrbmvcc(t, "mvccrange", nil, setts)
	writer := llrb.mvcc.writer

	d := dict.NewDict()

	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 10), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) bool {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
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
	repeat := 100
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]
		lowkey, highkey = lowkey[:len(lowkey)/2], highkey[:len(highkey)/2]

		// foward range
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		snapshot.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				return true
			})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
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
				if bytes.Compare(llrbvs[j], dvs[j]) != 0 {
					t.Errorf("expected %v, got %v", llrbvs[j], dvs[j])
				}
			}
		}

		// backward range
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		snapshot.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				return true
			})
		dks, dvs = make([][]byte, 0), make([][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
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
				if bytes.Compare(llrbvs[j], dvs[j]) != 0 {
					t.Errorf("expected %v, got %v", llrbvs[j], dvs[j])
				}
			}
		}
	}

	snapshot.Release()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccRange(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	llrb := makellrbmvcc(t, "mvccrange", nil, setts)
	writer := llrb.mvcc.writer

	d := dict.NewDict()

	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 10), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) bool {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
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
	repeat := 100
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]

		// foward range
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		snapshot.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				return true
			})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
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
				if bytes.Compare(llrbvs[j], dvs[j]) != 0 {
					t.Errorf("expected %v, got %v", llrbvs[j], dvs[j])
				}
			}
		}

		// backward range
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		snapshot.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				return true
			})
		dks, dvs = make([][]byte, 0), make([][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node) bool {
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
				if bytes.Compare(llrbvs[j], dvs[j]) != 0 {
					t.Errorf("expected %v, got %v", llrbvs[j], dvs[j])
				}
			}
		}
	}

	snapshot.Release()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccBasicIterate(t *testing.T) {
	// inserts
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrbmvcc(t, "bmvccrange", inserts, setts)
	writer := llrb.mvcc.writer

	// snapshot
	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	testcases := [][]interface{}{
		[]interface{}{nil, nil, "none", inserts[:]},
		[]interface{}{nil, nil, "low", inserts[:]},
		[]interface{}{nil, nil, "high", inserts[:]},
		[]interface{}{nil, nil, "both", inserts[:]},
		[]interface{}{inserts[0][0], nil, "none", inserts[1:]},
		[]interface{}{inserts[0][0], nil, "low", inserts[0:]},
		[]interface{}{inserts[0][0], nil, "high", inserts[1:]},
		[]interface{}{inserts[0][0], nil, "both", inserts[0:]},
		[]interface{}{nil, inserts[4][0], "none", inserts[:4]},
		[]interface{}{nil, inserts[4][0], "low", inserts[:4]},
		[]interface{}{nil, inserts[4][0], "high", inserts[:5]},
		[]interface{}{nil, inserts[4][0], "both", inserts[:5]},
		[]interface{}{inserts[0][0], inserts[4][0], "none", inserts[1:4]},
		[]interface{}{inserts[0][0], inserts[4][0], "low", inserts[0:4]},
		[]interface{}{inserts[0][0], inserts[4][0], "high", inserts[1:5]},
		[]interface{}{inserts[0][0], inserts[4][0], "both", inserts[0:5]},
		[]interface{}{inserts[0][0], inserts[0][0], "none", inserts[:0]},
		[]interface{}{inserts[0][0], inserts[0][0], "low", inserts[:1]},
		[]interface{}{inserts[0][0], inserts[0][0], "high", inserts[:1]},
		[]interface{}{inserts[0][0], inserts[0][0], "both", inserts[:1]},
	}

	var lowkey, highkey []byte
	for casenum, tcase := range testcases {
		lowkey, highkey = nil, nil
		incl := tcase[2].(string)
		if tcase[0] != nil {
			lowkey = tcase[0].([]byte)
		}
		if tcase[1] != nil {
			highkey = tcase[1].([]byte)
		}

		// forward iterate, return true
		refs, outs := tcase[3].([][2][]byte), make([][2][]byte, 0)
		iter := snapshot.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				nd = iter.Next()
			}
			iter.Close()
		}
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Fatalf(fmsg, casenum, string(lowkey), string(highkey))
		}

		// backward range, return true
		reverse := func(keys [][2][]byte) [][2][]byte {
			revkeys := make([][2][]byte, 0)
			for i := len(keys) - 1; i >= 0; i-- {
				revkeys = append(revkeys, keys[i])
			}
			return revkeys
		}

		refs = reverse(tcase[3].([][2][]byte))
		outs = make([][2][]byte, 0)
		iter = snapshot.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				nd = iter.Next()
			}
			iter.Close()
		}
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Fatalf(fmsg, casenum, string(lowkey), string(highkey))
		}
	}

	snapshot.Release()
	llrb.SetMemratio(0.04)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialMvccIterate(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	llrb := makellrbmvcc(t, "mvcciterate", nil, setts)
	writer := llrb.mvcc.writer

	d := dict.NewDict()

	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 10), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) bool {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
			})
		d.Upsert(key, value, nil)
		keys, values = append(keys, key), append(values, value)
		seqno++
	}

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// random iterate
	repeat := 100
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]
		lowkey, highkey = lowkey[:len(lowkey)/2], highkey[:len(highkey)/2]

		// forward iterate
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		iter := snapshot.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		iter = d.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		if len(dks) != len(llrbks) {
			t.Fatalf("expected %v, got %v", len(dks), len(llrbks))
		}
		for j, dk := range dks {
			if bytes.Compare(dk, llrbks[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[j]))
			}
			if bytes.Compare(dvs[j], llrbvs[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[j]), string(llrbvs[j]))
			}
		}

		// backward iterate
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		iter = snapshot.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		dks, dvs = make([][]byte, 0), make([][]byte, 0)
		iter = snapshot.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		if len(dks) != len(llrbks) {
			t.Fatalf("expected %v, got %v", len(dks), len(llrbks))
		}
		for j, dk := range dks {
			if bytes.Compare(dk, llrbks[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[j]))
			}
			if bytes.Compare(dvs[j], llrbvs[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[j]), string(llrbvs[j]))
			}
		}
	}

	snapshot.Release()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccIterate(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	llrb := makellrbmvcc(t, "mvcciterate", nil, setts)
	writer := llrb.mvcc.writer

	d := dict.NewDict()

	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) bool {
				if oldnd != nil {
					t.Errorf("expected nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
			})
		d.Upsert(key, value, nil)
		keys, values = append(keys, key), append(values, value)
		seqno++
	}

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// random iterate
	repeat := 100
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]

		// forward iterate
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		iter := snapshot.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		iter = d.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		if len(dks) != len(llrbks) {
			t.Fatalf("expected %v, got %v", len(dks), len(llrbks))
		}
		for j, dk := range dks {
			if bytes.Compare(dk, llrbks[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[j]))
			}
			if bytes.Compare(dvs[j], llrbvs[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[j]), string(llrbvs[j]))
			}
		}

		// backward iterate
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		iter = snapshot.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		dks, dvs = make([][]byte, 0), make([][]byte, 0)
		iter = snapshot.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			nd := iter.Next()
			for nd != nil {
				dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
				nd = iter.Next()
			}
			iter.Close()
		}
		if len(dks) != len(llrbks) {
			t.Fatalf("expected %v, got %v", len(dks), len(llrbks))
		}
		for j, dk := range dks {
			if bytes.Compare(dk, llrbks[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[j]))
			}
			if bytes.Compare(dvs[j], llrbvs[j]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[j]), string(llrbvs[j]))
			}
		}
	}

	snapshot.Release()
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccInsert(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	inserts := make([][2][]byte, 0)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccinsert", inserts, setts)
	writer := llrb.mvcc.writer

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}

	// check memory accounting
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}
	// TODO: useful count has increased.
	//useful := int64(2096640)
	//if x := stats["node.useful"].(int64); x != useful {
	//	t.Errorf("expected %v, got %v", useful, x)
	//}
	allocated, avail := int64(960000), int64(1072781824)
	if x := stats["node.allocated"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	}
	if x := stats["node.available"].(int64); x != avail {
		t.Errorf("expected %v, got %v", avail, x)
	}
	useful := int64(8388608)
	allocated, avail = int64(1280000), int64(1099510347776)
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
	var nd api.Node
	for i, key := range keys {
		nd = nil
		rc := snapshot.Get(
			key,
			func(_ api.Index, _ int64, _, x api.Node) bool {
				nd = x
				return true
			})
		if rc == false {
			t.Fatalf("missing key %v", string(key))
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccUpsert(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	inserts := make([][2][]byte, 0)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccupsert", inserts, setts)
	writer := llrb.mvcc.writer

	snapshot, err := validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// upsert same items
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	var newvalues [][]byte
	for i, key := range keys {
		_, value := makekeyvalue(nil, make([]byte, 200))
		newvalues = append(newvalues, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) bool {
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
				return false
			})
		seqno++
	}

	snapshot, err = validatesnapshot(100 /*mS*/, writer)
	if err != nil {
		t.Error(err)
	}
	snapshot.Release()

	// check memory accounting
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}
	//TODO: useful count has increased.
	//if useful := stats["node.useful"].(int64); useful != 2096640 {
	//	t.Errorf("expected %v, got %v", 2096640, useful)
	//}
	if useful := stats["value.useful"].(int64); useful != 18873600 {
		t.Errorf("expected %v, got %v", 18873600, useful)
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccDeleteMin(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.fpos"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	var inserts [][2][]byte
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 1 item
	count := 1
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccdelete", inserts, setts)

	// delete first item
	vbno, vbuuid := uint16(10), uint64(0xABCD)
	llrb.DeleteMin(
		func(index api.Index, _ int64, _, nd api.Node) bool {
			if nd == nil {
				t.Errorf("unexpected nil")
			} else if x := nd.Vbno(); x != vbno {
				t.Errorf("expected %v, got %v", vbno, x)
			} else if x := nd.Vbuuid(); x != vbuuid {
				t.Errorf("expected %v, got %v", vbuuid, x)
			}
			return false
		})

	if x := llrb.Count(); x > 0 {
		t.Errorf("expected 0, got %v", x)
	}
	// check memory accounting
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}
	if x, y := int64(0), stats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBMvccDelete(t *testing.T) {
	setts := DefaultSettings()
	setts["mvcc.enable"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true

	var inserts [][2][]byte
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := makekeyvalue(make([]byte, 10), make([]byte, 100))
		inserts = append(inserts, [2][]byte{key, value})
		keys, values = append(keys, key), append(values, value)
	}

	llrb := makellrbmvcc(t, "mvccdelete", inserts, setts)
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
			func(index api.Index, _ int64, _, nd api.Node) bool {
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
				return false
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
			func(index api.Index, _ int64, _, nd api.Node) bool {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
				return false
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
			func(index api.Index, _ int64, _, nd api.Node) bool {
				if nd == nil {
					t.Errorf("unexpected nil")
				} else if x := nd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
				return false
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
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}
	// TODO useful count has increased
	//if useful := stats["node.useful"].(int64); useful != 2096640 {
	//	t.Errorf("expected %v, got %v", 2096640, useful)
	//}
	if x, y := int64(0), stats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(1073741824), stats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if useful := stats["value.useful"].(int64); useful != 8388608 {
		t.Errorf("expected %v, got %v", 8388608, useful)
	} else if x, y = int64(0), stats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(1099511627776), stats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func makellrbmvcc(
	t *testing.T, nm string, inserts [][2][]byte, setts lib.Settings) *LLRB {

	llrb := NewLLRB(nm, setts)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(0x12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	for _, kv := range inserts {
		keys = append(keys, kv[0])
		values = append(values, kv[1])
	}
	llrb.UpsertMany(
		keys, values,
		func(index api.Index, i int64, newnd, oldnd api.Node) bool {
			if oldnd != nil {
				t.Errorf("expected old Llrbnode as nil")
			}
			newnd.Setvbno(vbno).SetVbuuid(vbuuid)
			newnd.SetBornseqno(seqno + uint64(i))
			llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
			llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno + uint64(i)})
			return true
		})
	return llrb
}

func validatesnapshot(sleep int, writer *LLRBWriter) (api.IndexSnapshot, error) {
	time.Sleep(time.Duration(sleep) * time.Millisecond)

	snapch := make(chan api.IndexSnapshot, 1)
	err := writer.llrb.RSnapshot(snapch)
	if err != nil {
		return nil, err
	}
	snapshot := <-snapch

	// validate
	snapshot.Validate()
	return snapshot, nil
}
