package llrb

import "testing"
import "fmt"
import "unsafe"
import "reflect"
import "time"
import "math/rand"
import "bytes"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/dict"
import "github.com/prataprc/storage.go/lib"

var _ = fmt.Sprintf("dummy")

func TestNewLLRB(t *testing.T) {
	setts := DefaultSettings()

	llrb := NewLLRB("test", setts)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}

	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	ovrhd, used, allc := int64(616), int64(0), int64(0)
	nodavail := setts.Int64("nodearena.capacity")
	if x := stats["node.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := stats["node.useful"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := stats["node.allocated"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
	} else if x, y := nodavail, stats["node.available"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	ovrhd, used, allc = int64(1128), int64(0), int64(0)
	valavail := setts.Int64("valarena.capacity")
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestNewLLRBNode(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = true
	setts["metadata.mvalue"] = true
	setts["metadata.fpos"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	vbno, vbuuid := uint16(10), uint64(0xABCD)
	bnseqno, ddseqno := uint64(12345678), uint64(12345690)
	nd := llrb.newnode(key, value)
	fmask := metadataMask(0).enableMvalue().enableVbuuid().enableFpos()
	fmask = fmask.enableBornSeqno().enableDeadSeqno()
	nd.metadata().initMetadata(vbno, fmask).setvbuuid(vbuuid)
	nd.metadata().setbnseq(bnseqno).setddseq(ddseqno)

	if k := nd.key(llrb.mdsize); bytes.Compare(k, key) != 0 {
		t.Errorf("expected %v, got %v", key, nd.key(llrb.mdsize))
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestNewLLRBNodePanic(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = false
	llrb := NewLLRB("test", setts)
	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	func() {
		defer func() {
			if recover() == nil {
				t.Errorf("expected panic")
			}
		}()
		llrb.newnode(key, value)
	}()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestCloneLLRBNode(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	llrb := NewLLRB("test", setts)

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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBBasicLookup(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrb(t, "basiclookup", inserts, setts)

	// get
	if llrb.Has(inserts[1][0]) == false {
		t.Errorf("expected key - %v", string(inserts[0][0]))
	}

	var nd api.Node
	rc := llrb.Get(inserts[2][0], func(x api.Node) bool {
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missingkey %v", string(inserts[2][0]))
	} else if v := nd.Value(); bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}

	if rc := llrb.Get([]byte("key10"), nil); rc == true {
		t.Errorf("expected nil when Get() on missing key")
	}

	// min
	nd = nil
	rc = llrb.Min(func(x api.Node) bool {
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missing minimum key")
	} else if k, v := nd.Key(), nd.Value(); bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}

	// max
	nd = nil
	rc = llrb.Max(func(x api.Node) bool {
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missing maximum key")
	} else if k, v := nd.Key(), nd.Value(); bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBBasicUpdates(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrb(t, "basicupdates", inserts, setts)

	// update
	newvalue := []byte("value11")
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(1234567890)
	countref := llrb.Count()
	llrb.Upsert(
		inserts[0][0], newvalue,
		func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
	}
	var nd api.Node
	rc := llrb.Get(inserts[0][0], func(x api.Node) bool {
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("expected valid node")
	} else if bytes.Compare(newvalue, nd.Value()) != 0 {
		t.Errorf("expected %v, got %v", newvalue, nd.Value())
	}

	llrb.SetMemratio(0.04)
	llrb.Validate()

	// delete
	countref, key, value := llrb.Count(), []byte(nil), []byte(nil)
	llrb.DeleteMin(
		func(index api.Index, nd api.Node) {
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
	} else if rc := llrb.Get(key, nil); rc == true {
		t.Errorf("expected missing key")
	}

	llrb.Validate()

	// delete-max
	countref, key, value = llrb.Count(), nil, nil
	llrb.DeleteMax(
		func(index api.Index, nd api.Node) {
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
	} else if rc := llrb.Get(key, nil); rc == true {
		t.Errorf("expeced missing key")
	}

	llrb.Validate()

	// delete-min
	countref, key, value = llrb.Count(), nil, nil
	llrb.Delete(
		[]byte("key2"),
		func(index api.Index, nd api.Node) {
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
	} else if rc := llrb.Get(key, nil); rc == true {
		t.Errorf("expected missing key")
	}

	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBBasicRange(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.fpos"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

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
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		seqno++
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
		refs := tcase[3].([][2][]byte)

		// forward range, return true
		outs := make([][2][]byte, 0)
		llrb.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// forward range, return false
		outs = make([][2][]byte, 0)
		llrb.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			return false
		})
		if len(refs) > 0 {
			refs = refs[:1]
		}
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}

		reverse := func(keys [][2][]byte) [][2][]byte {
			revkeys := make([][2][]byte, 0)
			for i := len(keys) - 1; i >= 0; i-- {
				revkeys = append(revkeys, keys[i])
			}
			return revkeys
		}

		// backward range, return true
		refs = reverse(tcase[3].([][2][]byte))
		outs = make([][2][]byte, 0)
		llrb.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// backward range, return false
		outs = make([][2][]byte, 0)
		llrb.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			return false
		})
		if len(refs) > 0 {
			refs = refs[:1]
		}
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
	}

	llrb.SetMemratio(0.04)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialRange(t *testing.T) {
	d := dict.NewDict()

	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}

	// inserts
	inserts, n, keys := make([][2][]byte, 0), 100000, []string{}
	for i := 0; i < n; i += 100 {
		key, value := fmt.Sprintf("%v", i), fmt.Sprintf("value%v", i)
		keys = append(keys, key)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}

	for _, kv := range inserts {
		llrb.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
	}

	// forward range
	incls := []string{"none", "low", "high", "both"}
	for i := int64(0); i < llrb.Count()-1; i = i + 100 {
		for j := int64(i); j < llrb.Count(); j = j + 100 {
			for _, incl := range incls {
				refkeys, outkeys := []string{}, []string{}
				lkey, hkey := []byte(keys[i]), []byte(keys[j])
				lkey, hkey = lkey[:len(lkey)/2], hkey[:len(hkey)/2]
				d.Range(lkey, hkey, incl, false, func(nd api.Node) bool {
					refkeys = append(refkeys, string(nd.Key()))
					return true
				})
				llrb.Range(lkey, hkey, incl, false, func(nd api.Node) bool {
					outkeys = append(outkeys, string(nd.Key()))
					return true
				})
				lks, hks := string(lkey), string(hkey)
				if !reflect.DeepEqual(refkeys, outkeys) {
					t.Fatalf("failed for %v %v %v", lks, hks, incl)
				}
			}
		}
	}

	// backward range
	for i := int64(0); i < llrb.Count()-1; i = i + 100 {
		for j := int64(i); j < llrb.Count(); j = j + 100 {
			for _, incl := range incls {
				refkeys, outkeys := []string{}, []string{}
				lkey, hkey := []byte(keys[i]), []byte(keys[j])
				lkey, hkey = lkey[:len(lkey)/2], hkey[:len(hkey)/2]
				d.Range(lkey, hkey, incl, true, func(nd api.Node) bool {
					refkeys = append(refkeys, string(nd.Key()))
					return true
				})
				llrb.Range(lkey, hkey, incl, true, func(nd api.Node) bool {
					outkeys = append(outkeys, string(nd.Key()))
					return true
				})
				lks, hks := string(lkey), string(hkey)
				if !reflect.DeepEqual(refkeys, outkeys) {
					t.Fatalf("failed for %v %v %v", lks, hks, incl)
				}
			}
		}
	}
}

func TestLLRBRange(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
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
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
	repeat := 100
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]

		// forward range
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		llrb.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			llrbks = append(llrbks, nd.Key())
			llrbvs = append(llrbvs, nd.Value())
			return true
		})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
			return true
		})

		if len(dks) != len(llrbks) {
			t.Fatalf("expected %v, got %v", len(dks), len(llrbks))
		}
		for i, dk := range dks {
			if bytes.Compare(dk, llrbks[i]) != 0 {
				t.Fatalf("expected %v, got %v", string(dk), string(llrbks[i]))
			}
			if bytes.Compare(dvs[i], llrbvs[i]) != 0 {
				t.Fatalf("expected %v, got %v", string(dvs[i]), string(llrbvs[i]))
			}
		}

		// backward range
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		llrb.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
			llrbks = append(llrbks, nd.Key())
			llrbvs = append(llrbvs, nd.Value())
			return true
		})
		dks, dvs = make([][]byte, 0), make([][]byte, 0)
		d.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
			dks, dvs = append(dks, nd.Key()), append(dvs, nd.Value())
			return true
		})
		if len(dks) != len(llrbks) {
			t.Fatalf("expected %v, got %v", len(dks), len(llrbks))
		}
		for i, dk := range dks {
			if bytes.Compare(dk, llrbks[i]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[i]))
			}
			if bytes.Compare(dvs[i], llrbvs[i]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[i]), string(llrbvs[i]))
			}
		}
	}

	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBIteratePool(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["iterpool.size"] = 1
	llrb := NewLLRB("test", setts)

	// seed the pool
	iter1 := llrb.Iterate(nil, nil, "both", false)
	iter2 := llrb.Iterate(nil, nil, "both", false)
	if len(llrb.iterpool) != 0 {
		t.Fatalf("unexpected iterpool size %v", len(llrb.iterpool))
	}

	iter1.Close()
	iter2.Close()
	if len(llrb.iterpool) != 1 {
		t.Fatalf("unexpected iterpool size %v", len(llrb.iterpool))
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBBasicIterate(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

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
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
				if oldnd != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				newnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			})
		seqno++
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
		refs := tcase[3].([][2][]byte)

		// forward range, return true
		outs := make([][2][]byte, 0)
		iter := llrb.Iterate(lowkey, highkey, incl, false)
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
		iter = llrb.Iterate(lowkey, highkey, incl, true)
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

	llrb.SetMemratio(0.04)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialIterate(t *testing.T) {
	d := dict.NewDict()

	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}

	// inserts
	inserts, n, keys := make([][2][]byte, 0), 100000, []string{}
	for i := 0; i < n; i += 100 {
		key, value := fmt.Sprintf("%v", i), fmt.Sprintf("value%v", i)
		keys = append(keys, key)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}

	for _, kv := range inserts {
		llrb.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
	}

	// forward iterate
	incls := []string{"none", "low", "high", "both"}
	for i := int64(0); i < llrb.Count()-1; i = i + 100 {
		for j := int64(i + 1); j < llrb.Count(); j = j + 100 {
			for _, incl := range incls {
				refkeys, outkeys := []string{}, []string{}
				lkey, hkey := []byte(keys[i]), []byte(keys[j])
				lkey, hkey = lkey[:len(lkey)/2], hkey[:len(hkey)/2]
				if iter := d.Iterate(lkey, hkey, incl, false); iter == nil {
					continue
				} else {
					for nd := iter.Next(); nd != nil; nd = iter.Next() {
						refkeys = append(refkeys, string(nd.Key()))
					}
					iter = llrb.Iterate(lkey, hkey, incl, false)
					for nd := iter.Next(); nd != nil; nd = iter.Next() {
						outkeys = append(outkeys, string(nd.Key()))
					}
					lks, hks := string(lkey), string(hkey)
					if !reflect.DeepEqual(refkeys, outkeys) {
						t.Fatalf("failed for %v %v %v", lks, hks, incl)
					}
				}
			}
		}
	}

	// backward iterate
	for i := int64(0); i < llrb.Count()-1; i = i + 100 {
		for j := int64(i + 1); j < llrb.Count(); j = j + 100 {
			for _, incl := range incls {
				refkeys, outkeys := []string{}, []string{}
				lkey, hkey := []byte(keys[i]), []byte(keys[j])
				lkey, hkey = lkey[:len(lkey)/2], hkey[:len(hkey)/2]
				if iter := d.Iterate(lkey, hkey, incl, true); iter == nil {
					continue
				} else {
					for nd := iter.Next(); nd != nil; nd = iter.Next() {
						refkeys = append(refkeys, string(nd.Key()))
					}
					iter = llrb.Iterate(lkey, hkey, incl, true)
					for nd := iter.Next(); nd != nil; nd = iter.Next() {
						outkeys = append(outkeys, string(nd.Key()))
					}
					lks, hks := string(lkey), string(hkey)
					if !reflect.DeepEqual(refkeys, outkeys) {
						t.Fatalf("failed for %v %v %v", lks, hks, incl)
					}
				}
			}
		}
	}
}

func TestLLRBIterate(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
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
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
	repeat := 100
	incls := []string{"both", "low", "high", "none"}
	for i := 0; i < repeat; i++ {
		incl := incls[rand.Intn(len(incls))]
		x := rand.Intn(len(keys))
		y := rand.Intn(len(keys))
		lowkey, highkey := keys[x], keys[y]

		// forward range
		llrbks, llrbvs := make([][]byte, 0), make([][]byte, 0)
		iter := llrb.Iterate(lowkey, highkey, incl, false)
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
		for i, dk := range dks {
			if bytes.Compare(dk, llrbks[i]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[i]))
			}
			if bytes.Compare(dvs[i], llrbvs[i]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[i]), string(llrbvs[i]))
			}
		}

		// backward range
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		iter = llrb.Iterate(lowkey, highkey, incl, true)
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
		iter = llrb.Iterate(lowkey, highkey, incl, true)
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
		for i, dk := range dks {
			if bytes.Compare(dk, llrbks[i]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dk), string(llrbks[i]))
			}
			if bytes.Compare(dvs[i], llrbvs[i]) != 0 {
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[i]), string(llrbvs[i]))
			}
		}
	}

	llrb.SetMemratio(0.04)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBInsert(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = false
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	useful := int64(2096640)
	allocated, avail := int64(1920000), int64(1071821824)
	if x := stats["node.useful"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	}
	if x := stats["node.allocated"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	}
	if x := stats["node.available"].(int64); x != avail {
		t.Errorf("expected %v, got %v", avail, x)
	}
	useful = int64(8388608)
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
	if x, y := int64(1000000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := int64(1000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	// Get items
	var nd api.Node
	vbno, vbuuid, seqno = uint16(10), uint64(0xABCD), uint64(12345678)
	for i, key := range keys {
		nd = nil
		rc := llrb.Get(key, func(x api.Node) bool {
			nd = x
			return true
		})
		if rc == false {
			t.Errorf("missing key")
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBUpsert(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = false
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
	var newvalues [][]byte
	for i, key := range keys {
		value := make([]byte, 200)
		_, value = makekeyvalue(nil, value)
		newvalues = append(newvalues, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	if useful := stats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	}
	if useful := stats["value.useful"].(int64); useful != 18873600 {
		t.Errorf("expected %v, got %v", 18873600, useful)
	}
	x, y := int64(1920000), stats["node.allocated"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(2240000), stats["value.allocated"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(1071821824), stats["node.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(1099509387776), stats["value.available"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestLLRBDelete(t *testing.T) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, newnd, oldnd api.Node) {
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
			func(index api.Index, nd api.Node) {
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
			func(index api.Index, nd api.Node) {
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
			func(index api.Index, nd api.Node) {
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
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	if useful := stats["node.useful"].(int64); useful != 2096640 {
		t.Errorf("expected %v, got %v", 2096640, useful)
	}
	if useful := stats["value.useful"].(int64); useful != 8388608 {
		t.Errorf("expected %v, got %v", 8388608, useful)
	} else if x, y := int64(0), stats["node.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), stats["value.allocated"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y := int64(1073741824), stats["node.available"].(int64)
	if x != y {
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

func BenchmarkLLRBCloneKey(b *testing.B) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	b.ResetTimer()

	key, value := makekeyvalue(make([]byte, 256), make([]byte, 0))
	nd := llrb.newnode(key, value)
	for i := 0; i < b.N; i++ {
		llrb.clone(nd)
	}
}

func BenchmarkLLRBCloneSmall(b *testing.B) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	b.ResetTimer()

	key, value := makekeyvalue(make([]byte, 64), make([]byte, 1024))
	nd := llrb.newnode(key, value)
	for i := 0; i < b.N; i++ {
		llrb.clone(nd)
	}
}

func BenchmarkLLRBCloneLarge(b *testing.B) {
	setts := DefaultSettings()
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	b.ResetTimer()

	key, value := makekeyvalue(make([]byte, 512), make([]byte, 1024*1000))
	nd := llrb.newnode(key, value)
	for i := 0; i < b.N; i++ {
		llrb.clone(nd)
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

func makellrb(
	t *testing.T, nm string, inserts [][2][]byte, setts lib.Settings) *LLRB {

	llrb := NewLLRB(nm, setts)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	for _, kv := range inserts {
		keys = append(keys, kv[0])
		values = append(values, kv[1])
	}
	llrb.UpsertMany(
		keys, values,
		func(index api.Index, i int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected old Llrbnode as nil")
			}
			llrb := index.(*LLRB)
			newnd.Setvbno(vbno).SetVbuuid(vbuuid)
			newnd.SetBornseqno(seqno + uint64(i))
			llrb.clock.updatevbuuids([]uint16{vbno}, []uint64{vbuuid})
			llrb.clock.updateseqnos([]uint16{vbno}, []uint64{seqno + uint64(i)})
		})
	return llrb
}
