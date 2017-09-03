package llrb

import "testing"
import "fmt"
import "unsafe"
import "reflect"
import "time"
import "math/rand"
import "bytes"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/dict"
import s "github.com/prataprc/gosettings"

var _ = fmt.Sprintf("dummy")

func TestNewLLRB(t *testing.T) {
	setts := testsetts(Defaultsettings())

	llrb := NewLLRB("test", setts)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}

	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	ovrhd, used, allc := int64(12064), int64(0), int64(0)
	keycapacity := setts.Int64("keycapacity")
	if x := stats["node.capacity"].(int64); x != keycapacity {
		t.Errorf("expected %v, got %v", keycapacity, x)
	} else if x := stats["node.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := stats["node.heap"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := stats["node.alloc"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
	}

	ovrhd, used, allc = int64(12064), int64(0), int64(0)
	valcapacity := setts.Int64("valcapacity")
	if x := stats["value.capacity"].(int64); x != valcapacity {
		t.Errorf("expected %v, got %v", valcapacity, x)
	} else if x := stats["value.overhead"].(int64); x != ovrhd {
		t.Errorf("expected %v, got %v", ovrhd, x)
	} else if x := stats["value.heap"].(int64); x != used {
		t.Errorf("expected %v, got %v", used, x)
	} else if x := stats["value.alloc"].(int64); x != allc {
		t.Errorf("expected %v, got %v", allc, x)
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

func TestNewNode(t *testing.T) {
	setts := testsetts(Defaultsettings())
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

func TestNewNodePanic(t *testing.T) {
	setts := testsetts(Defaultsettings())
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

func TestCloneNode(t *testing.T) {
	setts := testsetts(Defaultsettings())
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
	newnd := llrb.clonenode(nd)
	if llrb.equivalent(nd, newnd) == false {
		t.Errorf("expected %v, got %v", nd.repr(), newnd.repr())
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestBasicLookup(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := makellrb(t, "basiclookup", inserts, setts)

	// get
	if llrb.Has(inserts[1][0]) == false {
		t.Errorf("expected key - %v", string(inserts[0][0]))
	}

	var nd api.Node
	rc := llrb.Get(
		inserts[2][0],
		func(_ api.Index, _ int64, _, x api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
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

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestBasicUpdates(t *testing.T) {
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}

	setts := testsetts(Defaultsettings())
	setts["keycapacity"], setts["valcapacity"] = 1024, 1024
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
		func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
			vs := ond.Value()
			if bytes.Compare(vs, inserts[0][1]) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(inserts[0][1]), string(vs))
			}
			vs = nnd.Value()
			if bytes.Compare(vs, newvalue) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(newvalue), string(vs))
			}
			return false
		})
	// check
	if countref != llrb.Count() {
		t.Errorf("expected %v, got %v", countref, llrb.Count())
	}
	var nd api.Node
	rc := llrb.Get(
		inserts[0][0],
		func(_ api.Index, _ int64, _, x api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			nd = x
			return true
		})
	if rc == false {
		t.Errorf("expected valid node")
	} else if bytes.Compare(newvalue, nd.Value()) != 0 {
		t.Errorf("expected %v, got %v", newvalue, nd.Value())
	}

	llrb.ExpectedUtilization(0.019)
	llrb.Validate()

	countref, key := llrb.Count(), []byte(nil)
	llrb.Delete(
		[]byte("key2"),
		func(index api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			v := nd.Value()
			fmsg := "expected %v, got %v"
			if bytes.Compare(v, []byte("value2")) != 0 {
				t.Errorf(fmsg, "value2", string(v))
			} else if index.Count() != int64(len(inserts)-1) {
				t.Errorf(fmsg, len(inserts)-1, index.Count())
			}
			return false
		})
	// check
	if countref-1 != llrb.Count() {
		t.Errorf("expected %v, got %v", countref-1, llrb.Count())
	} else if rc := llrb.Get(key, nil); rc == true {
		t.Errorf("expected missing key")
	}

	llrb.ExpectedUtilization(0.009)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestBasicRange(t *testing.T) {
	setts := testsetts(Defaultsettings())
	setts["keycapacity"], setts["valcapacity"] = 1024, 1024
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
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
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
		llrb.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// forward range, return false
		outs = make([][2][]byte, 0)
		llrb.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
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
		llrb.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		if reflect.DeepEqual(outs, refs) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// backward range, return false
		outs = make([][2][]byte, 0)
		llrb.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
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

	llrb.ExpectedUtilization(0.02)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialRange(t *testing.T) {
	d := dict.NewDict("testdict")

	setts := testsetts(Defaultsettings())
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
		llrb.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				}
				return false
			})
		d.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				}
				return false
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
				d.Range(
					lkey, hkey, incl, false,
					func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						}
						refkeys = append(refkeys, string(nd.Key()))
						return true
					})
				llrb.Range(
					lkey, hkey, incl, false,
					func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						}
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
				d.Range(
					lkey, hkey, incl, true,
					func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						}
						refkeys = append(refkeys, string(nd.Key()))
						return true
					})
				llrb.Range(
					lkey, hkey, incl, true,
					func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
						if err != nil {
							t.Error(err)
						}
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

	llrb.ExpectedUtilization(0.008)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestRange(t *testing.T) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	d := dict.NewDict("testdict")
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
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
		llrb.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				return true
			})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
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
				fmsg := "expected %v, got %v"
				t.Fatalf(fmsg, string(dvs[i]), string(llrbvs[i]))
			}
		}

		// backward range
		llrbks, llrbvs = make([][]byte, 0), make([][]byte, 0)
		llrb.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				llrbks = append(llrbks, nd.Key())
				llrbvs = append(llrbvs, nd.Value())
				return true
			})
		dks, dvs = make([][]byte, 0), make([][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
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

func TestIteratePool(t *testing.T) {
	setts := testsetts(Defaultsettings())
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

func TestBasicIterate(t *testing.T) {
	setts := testsetts(Defaultsettings())
	setts["keycapacity"], setts["valcapacity"] = 1024, 1024
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
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected old Llrbnode as nil")
				}
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
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

	llrb.ExpectedUtilization(0.02)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestPartialIterate(t *testing.T) {
	d := dict.NewDict("testdict")

	setts := testsetts(Defaultsettings())
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
		llrb.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				}
				return false
			})
		d.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				}
				return false
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

func TestIterate(t *testing.T) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	d := dict.NewDict("testdict")
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
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

	llrb.ExpectedUtilization(0.03)
	llrb.Validate()

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestInsert(t *testing.T) {
	setts := testsetts(Defaultsettings())
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
		key, value := makekeyvalue(make([]byte, 100), make([]byte, 100))
		keys = append(keys, key)
		values = append(values, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected old Llrbnode to be nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
			})
		seqno++
	}

	llrb.Validate()

	// check memory accounting
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	useful := int64(1607520)
	allocated := int64(1600000)
	if x := stats["node.heap"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	}
	if x := stats["node.alloc"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
	}
	useful = int64(1286016)
	allocated = int64(1280000)
	if x := stats["value.heap"].(int64); x != useful {
		t.Errorf("expected %v, got %v", useful, x)
	}
	if x := stats["value.alloc"].(int64); x != allocated {
		t.Errorf("expected %v, got %v", allocated, x)
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
		rc := llrb.Get(
			key,
			func(_ api.Index, _ int64, _, x api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
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

func TestUpsert(t *testing.T) {
	setts := testsetts(Defaultsettings())
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
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected old Llrbnode to be nil")
				} else if x := index.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				keys, values = append(keys, key), append(values, value)
				return false
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
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond == nil {
					t.Errorf("unexpected nil")
				} else if x := nnd.Vbno(); x != vbno {
					t.Errorf("expected %v, got %v", vbno, x)
				} else if x := nnd.Vbuuid(); x != vbuuid {
					t.Errorf("expected %v, got %v", vbuuid, x)
				}
				x, y, z := values[i], ond.Value(), nnd.Value()
				if bytes.Compare(x, y) != 0 {
					fmsg := "%q expected old %s, got %s"
					t.Errorf(fmsg, string(key), string(x), string(y))
				} else if bytes.Compare(value, z) != 0 {
					fmsg := "%q expected new %s, got %s"
					t.Errorf(fmsg, string(key), string(value), string(z))
				}
				return false
			})
		seqno++
	}

	llrb.Validate()

	// check memory accounting
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	if useful := stats["node.heap"].(int64); useful != 1607520 {
		t.Errorf("expected %v, got %v", 1607520, useful)
	}
	if useful := stats["value.heap"].(int64); useful != 2250528 {
		t.Errorf("expected %v, got %v", 2250528, useful)
	}
	x, y := int64(1600000), stats["node.alloc"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	x, y = int64(2240000), stats["value.alloc"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y = int64(1000000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2000000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestTreecheck(t *testing.T) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = false
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 20 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		llrb.Upsert(
			key, value,
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				keys, values = append(keys, key), append(values, value)
				return false
			})
		seqno++
	}

	h_heightav := lib.NewhistorgramInt64(1, 256, 1)
	now := time.Now()
	n_blacks := treestats(llrb.getroot(), 1 /*depth*/, h_heightav, 0)
	fmt.Printf("HeightStats took %v for %v items\n", time.Since(now), count)
	fmt.Printf("Blacks in depth %v\n", n_blacks)

	llrb.Validate()
	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	setts := testsetts(Defaultsettings())
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
			func(index api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if ond != nil {
					t.Errorf("expected nil")
				} else if x := llrb.Count(); x != int64(i+1) {
					t.Errorf("expected %v, got %v", i, x)
				}
				keys, values = append(keys, key), append(values, value)
				nnd.Setvbno(vbno).SetVbuuid(vbuuid).SetBornseqno(seqno)
				return false
			})
		seqno++
	}

	llrb.ExpectedUtilization(0.27)
	llrb.Validate()

	// Delete items
	seqno = uint64(12345678)
	for i, key := range keys[:count/2] {
		llrb.Delete(
			key,
			func(index api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if nd == nil {
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

	llrb.ExpectedUtilization(0.13)
	llrb.Validate()

	time.Sleep(1 * time.Second)

	// check memory accounting
	stats, err := llrb.Fullstats()
	if err != nil {
		t.Error(err)
	}

	if useful := stats["node.heap"].(int64); useful != 1607360 {
		t.Errorf("expected %v, got %v", 1607360, useful)
	}
	if useful := stats["value.heap"].(int64); useful != 1285888 {
		t.Errorf("expected %v, got %v", 1285888, useful)
	} else if x, y := int64(800000), stats["node.alloc"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(640000), stats["value.alloc"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := int64(500000), stats["keymemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(500000), stats["valmemory"].(int64); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}

	if err := llrb.Destroy(); err != nil {
		t.Fatal(err)
	}
}

func TestClone(t *testing.T) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.deadseqno"] = false
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	inserts, n := make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}
	for _, kv := range inserts {
		llrb.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, newnd, oldnd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				} else if oldnd != nil {
					t.Errorf("expected nil")
				}
				return false
			})
	}

	refkeys, refvalues := make([][]byte, 0), make([][]byte, 0)
	llrb.Range(
		nil, nil, "both", false,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			key := make([]byte, len(nd.Key()))
			copy(key, nd.Key())
			refkeys = append(refkeys, key)
			value := make([]byte, len(nd.Value()))
			copy(value, nd.Value())
			refvalues = append(refvalues, value)
			return true
		})

	inserts, n = make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}
	for _, kv := range inserts {
		llrb.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, newnd, oldnd api.Node, err error) bool {
				return false
			})
	}

	newllrb, err := llrb.Clone(llrb.name + "-clone")
	if err != nil {
		t.Fatal(err)
	}

	keys, values := make([][]byte, 0), make([][]byte, 0)
	newllrb.Range(
		nil, nil, "both", false,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			key := make([]byte, len(nd.Key()))
			copy(key, nd.Key())
			keys = append(keys, key)
			value := make([]byte, len(nd.Value()))
			copy(value, nd.Value())
			values = append(values, value)
			return true
		})

	if x, y := len(refkeys), len(keys); x != y {
		t.Fatalf("expected %v, got %v", x, y)
	} else if x, y = len(refvalues), len(values); x != y {
		t.Fatalf("expected %v, got %v", x, y)
	}
	for i := 0; i < len(keys); i++ {
		if x, y := refkeys[i], keys[i]; bytes.Compare(x, y) != 0 {
			t.Fatalf("expected %v, got %v", string(x), string(y))
		} else if x, y := refvalues[i], values[i]; bytes.Compare(x, y) != 0 {
			t.Fatalf("expected %v, got %v", string(x), string(y))
		}
	}
}

func BenchmarkCloneKey(b *testing.B) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	b.ResetTimer()

	key, value := makekeyvalue(make([]byte, 256), make([]byte, 0))
	nd := llrb.newnode(key, value)
	for i := 0; i < b.N; i++ {
		llrb.clonenode(nd)
	}
}

func BenchmarkCloneSmall(b *testing.B) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	llrb := NewLLRB("test", setts)

	b.ResetTimer()

	key, value := makekeyvalue(make([]byte, 64), make([]byte, 1024))
	nd := llrb.newnode(key, value)
	for i := 0; i < b.N; i++ {
		llrb.clonenode(nd)
	}
}

func BenchmarkCloneLarge(b *testing.B) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["keycapacity"] = 1024 * 1024 * 100
	setts["valcapacity"] = 1024 * 1024 * 100
	llrb := NewLLRB("test", setts)

	b.ResetTimer()

	key, value := makekeyvalue(make([]byte, 512), make([]byte, 1024*1000))
	nd := llrb.newnode(key, value)
	for i := 0; i < b.N; i++ {
		llrb.clonenode(nd)
	}
}

func BenchmarkRange10000(b *testing.B) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["keycapacity"] = 1024 * 1024 * 100
	setts["valcapacity"] = 1024 * 1024 * 100
	llrb := NewLLRB("test", setts)
	// Insert 10000 items
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		llrb.Upsert(
			[]byte(key), []byte(value),
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					b.Error(err)
				} else if ond != nil {
					b.Errorf("expected old Llrbnode node is nil")
				}
				nnd.Setvbno(100).SetVbuuid(1000).SetBornseqno(uint64(i) + 1)
				return true
			})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		llrb.Range(nil, nil, "both", false, nil)
	}
}

func BenchmarkIter10000(b *testing.B) {
	setts := testsetts(Defaultsettings())
	setts["metadata.mvalue"] = true
	setts["metadata.bornseqno"] = true
	setts["metadata.vbuuid"] = true
	setts["keycapacity"] = 1024 * 1024 * 100
	setts["valcapacity"] = 1024 * 1024 * 100
	llrb := NewLLRB("test", setts)
	// Insert 10000 items
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		llrb.Upsert(
			[]byte(key), []byte(value),
			func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
				if err != nil {
					b.Error(err)
				} else if ond != nil {
					b.Errorf("expected old Llrbnode node is nil")
				}
				nnd.Setvbno(100).SetVbuuid(1000).SetBornseqno(uint64(i) + 1)
				return true
			})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := llrb.Iterate(nil, nil, "both", false)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
		}
		iter.Close()
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
	t *testing.T, nm string, inserts [][2][]byte, setts s.Settings) *LLRB {

	llrb := NewLLRB(nm, setts)
	if llrb.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	mcmds := make([]*api.MutationCmd, 0)
	for _, kv := range inserts {
		mcmds = append(mcmds, &api.MutationCmd{
			Cmd:   api.UpsertCmd,
			Key:   kv[0],
			Value: kv[1],
		})
	}
	llrb.Mutations(
		mcmds,
		func(index api.Index, i int64, nnd, ond api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			} else if ond != nil {
				t.Errorf("expected old Llrbnode as nil")
			}
			nnd.Setvbno(vbno).SetVbuuid(vbuuid)
			nnd.SetBornseqno(seqno + uint64(i))
			return true
		})
	return llrb
}

func testsetts(setts s.Settings) s.Settings {
	setts["keycapacity"] = 1024 * 1024 * 10
	setts["valcapacity"] = 1024 * 1024 * 10
	return setts
}
