package dict

import "testing"
import "bytes"
import "reflect"
import "fmt"

import "github.com/prataprc/storage.go/api"

var _ = fmt.Sprintf("dummy")

func TestDict(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
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
	for _, kv := range inserts {
		d.Upsert(
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
	// lookups
	if d.Has(inserts[1][0]) == false {
		t.Errorf("expected key %v", string(inserts[0][0]))
	}

	var nd api.Node
	rc := d.Get(
		inserts[2][0],
		func(_ api.Index, _ int64, _, x api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			nd = x
			return true
		})
	if !rc {
		t.Errorf("missing key %v", string(inserts[2][0]))
	}
	if bytes.Compare(nd.Value(), inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(nd.Value()))
	}

	if rc := d.Get([]byte("missingkey"), nil); rc == true {
		t.Errorf("expected %v", nil)
	}

	nd = nil
	rc = d.Min(func(_ api.Index, _ int64, _, x api.Node, err error) bool {
		if err != nil {
			t.Error(err)
		}
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missing minimum key")
	} else if bytes.Compare(nd.Key(), inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(nd.Key()))
	} else if bytes.Compare(nd.Value(), inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(nd.Value()))
	}

	nd = nil
	rc = d.Max(func(_ api.Index, _ int64, _, x api.Node, err error) bool {
		if err != nil {
			t.Error(err)
		}
		nd = x
		return true
	})
	if rc == false {
		t.Errorf("missing maximum key")
	} else if bytes.Compare(nd.Key(), []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(nd.Key()))
	} else if bytes.Compare(nd.Value(), []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(nd.Value()))
	}

	// upsert
	d.Upsert(
		inserts[0][0], []byte("value11"),
		func(_ api.Index, _ int64, newnd, oldnd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			} else if v := oldnd.Value(); bytes.Compare(v, inserts[0][1]) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(inserts[0][1]), string(v))
			}
			return false
		})
	// delete-min
	d.DeleteMin(func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
		if err != nil {
			t.Error(err)
		} else if k := nd.Key(); bytes.Compare(k, inserts[0][0]) != 0 {
			t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
		} else if v := nd.Value(); bytes.Compare(v, []byte("value11")) != 0 {
			t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
		}
		return false
	})
	if int(d.Count()) != (len(inserts) - 1) {
		t.Errorf("expected %v, got %v", len(inserts)-1, d.Count())
	}
	// delete-max
	d.DeleteMax(func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
		if err != nil {
			t.Error(err)
		} else if k := nd.Key(); bytes.Compare(k, []byte("key5")) != 0 {
			t.Errorf("expected %v, got %v", "key5", string(k))
		} else if v := nd.Value(); bytes.Compare(v, []byte("value5")) != 0 {
			t.Errorf("expected %v, got %v", "value5", string(v))
		}
		return false
	})
	if int(d.Count()) != (len(inserts) - 2) {
		t.Errorf("expected %v, got %v", len(inserts)-2, d.Count())
	}
	// delete
	d.Delete(
		[]byte("key2"),
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			} else if v := nd.Value(); bytes.Compare(v, []byte("value2")) != 0 {
				t.Errorf("expected %v, got %v", "value2", string(v))
			}
			return false
		})
	if int(d.Count()) != (len(inserts) - 3) {
		t.Errorf("expected %v, got %v", len(inserts)-3, d.Count())
	}
	// test corner cases for Min, Max, DeleteMin, DeleteMax
	d.DeleteMin(nil)
	d.DeleteMin(nil)
	if d.Min(nil) == true {
		t.Errorf("expected false")
	}
	if d.Max(nil) == true {
		t.Errorf("expected false")
	}
	d.DeleteMin(
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			} else if k, v := nd.Key(), nd.Value(); k != nil || v != nil {
				t.Errorf("expected {nil,nil}, got {%v,%v}", k, v)
			}
			return false
		})
	d.DeleteMax(func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
		if err != nil {
			t.Error(err)
		} else if k, v := nd.Key(), nd.Value(); k != nil || v != nil {
			t.Errorf("expected {nil,nil}, got {%v,%v}", k, v)
		}
		return false
	})
	d.Delete(
		[]byte("hello"),
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			} else if v := nd.Value(); v != nil {
				t.Errorf("expected nil, got %v", v)
			}
			return false
		})
}

func TestDictBasicRange(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
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
	for _, kv := range inserts {
		d.Upsert(
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
	reverse := func(keys [][2][]byte) [][2][]byte {
		revkeys := make([][2][]byte, 0)
		for i := len(keys) - 1; i >= 0; i-- {
			revkeys = append(revkeys, keys[i])
		}
		return revkeys
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
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		if reflect.DeepEqual(outs, tcase[3]) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Fatalf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// forward range, return false
		outs = make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
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

		// backward range, return true
		outs = make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		ok := reflect.DeepEqual(outs, reverse(tcase[3].([][2][]byte)))
		if ok == false {
			t.Log(outs)
			t.Log(reverse(tcase[3].([][2][]byte)))
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// backward range, return false
		outs = make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
				return false
			})
		ref = tcase[3].([][2][]byte)
		if len(ref) > 0 {
			ref = ref[len(ref)-1 : len(ref)]
		}
		if reflect.DeepEqual(outs, ref) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Errorf(fmsg, casenum, string(lowkey), string(highkey))
		}
	}
}

func TestPartialRange(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}

	// inserts
	inserts, n := make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}

	for _, kv := range inserts {
		d.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, newnd, oldnd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				if oldnd != nil {
					t.Errorf("expected nil")
				}
				return false
			})
	}

	// forward range
	keys := make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "none", false,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	if len(keys) > 0 {
		t.Fatalf("expected empty result %v", keys)
	}

	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "low", false,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	refkeys := []string{"key10", "key12", "key14", "key16", "key18"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "high", false,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	refkeys = []string{"key2", "key20", "key22", "key24", "key26", "key28"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "both", false,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	refkeys = []string{
		"key10", "key12", "key14", "key16", "key18",
		"key2", "key20", "key22", "key24", "key26", "key28"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	// backward range
	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "none", true,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	if len(keys) > 0 {
		t.Fatalf("expected empty result %v", keys)
	}

	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "low", true,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	refkeys = []string{"key18", "key16", "key14", "key12", "key10"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "high", true,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	refkeys = []string{"key28", "key26", "key24", "key22", "key20", "key2"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	d.Range(
		[]byte("key1"), []byte("key2"), "both", true,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				t.Error(err)
			}
			keys = append(keys, string(nd.Key()))
			return true
		})
	refkeys = []string{
		"key28", "key26", "key24", "key22", "key20",
		"key2", "key18", "key16", "key14", "key12", "key10"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}
}

func TestDictRange(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}

	// inserts
	inserts, n := make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}
	for _, kv := range inserts {
		d.Upsert(
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

	inclusions := []string{"none", "low", "high", "both"}
	var tcases [][]interface{}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			for _, incl := range inclusions {
				lkey, hkey := fmt.Sprintf("key%v", i), fmt.Sprintf("key%v", j)
				tcase := []interface{}{[]byte(lkey), []byte(hkey), incl}
				tcases = append(tcases, tcase)
			}
		}
	}

	verify := func(lkey, hkey []byte, incl string, nd api.Node) {
		lowcmp, highcmp := 1, -1
		if bytes.Compare(lkey, hkey) == 0 && incl != "none" {
			incl = "both"
		}
		switch incl {
		case "low":
			lowcmp = 0
		case "high":
			highcmp = 0
		case "both":
			lowcmp, highcmp = 0, 0
		}
		lpartial := incl == "low" || incl == "both"
		hpartial := incl == "high" || incl == "both"
		lk, hk := string(lkey), string(hkey)
		if api.Binarycmp(nd.Key(), lkey, lpartial) < lowcmp {
			fmsg := "failed for %v (%v,%v,%v)"
			t.Fatalf(fmsg, string(nd.Key()), lk, hk, incl)
		}
		if api.Binarycmp(nd.Key(), hkey, hpartial) > highcmp {
			fmsg := "failed for %v (%v,%v,%v)"
			t.Fatalf(fmsg, string(nd.Key()), lk, hk, incl)
		}
	}

	for _, tcase := range tcases {
		lkey, hkey := tcase[0].([]byte), tcase[1].([]byte)
		incl := tcase[2].(string)

		// forward range, return true
		count, prev := 0, []byte(nil)
		d.Range(
			lkey, hkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				key := nd.Key()
				if prev != nil && bytes.Compare(key, prev) != 1 {
					fmsg := "failed for %v (%v,%v,%v)"
					t.Fatalf(fmsg, string(key), string(lkey), string(hkey), incl)
				}
				verify(lkey, hkey, incl, nd)
				count++
				prev = key
				return true
			})
		// forward range, return false
		count = 0
		d.Range(
			lkey, hkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				verify(lkey, hkey, incl, nd)
				count++
				return false
			})
		if count > 1 {
			fmsg := "failed count %v (%v,%v,%v)"
			t.Fatalf(fmsg, count, string(lkey), string(hkey), incl)
		}

		// backward range, return true
		count, prev = 0, []byte(nil)
		d.Range(
			lkey, hkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				key := nd.Key()
				if prev != nil && bytes.Compare(key, prev) != -1 {
					fmsg := "failed for %v (%v,%v,%v)"
					t.Fatalf(fmsg, string(key), string(lkey), string(hkey), incl)
				}
				verify(lkey, hkey, incl, nd)
				count++
				prev = key
				return true
			})
		// backward range, return false
		count = 0
		d.Range(
			lkey, hkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				verify(lkey, hkey, incl, nd)
				count++
				return false
			})
		if count > 1 {
			fmsg := "failed count %v (%v,%v,%v)"
			t.Fatalf(fmsg, count, string(lkey), string(hkey), incl)
		}
	}
}

func TestDictBasicIterate(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
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
	for _, kv := range inserts {
		d.Upsert(
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

		// forward iteration
		refs := make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		iter := d.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			outs := make([][2][]byte, 0)
			for nd := iter.Next(); nd != nil; nd = iter.Next() {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			}
			if !reflect.DeepEqual(outs, refs) {
				t.Log(outs)
				t.Log(refs)
				t.Fatalf("failed %v", casenum)
			}
			iter.Close()
		} else if len(refs) > 0 {
			t.Fatalf("unexpected nil iterator")
		}
		// backward iteration
		refs = make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		iter = d.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			outs := make([][2][]byte, 0)
			for nd := iter.Next(); nd != nil; nd = iter.Next() {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			}
			if !reflect.DeepEqual(outs, refs) {
				t.Fatalf("failed %v", casenum)
			}
			iter.Close()
		} else if len(refs) > 0 {
			t.Fatalf("unexpected nil iterator")
		}
	}
}

func TestPartialIterate(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}

	// inserts
	inserts, n := make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}

	for _, kv := range inserts {
		d.Upsert(
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

	// forward range
	keys := make([]string, 0)
	iter := d.Iterate([]byte("key1"), []byte("key2"), "none", false)
	nd := iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	if len(keys) > 0 {
		t.Fatalf("expected empty result %v", keys)
	}

	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "low", false)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	refkeys := []string{"key10", "key12", "key14", "key16", "key18"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "high", false)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	refkeys = []string{"key2", "key20", "key22", "key24", "key26", "key28"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "both", false)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	refkeys = []string{
		"key10", "key12", "key14", "key16", "key18",
		"key2", "key20", "key22", "key24", "key26", "key28"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	// backward range
	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "none", true)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	if len(keys) > 0 {
		t.Fatalf("expected empty result %v", keys)
	}

	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "low", true)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	refkeys = []string{"key18", "key16", "key14", "key12", "key10"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "high", true)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	refkeys = []string{"key28", "key26", "key24", "key22", "key20", "key2"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}

	keys = make([]string, 0)
	iter = d.Iterate([]byte("key1"), []byte("key2"), "both", true)
	nd = iter.Next()
	for nd != nil {
		keys = append(keys, string(nd.Key()))
		nd = iter.Next()
	}
	refkeys = []string{
		"key28", "key26", "key24", "key22", "key20",
		"key2", "key18", "key16", "key14", "key12", "key10"}
	if !reflect.DeepEqual(refkeys, keys) {
		t.Fatalf("expected %v, got %v", refkeys, keys)
	}
}

func TestDictIterate(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}

	// inserts
	inserts, n := make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}
	for _, kv := range inserts {
		d.Upsert(
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

	inclusions := []string{"none", "low", "high", "both"}
	tcases := make([][]interface{}, 0)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			for _, incl := range inclusions {
				lkey, hkey := fmt.Sprintf("key%v", i), fmt.Sprintf("key%v", j)
				tcase := []interface{}{[]byte(lkey), []byte(hkey), incl}
				tcases = append(tcases, tcase)
			}
		}
	}

	for casenum, tcase := range tcases {
		lowkey, highkey := tcase[0].([]byte), tcase[1].([]byte)
		incl := tcase[2].(string)

		// forward iteration
		refs := make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, false,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		outs := make([][2][]byte, 0)
		iter := d.Iterate(lowkey, highkey, incl, false)
		if iter != nil {
			for nd := iter.Next(); nd != nil; nd = iter.Next() {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			}
			iter.Close()
		}
		if !reflect.DeepEqual(outs, refs) {
			t.Fatalf("failed %v", casenum)
		}

		// backward iteration
		refs = make([][2][]byte, 0)
		d.Range(
			lowkey, highkey, incl, true,
			func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
				if err != nil {
					t.Error(err)
				}
				refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
				return true
			})
		outs = make([][2][]byte, 0)
		iter = d.Iterate(lowkey, highkey, incl, true)
		if iter != nil {
			for nd := iter.Next(); nd != nil; nd = iter.Next() {
				outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			}
			iter.Close()
		}
		if !reflect.DeepEqual(outs, refs) {
			t.Fatalf("failed %v", casenum)
		}
	}
}

func TestDictRsnapshot(t *testing.T) {
	d := NewDict()
	inserts := [][2][]byte{
		[2][]byte{[]byte("key1"), []byte("value1")},
		[2][]byte{[]byte("key2"), []byte("value2")},
		[2][]byte{[]byte("key3"), []byte("value3")},
		[2][]byte{[]byte("key4"), []byte("value4")},
		[2][]byte{[]byte("key5"), []byte("value5")},
	}
	for _, kv := range inserts {
		d.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, newnd, oldnd api.Node, err error) bool {
				if oldnd != nil {
					t.Errorf("expected nil")
				}
				return false
			})
	}

	snapch := make(chan api.IndexSnapshot, 1)
	err := d.RSnapshot(snapch, true)
	if err != nil {
		t.Error(err)
	}
	rd := <-snapch

	d.Upsert(inserts[1][0], []byte("newvalue"), nil)
	rc := rd.Get(
		inserts[1][0],
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if v := nd.Value(); bytes.Compare(v, inserts[1][1]) != 0 {
				t.Errorf("expected %v, got %v", inserts[1][1], v)
				return false
			}
			return true
		})
	if rc == false {
		t.Errorf("missing key %v", string(inserts[1][0]))
	}
}

func TestDictClone(t *testing.T) {
	d := NewDict()
	if d.Count() != 0 {
		t.Fatalf("expected an empty dict")
	}
	// inserts
	inserts, n := make([][2][]byte, 0), 100
	for i := 0; i < n; i += 2 {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		inserts = append(inserts, [2][]byte{[]byte(key), []byte(value)})
	}
	for _, kv := range inserts {
		d.Upsert(
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
	d.Range(
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
		d.Upsert(
			kv[0], kv[1],
			func(_ api.Index, _ int64, newnd, oldnd api.Node, err error) bool {
				return false
			})
	}

	newd := d.Clone(d.id + "-clone")

	keys, values := make([][]byte, 0), make([][]byte, 0)
	newd.Range(
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

func BenchmarkDictSnapshot(b *testing.B) {
	d := NewDict()
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		d.Upsert([]byte(key), []byte(value), nil)
	}

	snapch := make(chan api.IndexSnapshot, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.RSnapshot(snapch, true)
		<-snapch
	}
}
