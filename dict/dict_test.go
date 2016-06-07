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
			func(_ api.Index, _ int64, newnd, oldnd api.Node) {
				if oldnd != nil {
					t.Errorf("expected nil")
				}
			})
	}
	// lookups
	if d.Has(inserts[1][0]) == false {
		t.Errorf("expected key %v", string(inserts[0][0]))
	}

	var nd api.Node
	rc := d.Get(inserts[2][0], func(x api.Node) bool {
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
	rc = d.Min(func(x api.Node) bool {
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
	rc = d.Max(func(x api.Node) bool {
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
		func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if v := oldnd.Value(); bytes.Compare(v, inserts[0][1]) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(inserts[0][1]), string(v))
			}
		})
	// delete-min
	d.DeleteMin(func(_ api.Index, nd api.Node) {
		if k := nd.Key(); bytes.Compare(k, inserts[0][0]) != 0 {
			t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
		} else if v := nd.Value(); bytes.Compare(v, []byte("value11")) != 0 {
			t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
		}
	})
	if int(d.Count()) != (len(inserts) - 1) {
		t.Errorf("expected %v, got %v", len(inserts)-1, d.Count())
	}
	// delete-max
	d.DeleteMax(func(_ api.Index, nd api.Node) {
		if k := nd.Key(); bytes.Compare(k, []byte("key5")) != 0 {
			t.Errorf("expected %v, got %v", "key5", string(k))
		} else if v := nd.Value(); bytes.Compare(v, []byte("value5")) != 0 {
			t.Errorf("expected %v, got %v", "value5", string(v))
		}
	})
	if int(d.Count()) != (len(inserts) - 2) {
		t.Errorf("expected %v, got %v", len(inserts)-2, d.Count())
	}
	// delete
	d.Delete([]byte("key2"), func(_ api.Index, nd api.Node) {
		if v := nd.Value(); bytes.Compare(v, []byte("value2")) != 0 {
			t.Errorf("expected %v, got %v", "value2", string(v))
		}
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
	d.DeleteMin(func(_ api.Index, nd api.Node) {
		if k, v := nd.Key(), nd.Value(); k != nil || v != nil {
			t.Errorf("expected {nil,nil}, got {%v,%v}", k, v)
		}
	})
	d.DeleteMax(func(_ api.Index, nd api.Node) {
		if k, v := nd.Key(), nd.Value(); k != nil || v != nil {
			t.Errorf("expected {nil,nil}, got {%v,%v}", k, v)
		}
	})
	d.Delete([]byte("hello"), func(_ api.Index, nd api.Node) {
		if v := nd.Value(); v != nil {
			t.Errorf("expected nil, got %v", v)
		}
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
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
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
		d.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		if reflect.DeepEqual(outs, tcase[3]) == false {
			fmsg := "failed for %v (%v,%v)"
			t.Fatalf(fmsg, casenum, string(lowkey), string(highkey))
		}
		// forward range, return false
		outs = make([][2][]byte, 0)
		d.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
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
		d.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
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
		d.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
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
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
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
		lowcmp, highcmp := 0, 0
		switch incl {
		case "none":
			lowcmp, highcmp = 1, -1
		case "low":
			highcmp = -1
		case "high":
			lowcmp = 1
		}
		if bytes.Compare(lkey, hkey) == 0 {
			lowcmp, highcmp = 0, 0
		}
		if bytes.Compare(nd.Key(), lkey) < lowcmp {
			fmsg := "failed for %v (%v,%v,%v)"
			t.Fatalf(fmsg, string(nd.Key()), string(lkey), string(hkey), incl)
		}
		if bytes.Compare(nd.Key(), hkey) > highcmp {
			fmsg := "failed for %v (%v,%v,%v)"
			t.Fatalf(fmsg, string(nd.Key()), string(lkey), string(hkey), incl)
		}
	}

	for _, tcase := range tcases {
		lkey, hkey := tcase[0].([]byte), tcase[1].([]byte)
		incl := tcase[2].(string)

		// forward range, return true
		count, prev := 0, []byte(nil)
		d.Range(lkey, hkey, incl, false, func(nd api.Node) bool {
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
		if bytes.Compare(lkey, hkey) == 0 && count > 1 {
			fmsg := "failed count %v (%v,%v,%v)"
			t.Fatalf(fmsg, count, string(lkey), string(hkey), incl)
		}
		// forward range, return false
		count = 0
		d.Range(lkey, hkey, incl, false, func(nd api.Node) bool {
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
		d.Range(lkey, hkey, incl, true, func(nd api.Node) bool {
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
		if bytes.Compare(lkey, hkey) == 0 && count > 1 {
			fmsg := "failed count %v (%v,%v,%v)"
			t.Fatalf(fmsg, count, string(lkey), string(hkey), incl)
		}
		// backward range, return false
		count = 0
		d.Range(lkey, hkey, incl, true, func(nd api.Node) bool {
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
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
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
		d.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		iter := d.Iterate(lowkey, highkey, incl, false)
		outs := make([][2][]byte, 0)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
		}
		if !reflect.DeepEqual(outs, refs) {
			t.Fatalf("failed %v", casenum)
		}
		iter.Close()
		// backward iteration
		refs = make([][2][]byte, 0)
		d.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
			refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		iter = d.Iterate(lowkey, highkey, incl, true)
		outs = make([][2][]byte, 0)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
		}
		if !reflect.DeepEqual(outs, refs) {
			t.Fatalf("failed %v", casenum)
		}
		iter.Close()
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
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
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
		d.Range(lowkey, highkey, incl, false, func(nd api.Node) bool {
			refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		iter := d.Iterate(lowkey, highkey, incl, false)
		outs := make([][2][]byte, 0)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
		}
		if !reflect.DeepEqual(outs, refs) {
			t.Fatalf("failed %v", casenum)
		}
		iter.Close()

		// backward iteration
		refs = make([][2][]byte, 0)
		d.Range(lowkey, highkey, incl, true, func(nd api.Node) bool {
			refs = append(refs, [2][]byte{nd.Key(), nd.Value()})
			return true
		})
		iter = d.Iterate(lowkey, highkey, incl, true)
		outs = make([][2][]byte, 0)
		for nd := iter.Next(); nd != nil; nd = iter.Next() {
			outs = append(outs, [2][]byte{nd.Key(), nd.Value()})
		}
		if !reflect.DeepEqual(outs, refs) {
			t.Fatalf("failed %v", casenum)
		}
		iter.Close()
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
		d.Upsert(kv[0], kv[1], func(_ api.Index, _ int64, newnd, oldnd api.Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
	}

	snapch := make(chan api.IndexSnapshot, 1)
	err := d.RSnapshot(snapch)
	if err != nil {
		t.Error(err)
	}
	rd := <-snapch

	d.Upsert(inserts[1][0], []byte("newvalue"), nil)
	rc := rd.Get(inserts[1][0], func(nd api.Node) bool {
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

func BenchmarkDictSnapshot(b *testing.B) {
	d := NewDict()
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		d.Upsert([]byte(key), []byte(value), nil)
	}

	snapch := make(chan api.IndexSnapshot, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.RSnapshot(snapch)
		<-snapch
	}
}
