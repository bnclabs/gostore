package storage

import "testing"
import "bytes"
import "fmt"

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
			func(_ Index, _ int64, newnd, oldnd Node) {
				if oldnd != nil {
					t.Errorf("expected nil")
				}
			})
	}
	// lookups
	if d.Has(inserts[1][0]) == false {
		t.Errorf("expected key %v", string(inserts[0][0]))
	}
	nd := d.Get(inserts[2][0])
	if bytes.Compare(nd.Value(), inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(nd.Value()))
	}
	if nd := d.Get([]byte("missingkey")); nd != nil {
		t.Errorf("expected %v, got %v", nil, string(nd.Value()))
	}
	if nd := d.Min(); bytes.Compare(nd.Key(), inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(nd.Key()))
	} else if bytes.Compare(nd.Value(), inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(nd.Value()))
	}
	if nd := d.Max(); bytes.Compare(nd.Key(), []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(nd.Key()))
	} else if bytes.Compare(nd.Value(), []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(nd.Value()))
	}
	// upsert
	d.Upsert(
		inserts[0][0], []byte("value11"),
		func(_ Index, _ int64, newnd, oldnd Node) {
			if v := oldnd.Value(); bytes.Compare(v, inserts[0][1]) != 0 {
				fmsg := "expected %v, got %v\n"
				t.Errorf(fmsg, string(inserts[0][1]), string(v))
			}
		})
	// delete-min
	d.DeleteMin(func(_ Index, nd Node) {
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
	d.DeleteMax(func(_ Index, nd Node) {
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
	d.Delete([]byte("key2"), func(_ Index, nd Node) {
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
	if nd = d.Min(); nd != nil {
		t.Errorf("expected nil")
	}
	if nd = d.Max(); nd != nil {
		t.Errorf("expected nil")
	}
	d.DeleteMin(func(_ Index, nd Node) {
		if k, v := nd.Key(), nd.Value(); k != nil || v != nil {
			t.Errorf("expected {nil,nil}, got {%v,%v}", k, v)
		}
	})
	d.DeleteMax(func(_ Index, nd Node) {
		if k, v := nd.Key(), nd.Value(); k != nil || v != nil {
			t.Errorf("expected {nil,nil}, got {%v,%v}", k, v)
		}
	})
	d.Delete([]byte("hello"), func(_ Index, nd Node) {
		if v := nd.Value(); v != nil {
			t.Errorf("expected nil, got %v", v)
		}
	})
}

func TestDictRange(t *testing.T) {
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
		d.Upsert(kv[0], kv[1], func(_ Index, _ int64, newnd, oldnd Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
	}
	// both
	i, ln := 0, 0
	d.Range(
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
	d.Range(
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
	d.Range(
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
	d.Range(
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
	// corner case with low as nil
	ln = 0
	d.Range(
		nil, inserts[4][0], "high",
		func(_ Node) bool { ln += 1; return true })
	if ln != 5 {
		t.Errorf("expected %v, got %v", 5, ln)
	}
	// corner case with high as nil
	ln = 0
	d.Range(
		inserts[0][0], nil, "none",
		func(_ Node) bool { ln += 1; return true })
	if ln != 4 {
		t.Errorf("expected %v, got %v", 4, ln)
	}
	// corner case with return as false
	ln = 0
	d.Range(
		inserts[0][0], nil, "low",
		func(_ Node) bool { ln += 1; return false })
	if ln != 1 {
		t.Errorf("expected %v, got %v", 1, ln)
	}
	// corner case on the high side.
	ln = 0
	d.Range(
		inserts[0][0], inserts[0][0], "high",
		func(_ Node) bool { ln += 1; return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	ln = 0
	d.Range(
		inserts[4][0], inserts[4][0], "low",
		func(_ Node) bool { ln += 1; return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
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
		d.Upsert(kv[0], kv[1], func(_ Index, _ int64, newnd, oldnd Node) {
			if oldnd != nil {
				t.Errorf("expected nil")
			}
		})
	}

	snapch := make(chan Snapshot, 1)
	err := d.RSnapshot(snapch)
	if err != nil {
		t.Error(err)
	}
	rd := <-snapch

	d.Upsert(inserts[1][0], []byte("newvalue"), nil)
	nd := rd.Get(inserts[1][0])
	if v := nd.Value(); bytes.Compare(v, inserts[1][1]) != 0 {
		t.Errorf("expected %v, got %v", inserts[1][1], v)
	}
}

func BenchmarkDictSnapshot(b *testing.B) {
	d := NewDict()
	for i := 0; i < 10000; i++ {
		key, value := fmt.Sprintf("key%v", i), fmt.Sprintf("value%v", i)
		d.Upsert([]byte(key), []byte(value), nil)
	}

	snapch := make(chan Snapshot, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.RSnapshot(snapch)
		<-snapch
	}
}
