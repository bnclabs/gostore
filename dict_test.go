package llrb

import "testing"
import "bytes"

func TestDict(t *testing.T) {
	d := NewDict()
	if d.Len() != 0 {
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
		if v := d.Upsert(kv[0], kv[1]); v != nil {
			t.Errorf("expected nil")
		}
	}
	// lookups
	if d.Has(inserts[1][0]) == false {
		t.Errorf("expected key %v", string(inserts[0][0]))
	}
	if v := d.Get(inserts[2][0]); bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	if k, v := d.Min(); bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	if k, v := d.Max(); bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}
	// upsert
	v := d.Upsert(inserts[0][0], []byte("value11"))
	if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v\n", string(inserts[0][1]), string(v))
	}
	// deletes
	if k, v := d.DeleteMin(); bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, []byte("value11")) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	} else if d.Len() != (len(inserts) - 1) {
		t.Errorf("expected %v, got %v", len(inserts)-1, d.Len())
	}
	if k, v := d.DeleteMax(); bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	} else if d.Len() != (len(inserts) - 2) {
		t.Errorf("expected %v, got %v", len(inserts)-2, d.Len())
	}
	if v := d.Delete([]byte("key2")); bytes.Compare(v, []byte("value2")) != 0 {
		t.Errorf("expected %v, got %v", "value2", string(v))
	} else if d.Len() != (len(inserts) - 3) {
		t.Errorf("expected %v, got %v", len(inserts)-3, d.Len())
	}
}

func TestDictRange(t *testing.T) {
	d := NewDict()
	if d.Len() != 0 {
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
		if v := d.Upsert(kv[0], kv[1]); v != nil {
			t.Errorf("expected nil")
		}
	}
	// both
	i, ln := 0, 0
	d.Range(
		inserts[0][0], inserts[4][0], "both",
		func(k, v []byte) bool {
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
		func(k, v []byte) bool {
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
		func(k, v []byte) bool {
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
		func(k, v []byte) bool {
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
	d.Range(
		inserts[0][0], inserts[0][0], "high",
		func(k, v []byte) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	i, ln = 0, 0
	d.Range(
		inserts[4][0], inserts[4][0], "low",
		func(k, v []byte) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
}
