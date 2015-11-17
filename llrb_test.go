package llrb

import "testing"
import "fmt"
import "unsafe"
import "time"
import "bytes"

var _ = fmt.Sprintf("dummy")

func TestNewLLRB(t *testing.T) {
	config := makenewconfig()
	llrb := NewLLRB(config)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}
	avail := config["nodemem.capacity"].(int) + config["valmem.capacity"].(int)
	if x, y := int64(4208), llrb.Memory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.Allocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(avail), llrb.Available(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestNewLLRBNode(t *testing.T) {
	llrb := NewLLRB(makenewconfig())
	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	nd := llrb.newnode(key, value, vbno, vbuuid, seqno)
	if k := nd.key(); bytes.Compare(k, key) != 0 {
		t.Errorf("expected %v, got %v", key, nd.key())
	} else if v := nd.nodevalue().value(); bytes.Compare(v, value) != 0 {
		t.Errorf("expected %v, got %v", value, v)
	} else if x := nd.vbno(); x != vbno {
		t.Errorf("expected %v, got %v", vbno, x)
	} else if x := nd.vbuuid; x != vbuuid {
		t.Errorf("expected %v, got %v", vbuuid, x)
	} else if x := nd.seqno; x != seqno {
		t.Errorf("expected %v, got %v", seqno, x)
	}
	llrb.Freenode(nd)
}

func TestCloneLLRBNode(t *testing.T) {
	llrb := NewLLRB(makenewconfig())
	key, value := makekeyvalue(make([]byte, 128), make([]byte, 1024))
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	nd := llrb.newnode(key, value, vbno, vbuuid, seqno)
	nd.setblack().setdirty()
	nd.left = (*node)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd)) +
		unsafe.Sizeof(*nd)))
	nd.right = (*node)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd.left)) +
		unsafe.Sizeof(*nd)))
	nd.settimestamp(int64(time.Now().UnixNano()))
	newnd := llrb.clone(nd)
	if llrb.equivalent(nd, newnd) == false {
		t.Errorf("expected %v, got %v", nd.repr(), newnd.repr())
	}
}

func TestLLRBBasic(t *testing.T) {
	llrb := NewLLRB(makenewconfig())
	if llrb.Count() != 0 {
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
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	for _, kv := range inserts {
		v := llrb.Upsert(kv[0], kv[1], vbno, vbuuid, seqno)
		if v != nil {
			t.Errorf("expected nil")
		}
		seqno++
	}
	// lookups
	if llrb.Has(inserts[1][0]) == false {
		t.Errorf("expected key %v", string(inserts[0][0]))
	}
	nd := llrb.Get(inserts[2][0])
	v := nd.nodevalue().value()
	if bytes.Compare(v, inserts[2][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[2][1]), string(v))
	}
	nd = llrb.Min()
	k, v := nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	}
	nd = llrb.Max()
	k, v = nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	}
	// upsert
	nd = llrb.Upsert(inserts[0][0], []byte("value11"), vbno, vbuuid, seqno)
	v = nd.nodevalue().value()
	if bytes.Compare(v, inserts[0][1]) != 0 {
		t.Errorf("expected %v, got %v\n", string(inserts[0][1]), string(v))
	}
	llrb.Freenode(nd)
	// deletes
	nd = llrb.DeleteMin()
	k, v = nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, inserts[0][0]) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][0]), string(k))
	} else if bytes.Compare(v, []byte("value11")) != 0 {
		t.Errorf("expected %v, got %v", string(inserts[0][1]), string(v))
	} else if llrb.Count() != int64(len(inserts)-1) {
		t.Errorf("expected %v, got %v", len(inserts)-1, llrb.Count())
	}
	llrb.Freenode(nd)
	nd = llrb.DeleteMax()
	k, v = nd.key(), nd.nodevalue().value()
	if bytes.Compare(k, []byte("key5")) != 0 {
		t.Errorf("expected %v, got %v", "key5", string(k))
	} else if bytes.Compare(v, []byte("value5")) != 0 {
		t.Errorf("expected %v, got %v", "value5", string(v))
	} else if llrb.Count() != int64(len(inserts)-2) {
		t.Errorf("expected %v, got %v", len(inserts)-2, llrb.Count())
	}
	llrb.Freenode(nd)
	nd = llrb.Delete([]byte("key2"))
	v = nd.nodevalue().value()
	if bytes.Compare(v, []byte("value2")) != 0 {
		t.Errorf("expected %v, got %v", "value2", string(v))
	} else if llrb.Count() != int64(len(inserts)-3) {
		t.Errorf("expected %v, got %v", len(inserts)-3, llrb.Count())
	}
	llrb.Freenode(nd)
}

func TestLLRBBasicRange(t *testing.T) {
	llrb := NewLLRB(makenewconfig())
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
		if v := llrb.Upsert(kv[0], kv[1], vbno, vbuuid, seqno); v != nil {
			t.Errorf("expected nil")
		}
		seqno++
	}
	// both
	i, ln := 0, 0
	llrb.Range(
		inserts[0][0], inserts[4][0], "both",
		func(nd *node) bool {
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
	llrb.Range(
		inserts[0][0], inserts[4][0], "none",
		func(nd *node) bool {
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
	llrb.Range(
		inserts[0][0], inserts[4][0], "high",
		func(nd *node) bool {
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
	llrb.Range(
		inserts[0][0], inserts[4][0], "low",
		func(nd *node) bool {
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
	llrb.Range(
		inserts[0][0], inserts[0][0], "high",
		func(nd *node) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
	// corner case on the low side.
	i, ln = 0, 0
	llrb.Range(
		inserts[4][0], inserts[4][0], "low",
		func(nd *node) bool { return true })
	if ln != 0 {
		t.Errorf("expected %v, got %v", 0, ln)
	}
}

func TestLLRBInsert(t *testing.T) {
	config := makenewconfig()
	llrb := NewLLRB(config)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	// insert first item
	nd := llrb.Upsert([]byte("abcdef"), []byte("value"), vbno, vbuuid, seqno)
	if nd != nil {
		t.Errorf("expected nil")
	} else if x, y := int64(1), llrb.Count(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// insert second item
	seqno++
	nd1 := llrb.Upsert([]byte("bcdefg"), []byte("value1"), vbno, vbuuid, seqno)
	if nd1 != nil {
		t.Errorf("expected nil")
	} else if x, y := int64(2), llrb.Count(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// insert third item
	seqno++
	nd2 := llrb.Upsert([]byte("aacdef"), []byte("value2"), vbno, vbuuid, seqno)
	if nd2 != nil {
		t.Errorf("expected nil")
	} else if x, y := int64(3), llrb.Count(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// check memory accounting
	if x, y := int64(223717202), llrb.Memory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(576), llrb.Allocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(11811159488), llrb.Available(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(18), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(17), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestLLRBUpsert(t *testing.T) {
	llrb := NewLLRB(makenewconfig())
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	// insert
	inserts := [][2][]byte{
		[2][]byte{[]byte("abcdef"), []byte("value")},
		[2][]byte{[]byte("bcdefg"), []byte("value1")},
		[2][]byte{[]byte("aacdef"), []byte("value2")},
	}
	for _, kv := range inserts {
		llrb.Upsert(kv[0], kv[1], vbno, vbuuid, seqno)
		seqno++
	}
	// upsert
	upserts := [][2][]byte{
		[2][]byte{[]byte("abcdef"), []byte("upsertvalue")},
		[2][]byte{[]byte("bcdefg"), []byte("upsertvalue1")},
		[2][]byte{[]byte("aacdef"), []byte("upsertvalue2")},
	}
	for i, kv := range upserts {
		nd := llrb.Upsert(kv[0], kv[1], vbno, vbuuid, seqno)
		refk, refv := inserts[i][0], inserts[i][1]
		if k := nd.key(); bytes.Compare(k, refk) != 0 {
			t.Errorf("expected %v, got %v", refk, k)
		}
		if v := nd.nodevalue().value(); bytes.Compare(v, refv) != 0 {
			t.Errorf("expected %v, got %v", refv, v)
		}
		llrb.Freenode(nd)
		seqno++
	}
	// check the count
	if x, y := int64(3), llrb.Count(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// check memory accounting
	if x, y := int64(223717202), llrb.Memory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(576), llrb.Allocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(11811159488), llrb.Available(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(18), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(35), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func makekeyvalue(key, value []byte) ([]byte, []byte) {
	if key != nil {
		for i := 0; i < len(key); i++ {
			key[i] = byte(97 + (i % 26))
		}
	}
	if value != nil {
		for i := 0; i < len(value); i++ {
			value[i] = byte(97 + (i % 26))
		}
	}
	return key, value
}

func makenewconfig() map[string]interface{} {
	config := map[string]interface{}{
		"nodemem.minblock": 96,
		"nodemem.maxblock": 1024,
		"nodemem.capacity": 1024 * 1024 * 1024,
		"valmem.minblock":  96,
		"valmem.maxblock":  1024 * 1024,
		"valmem.capacity":  10 * 1024 * 1024 * 1024,
	}
	return config
}
