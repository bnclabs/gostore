package llrb

import "testing"
import "fmt"
import "unsafe"
import "time"
import "math/rand"
import "bytes"

var _ = fmt.Sprintf("dummy")

func TestNewLLRB(t *testing.T) {
	config := makenewconfig()
	llrb := NewLLRB("test", config, nil)
	if llrb == nil {
		t.Errorf("unexpected nil")
	}
	nodavail := config["nodearena.capacity"].(int)
	valavail := config["valarena.capacity"].(int)
	if overhead, useful := llrb.NodeArena(); overhead != 2112 {
		t.Errorf("expected %v, got %v", 0, 2112)
	} else if useful != 0 {
		t.Errorf("expected %v, got %v", 0, useful)
	} else if overhead, useful := llrb.ValueArena(); overhead != 2112 {
		t.Errorf("expected %v, got %v", 0, overhead)
	} else if useful != 0 {
		t.Errorf("expected %v, got %v", 0, overhead)
	} else if x, y := int64(0), llrb.NodeAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.ValueAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y := int64(nodavail), llrb.NodeAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(valavail), llrb.ValueAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestNewLLRBNode(t *testing.T) {
	llrb := NewLLRB("test", makenewconfig(), nil)
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
	llrb := NewLLRB("test", makenewconfig(), nil)
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
	llrb := NewLLRB("test", makenewconfig(), nil)
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
	llrb := NewLLRB("test", makenewconfig(), nil)
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
	llrb := NewLLRB("test", config, nil)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		if nd := llrb.Upsert(key, value, vbno, vbuuid, seqno); nd != nil {
			t.Errorf("expected nil")
		} else if x := llrb.Count(); x != int64(i+1) {
			t.Errorf("expected %v, got %v", i, x)
		}
		seqno++
	}
	// check memory accounting
	if overhead, useful := llrb.NodeArena(); overhead != 3541 {
		t.Errorf("expected %v, got %v", 3541, overhead)
	} else if overhead, useful = llrb.ValueArena(); overhead != 22656 {
		t.Errorf("expected %v, got %v", 22656, overhead)
	} else if useful != 20971520 {
		t.Errorf("expected %v, got %v", 20971520, useful)
	} else if x, y := int64(1920000), llrb.NodeAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1280000), llrb.ValueAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1071821824), llrb.NodeAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(10736138240), llrb.ValueAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	// Get items
	vbno, vbuuid, seqno = uint16(10), uint64(0xABCD), uint64(12345678)
	for i, key := range keys {
		nd := llrb.Get(key)
		if nd == nil {
			t.Errorf("unexpected nil")
		} else if nd.vbno() != vbno {
			t.Errorf("expected %v, got %v", vbno, nd.vbno())
		} else if nd.vbuuid != vbuuid {
			t.Errorf("expected %v, got %v", vbuuid, nd.vbuuid)
		} else if nd.seqno != seqno {
			t.Errorf("expected %v, got %v", seqno, nd.seqno)
		}
		x, y := values[i], nd.nodevalue().value()
		if bytes.Compare(x, y) != 0 {
			t.Errorf("expected %v, got %v", x, y)
		}
		seqno++
	}
}

func TestLLRBUpsert(t *testing.T) {
	config := makenewconfig()
	llrb := NewLLRB("test", config, nil)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		if nd := llrb.Upsert(key, value, vbno, vbuuid, seqno); nd != nil {
			t.Errorf("expected nil")
		} else if x := llrb.Count(); x != int64(i+1) {
			t.Errorf("expected %v, got %v", i, x)
		}
		keys, values = append(keys, key), append(values, value)
		seqno++
	}

	// upsert same items
	newvalues := make([][]byte, 0)
	for i, key := range keys {
		value := make([]byte, 200)
		_, value = makekeyvalue(nil, value)
		newvalues = append(newvalues, value)
		nd := llrb.Upsert(key, value, vbno, vbuuid, seqno)
		if nd == nil {
			t.Errorf("unexpected nil")
		} else if nd.vbno() != vbno {
			t.Errorf("expected %v, got %v", vbno, nd.vbno())
		} else if nd.vbuuid != vbuuid {
			t.Errorf("expected %v, got %v", vbuuid, nd.vbuuid)
		}
		x, y := values[i], nd.nodevalue().value()
		if bytes.Compare(x, y) != 0 {
			t.Errorf("expected %v, got %v", x, y)
		}
		llrb.Freenode(nd)
		seqno++
	}

	// check memory accounting
	if overhead, useful := llrb.NodeArena(); overhead != 3541 {
		t.Errorf("expected %v, got %v", 3541, overhead)
	} else if overhead, useful = llrb.ValueArena(); overhead != 34422 {
		t.Errorf("expected %v, got %v", 34422, overhead)
	} else if useful != 41941504 {
		t.Errorf("expected %v, got %v", 41941504, useful)
	} else if x, y := int64(1920000), llrb.NodeAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2240000), llrb.ValueAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1071821824), llrb.NodeAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(10735178240), llrb.ValueAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1000000), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(2000000), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestLLRBDelete(t *testing.T) {
	config := makenewconfig()
	llrb := NewLLRB("test", config, nil)
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		if nd := llrb.Upsert(key, value, vbno, vbuuid, seqno); nd != nil {
			t.Errorf("expected nil")
		} else if x := llrb.Count(); x != int64(i+1) {
			t.Errorf("expected %v, got %v", i, x)
		}
		keys, values = append(keys, key), append(values, value)
		seqno++
	}
	// Delete items
	vbno, vbuuid, seqno = uint16(10), uint64(0xABCD), uint64(12345678)
	for i, key := range keys {
		nd := llrb.Delete(key)
		if nd == nil {
			t.Errorf("unexpected nil")
		} else if nd.vbno() != vbno {
			t.Errorf("expected %v, got %v", vbno, nd.vbno())
		} else if nd.vbuuid != vbuuid {
			t.Errorf("expected %v, got %v", vbuuid, nd.vbuuid)
		} else if nd.seqno != seqno {
			t.Errorf("expected %v, got %v", seqno, nd.seqno)
		}
		x, y := values[i], nd.nodevalue().value()
		if bytes.Compare(x, y) != 0 {
			t.Errorf("expected %v, got %v", x, y)
		}
		llrb.Freenode(nd)
		seqno++
	}
	// check memory accounting
	if overhead, useful := llrb.NodeArena(); overhead != 3541 {
		t.Errorf("expected %v, got %v", 3541, overhead)
	} else if overhead, useful = llrb.ValueArena(); overhead != 22656 {
		t.Errorf("expected %v, got %v", 22656, overhead)
	} else if useful != 20971520 {
		t.Errorf("expected %v, got %v", 20971520, useful)
	} else if x, y := int64(0), llrb.NodeAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.ValueAllocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(1073741824), llrb.NodeAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(10737418240), llrb.ValueAvailable(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.KeyMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if x, y = int64(0), llrb.ValueMemory(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func TestLLRBRange(t *testing.T) {
	llrb := NewLLRB("test", makenewconfig(), nil)
	d := NewDict()
	vbno, vbuuid, seqno := uint16(10), uint64(0xABCD), uint64(12345678)
	keys, values := make([][]byte, 0), make([][]byte, 0)
	// insert 10K items
	count := 10 * 1000
	for i := 0; i < count; i++ {
		key, value := make([]byte, 100), make([]byte, 100)
		key, value = makekeyvalue(key, value)
		if nd := llrb.Upsert(key, value, vbno, vbuuid, seqno); nd != nil {
			t.Errorf("expected nil")
		} else if x := llrb.Count(); x != int64(i+1) {
			t.Errorf("expected %v, got %v", i, x)
		}
		d.Upsert(key, value)
		keys, values = append(keys, key), append(values, value)
		seqno++
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
		llrb.Range(lowkey, highkey, incl, func(nd *node) bool {
			llrbks = append(llrbks, nd.key())
			llrbvs = append(llrbvs, nd.nodevalue().value())
			return true
		})
		dks, dvs := make([][]byte, 0), make([][]byte, 0)
		d.Range(lowkey, highkey, incl, func(k, v []byte) bool {
			dks, dvs = append(dks, k), append(dvs, v)
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

func makenewconfig() map[string]interface{} {
	config := map[string]interface{}{
		"nodearena.minblock": 96,
		"nodearena.maxblock": 1024,
		"nodearena.capacity": 1024 * 1024 * 1024,
		"nodepool.capacity":  2 * 1024 * 1024,
		"valarena.minblock":  96,
		"valarena.maxblock":  1024 * 1024,
		"valarena.capacity":  10 * 1024 * 1024 * 1024,
		"valpool.capacity":   10 * 2 * 1024 * 1024,
	}
	return config
}
