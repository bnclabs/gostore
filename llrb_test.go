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

func TestNewNode(t *testing.T) {
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
	llrb.freenode(nd)
}

func TestCloneNode(t *testing.T) {
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

func TestInsert(t *testing.T) {
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

func TestUpsert(t *testing.T) {
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
