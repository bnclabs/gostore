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
	nd.left = (*node)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd)) + unsafe.Sizeof(*nd)))
	nd.right = (*node)(unsafe.Pointer((uintptr)(unsafe.Pointer(nd.left)) + unsafe.Sizeof(*nd)))
	nd.settimestamp(int64(time.Now().UnixNano()))
	newnd := llrb.clone(nd)
	if llrb.equivalent(nd, newnd) == false {
		t.Errorf("expected %v, got %v", nd.repr(), newnd.repr())
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
		"valmem.capacity":  1024 * 1024 * 1024,
	}
	return config
}
