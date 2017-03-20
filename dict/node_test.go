package dict

import "testing"
import "bytes"

func TestNode(t *testing.T) {
	key, value := []byte("key1"), []byte("value1")
	nd := newdictnode(key, value)

	vbno, vbuuid := uint16(10001), uint64(0x12345678)
	bornseqno, deadseqno := uint64(100), uint64(101)

	nd.Setvbno(vbno)
	if nd.Vbno() != vbno {
		t.Errorf("expected %v, got %v", vbno, nd.Vbno())
	}
	nd.SetVbuuid(vbuuid)
	if nd.Vbuuid() != vbuuid {
		t.Errorf("expected %v, got %v", vbuuid, nd.Vbuuid())
	}
	nd.SetBornseqno(bornseqno)
	if nd.Bornseqno() != bornseqno {
		t.Errorf("expected %v, got %v", bornseqno, nd.Bornseqno())
	}
	if bytes.Compare(key, nd.Key()) != 0 {
		t.Errorf("expected %q, got %q", key, nd.Key())
	}
	if bytes.Compare(value, nd.Value()) != 0 {
		t.Errorf("expected %q, got %q", value, nd.Value())
	}

	nd.SetDeadseqno(deadseqno)
	if nd.Deadseqno() != deadseqno {
		t.Errorf("expected %v, got %v", deadseqno, nd.Deadseqno())
	} else if nd.IsDeleted() == false {
		t.Errorf("expected false")
	}
}

func TestNodeClone(t *testing.T) {
	key, value := []byte("key1"), []byte("value1")
	nd := newdictnode(key, value)

	nd.Setvbno(uint16(10001)).SetVbuuid(0x12345678).SetBornseqno(100)
	nd.SetDeadseqno(101)

	newnd := nd.clone()

	if x, y := nd.Vbno(), newnd.Vbno(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := nd.Vbuuid(), newnd.Vbuuid(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := nd.Bornseqno(), newnd.Bornseqno(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if bytes.Compare(key, newnd.Key()) != 0 {
		t.Errorf("expected %q, got %q", key, nd.Key())
	}
	if bytes.Compare(value, newnd.Value()) != 0 {
		t.Errorf("expected %q, got %q", value, nd.Value())
	}

	if x, y := nd.Deadseqno(), newnd.Deadseqno(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	} else if nd.IsDeleted() == false {
		t.Errorf("expected false")
	}
}
