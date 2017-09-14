package llrb

import "testing"
import "reflect"
import "unsafe"
import "bytes"

func TestLlrbnode(t *testing.T) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))

	block2 := make([]byte, 1024)
	sl = (*reflect.SliceHeader)(unsafe.Pointer(&block2))
	value := []byte("okay")
	nv := (*nodevalue)(unsafe.Pointer(sl.Data))
	nv.setvalue(value)

	if p := nd.setnodevalue(nv).nodevalue(); p != nv {
		t.Errorf("expected %p, got %p", nv, p)
	}
	klen := uint16(0x8121)
	if x := nd.setkeylen(klen).getkeylen(); klen != x {
		t.Errorf("expected %x, got %x", klen, x)
	}
	access := uint64(0x8123456781)
	if x := nd.setaccess(access).getaccess(); access != x {
		t.Errorf("expected %x, got %x", access, x)
	}
	key := []byte("hello world")
	if x := nd.setkey(key).getkey(); bytes.Compare(x, key) != 0 {
		t.Errorf("expected %s, got %s", key, x)
	}
	seqno := uint64(0x812345618123451)
	if x := nd.setseqno(seqno).getseqno(); x != seqno {
		t.Errorf("expected %x, got %x", seqno, x)
	}
	if nd.isblack() == true {
		t.Errorf("unexpected true")
	} else if nd.isred() == false {
		t.Errorf("unexpected false")
	} else if nd.setblack().isblack() == false {
		t.Errorf("unexpected false")
	} else if nd.setblack().isred() == true {
		t.Errorf("unexpected true")
	} else if nd.setred().isblack() == true {
		t.Errorf("unexpected true")
	} else if nd.setred().isred() == false {
		t.Errorf("unexpected false")
	} else if nd.setred().togglelink().isblack() == false {
		t.Errorf("unexpected false")
	} else if nd.setblack().togglelink().isred() == false {
		t.Errorf("unexpected false")
	}
	if nd.setdirty().isdirty() == false {
		t.Errorf("unexpected false")
	} else if nd.setdirty().cleardirty().isdirty() == true {
		t.Errorf("unexpected true")
	}
	if nd.setdeleted().isdeleted() == false {
		t.Errorf("unexpected false")
	} else if nd.setdeleted().cleardeleted().isdeleted() == true {
		t.Errorf("unexpected true")
	}

	if x := nd.getkeylen(); uint16(len(key)) != x {
		t.Errorf("expected %v, got %v", klen, x)
	} else if x := nd.getaccess(); access != x {
		t.Errorf("expected %x, got %x", access, x)
	} else if x := nd.getkey(); bytes.Compare(x, key) != 0 {
		t.Errorf("expected %x, got %x", key, x)
	} else if x := nd.getseqno(); x != seqno {
		t.Errorf("expected %x, got %x", seqno, x)
	} else if x := nd.Value(); bytes.Compare(x, value) != 0 {
		t.Errorf("expected %s, got %s", value, x)
	}

	nd.setdirty().setdeleted().setblack()
	t.Logf("nd.repr: %v", nd.repr())
	nd.pprint("")
}

func TestNodeLtkey(t *testing.T) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key := []byte("abcdef")

	// check with empty key
	nd.setkey([]byte(""))
	if nd.ltkey([]byte("a"), false) != true {
		t.Errorf("expected true")
	} else if nd.ltkey([]byte(""), false) != false {
		t.Errorf("expected false")
	}
	// check with valid key
	nd.setkey(key)
	if nd.ltkey([]byte("a"), false) != false {
		t.Errorf("expected false")
	} else if nd.ltkey([]byte(""), false) != false {
		t.Errorf("expected false")
	} else if nd.ltkey([]byte("b"), false) != true {
		t.Errorf("expected true")
	} else if nd.ltkey([]byte("abcdef"), false) != false {
		t.Errorf("expected false")
	}
}

func TestNodeLekey(t *testing.T) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key := []byte("abcdef")

	// check with empty key
	nd.setkey([]byte(""))
	if nd.lekey([]byte("a"), false) != true {
		t.Errorf("expected true")
	} else if nd.lekey([]byte(""), false) != true {
		t.Errorf("expected true")
	}
	// check with valid key
	nd.setkey(key)
	if nd.lekey([]byte("a"), false) != false {
		t.Errorf("expected false")
	} else if nd.lekey([]byte(""), false) != false {
		t.Errorf("expected false")
	} else if nd.lekey([]byte("b"), false) != true {
		t.Errorf("expected true")
	} else if nd.lekey([]byte("abcdef"), false) != true {
		t.Errorf("expected true")
	}
}

func TestNodeGtkey(t *testing.T) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key := []byte("abcdef")

	// check with empty key
	nd.setkey([]byte(""))
	if nd.gtkey([]byte("a"), false) != false {
		t.Errorf("expected false")
	} else if nd.gtkey([]byte(""), false) != false {
		t.Errorf("expected false")
	}
	// check with valid key
	nd.setkey(key)
	if nd.gtkey([]byte("a"), false) != true {
		t.Errorf("expected true")
	} else if nd.gtkey([]byte(""), false) != true {
		t.Errorf("expected true")
	} else if nd.gtkey([]byte("b"), false) != false {
		t.Errorf("expected false")
	} else if nd.gtkey([]byte("abcdef"), false) != false {
		t.Errorf("expected false")
	}
}

func TestNodeGekey(t *testing.T) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key := []byte("abcdef")

	// check with empty key
	nd.setkey([]byte(""))
	if nd.gekey([]byte("a"), false) != false {
		t.Errorf("expected false")
	} else if nd.gekey([]byte(""), false) != true {
		t.Errorf("expected true")
	}
	// check with valid key
	nd.setkey(key)
	if nd.gekey([]byte("a"), false) != true {
		t.Errorf("expected true")
	} else if nd.gekey([]byte(""), false) != true {
		t.Errorf("expected true")
	} else if nd.gekey([]byte("b"), false) != false {
		t.Errorf("expected false")
	} else if nd.gekey([]byte("abcdef"), false) != true {
		t.Errorf("expected true")
	}
}

func BenchmarkNodeSetaccess(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.setaccess(0x123456789)
	}
}

func BenchmarkNodeGetaccess(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	nd.setaccess(0x123456789)
	for i := 0; i < b.N; i++ {
		nd.getaccess()
	}
}

func BenchmarkNodeSetkey(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa")
	for i := 0; i < b.N; i++ {
		nd.setkey(key)
	}
}

func BenchmarkNodeGetkey(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaa")
	nd.setkey(key)
	for i := 0; i < b.N; i++ {
		nd.getkey()
	}
}

func BenchmarkNodeSetseqno(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.setseqno(0x1234567789)
	}
}

func BenchmarkNodeGetseqno(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	nd.setseqno(0x123456778)
	for i := 0; i < b.N; i++ {
		nd.getseqno()
	}
}

func BenchmarkNodeSetred(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.setred()
	}
}

func BenchmarkNodeIsred(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.isred()
	}
}

func BenchmarkNodeSetdirty(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.setdirty()
	}
}

func BenchmarkNodeIsdirty(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.isdirty()
	}
}

func BenchmarkNodeSetdeleted(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.setdeleted()
	}
}

func BenchmarkNodeIsdeleted(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	for i := 0; i < b.N; i++ {
		nd.isdeleted()
	}
}

func BenchmarkNodeLt(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key, otherkey := make([]byte, 512), make([]byte, 512)
	nd.setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.ltkey(otherkey, false)
	}
}

func BenchmarkNodeLe(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key, otherkey := make([]byte, 512), make([]byte, 512)
	nd.setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.lekey(otherkey, false)
	}
}

func BenchmarkNodeGt(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key, otherkey := make([]byte, 512), make([]byte, 512)
	nd.setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.gtkey(otherkey, false)
	}
}

func BenchmarkNodeGe(b *testing.B) {
	block1 := make([]byte, 1024)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&block1))
	nd := (*Llrbnode)(unsafe.Pointer(sl.Data))
	key, otherkey := make([]byte, 512), make([]byte, 512)
	nd.setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.gekey(otherkey, false)
	}
}
