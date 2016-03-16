package storage

import "testing"
import "bytes"
import "fmt"
import "strings"
import "unsafe"

var _ = fmt.Sprintf("dummy")

func Testnode(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)
	copy(key, "hello world")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool

	vbno, fmask := uint16(0x1234), metadataMask(0)
	nd.metadata().initMetadata(vbno, fmask)
	nd.setkeysize(len(key))
	if x := nd.metadata().vbno(); x != vbno {
		t.Errorf("expected %v, got %v", vbno, x)
	} else if nd.keysize() != len(key) {
		t.Errorf("expected %v, got %v", len(key), nd.keysize())
	}

	vptr, mpool := marena.alloc(20)
	nv := (*nodevalue)(vptr)
	nv.pool = mpool

	if nd.setnodevalue(nv); nd.nodevalue() != nv {
		t.Errorf("expected %v, got %v", nv, nd.nodevalue())
	}

	nd.metadata().setblack()
	if nd.metadata().isred() == true || nd.metadata().isblack() == false {
		t.Errorf("expected black")
	}
	nd.metadata().setred()
	if nd.metadata().isred() == false || nd.metadata().isblack() == true {
		t.Errorf("expected red")
	}
	nd.metadata().togglelink()
	if nd.metadata().isred() == true || nd.metadata().isblack() == false {
		t.Errorf("expected black")
	} else if nd.metadata().setdirty(); nd.metadata().isdirty() == false {
		t.Errorf("expected dirty")
	} else if nd.metadata().cleardirty(); nd.metadata().isdirty() == true {
		t.Errorf("unexpected dirty")
	}
	nd.metadata().setaccess(1000)
	if x := nd.metadata().access(); x != 1000 {
		t.Errorf("expected %v, got %v", 1000, x)
	}

	mdsize := nd.metadata().sizeof()
	if nd.setkey(mdsize, key); bytes.Compare(nd.key(), key) != 0 {
		t.Errorf("expected %v, got %v", key, nd.key())
	}

	mpool.free(ptr)
}

func TestNodeFields(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)
	copy(key, "hello world")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno().enableVbuuid()
	fmask = fmask.enableMvalue()
	nd.metadata().initMetadata(0, fmask)
	nd.pool = mpool

	ptr, mpool = marena.alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv.pool = mpool
	nd.metadata().setmvalue((uint64)((uintptr)(unsafe.Pointer(nv))), 0)

	// metadata fields
	vbno, bornsno := uint16(0x1111), uint64(0x1111222233334444)
	deadsno, vbuuid := uint64(0x1111222233384444), uint64(0xABCDEFABCDEF4444)
	nd.Setvbno(vbno).SetBornseqno(bornsno)
	nd.SetDeadseqno(deadsno).SetVbuuid(vbuuid)
	if x := nd.Vbno(); x != vbno {
		t.Errorf("expected %v, got %v", vbno, x)
	} else if x := nd.Bornseqno(); x != bornsno {
		t.Errorf("expected %v, got %v", bornsno, x)
	} else if x := nd.Deadseqno(); x != deadsno {
		t.Errorf("expected %v, got %v", deadsno, x)
	} else if x := nd.Vbuuid(); x != vbuuid {
		t.Errorf("expected %v, got %v", deadsno, x)
	}

	// key, value
	key, value := []byte("hello world"), []byte("say cheese")
	nd.setkeysize(len(key)).setkey(nd.metadata().sizeof(), key)
	nd.nodevalue().setvalsize(int64(len(value))).setvalue(value)
	if x := nd.keysize(); x != len(key) {
		t.Errorf("expected %v, got %v", len(key), x)
	} else if x := nd.Key(); bytes.Compare(x, key) != 0 {
		t.Errorf("expected %v, got %v", key, x)
	} else if x := nd.nodevalue().valsize(); x != len(value) {
		t.Errorf("expected %v, got %v", len(value), x)
	} else if x := nd.Value(); bytes.Compare(x, value) != 0 {
		t.Errorf("expected %v, got %v", value, x)
	}

	// isred, isblack
	nd.metadata().setred()
	if isred(nd) != true {
		t.Errorf("expected %v, got %v", true, false)
	}
	nd.metadata().setblack()
	if isblack(nd) != true {
		t.Errorf("expected %v, got %v", true, false)
	}

	// repr
	if s := nd.repr(); strings.Contains(s, " ") != true {
		t.Errorf("repr: %v", s)
	}

}

func TestLtkey(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool

	// check with empty key
	nd.metadata().initMetadata(0x1234, 0)
	nd.setkey(nd.metadata().sizeof(), []byte(""))
	if nd.ltkey([]byte("a")) != true {
		t.Errorf("expected true")
	} else if nd.ltkey([]byte("")) != false {
		t.Errorf("expected false")
	}
	// check with valid key
	nd.setkey(nd.metadata().sizeof(), key)
	if nd.ltkey([]byte("a")) != false {
		t.Errorf("expected false")
	} else if nd.ltkey([]byte("")) != false {
		t.Errorf("expected false")
	} else if nd.ltkey([]byte("b")) != true {
		t.Errorf("expected true")
	} else if nd.ltkey([]byte("abcdef")) != false {
		t.Errorf("expected false")
	}
	mpool.free(ptr)
}

func TestLekey(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool

	// check with empty key
	vbno, fmask := uint16(0x1234), metadataMask(0)
	nd.metadata().initMetadata(vbno, fmask)
	nd.setkey(nd.metadata().sizeof(), []byte(""))
	if nd.lekey([]byte("a")) != true {
		t.Errorf("expected true")
	} else if nd.lekey([]byte("")) != true {
		t.Errorf("expected true")
	}
	// check with valid key
	nd.setkey(nd.metadata().sizeof(), key)
	if nd.lekey([]byte("a")) != false {
		t.Errorf("expected false")
	} else if nd.lekey([]byte("")) != false {
		t.Errorf("expected false")
	} else if nd.lekey([]byte("b")) != true {
		t.Errorf("expected true")
	} else if nd.lekey([]byte("abcdef")) != true {
		t.Errorf("expected true")
	}

	mpool.free(ptr)
}

func TestGtkey(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool

	// check with empty key
	vbno, fmask := uint16(0x1234), metadataMask(0)
	nd.metadata().initMetadata(vbno, fmask)
	nd.setkey(nd.metadata().sizeof(), []byte(""))
	if nd.gtkey([]byte("a")) != false {
		t.Errorf("expected false")
	} else if nd.gtkey([]byte("")) != false {
		t.Errorf("expected false")
	}
	// check with valid key
	nd.setkey(nd.metadata().sizeof(), key)
	if nd.gtkey([]byte("a")) != true {
		t.Errorf("expected true")
	} else if nd.gtkey([]byte("")) != true {
		t.Errorf("expected true")
	} else if nd.gtkey([]byte("b")) != false {
		t.Errorf("expected false")
	} else if nd.gtkey([]byte("abcdef")) != false {
		t.Errorf("expected false")
	}

	mpool.free(ptr)
}

func TestGekey(t *testing.T) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool

	// check with empty key
	vbno, fmask := uint16(0x1234), metadataMask(0)
	nd.metadata().initMetadata(vbno, fmask)
	nd.setkey(nd.metadata().sizeof(), []byte(""))
	if nd.gekey([]byte("a")) != false {
		t.Errorf("expected false")
	} else if nd.gekey([]byte("")) != true {
		t.Errorf("expected true")
	}
	// check with valid key
	nd.setkey(nd.metadata().sizeof(), key)
	if nd.gekey([]byte("a")) != true {
		t.Errorf("expected true")
	} else if nd.gekey([]byte("")) != true {
		t.Errorf("expected true")
	} else if nd.gekey([]byte("b")) != false {
		t.Errorf("expected false")
	} else if nd.gekey([]byte("abcdef")) != true {
		t.Errorf("expected true")
	}

	mpool.free(ptr)
}

func BenchmarkNodefields(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.metadata().initMetadata(0x1234, metadataMask(0).enableMvalue())
		nd.setkeysize(len(key))
		nd.keysize()
		nd.setnodevalue(nil)
		nd.nodevalue()
		md := nd.metadata()
		md = md.setdirty().cleardirty()
		md = md.setred().setblack().togglelink().setaccess(1000)
		md.vbno()
		md.isred()
		md.isblack()
		md.isdirty()
		md.access()
	}

	mpool.free(ptr)
}

func BenchmarkNodeSetKey(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 215)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool
	mdsize := nd.metadata().sizeof()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.metadata().initMetadata(0x1234, 0)
		nd.setkey(mdsize, key)
	}

	mpool.free(ptr)
}

func BenchmarkNodeGetKey(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool
	nd.metadata().initMetadata(0x1234, 0)
	nd.setkey(nd.metadata().sizeof(), key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.key()
	}
}

func BenchmarkCompareLtkey(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool
	nd.metadata().initMetadata(0x1234, 0)
	nd.setkey(nd.metadata().sizeof(), key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.ltkey(otherkey)
	}

	mpool.free(ptr)
}

func BenchmarkCompareLekey(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool
	nd.metadata().initMetadata(0x1234, 0)
	nd.setkey(nd.metadata().sizeof(), key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.lekey(otherkey)
	}

	mpool.free(ptr)
}

func BenchmarkCompareGtkey(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool
	nd.metadata().initMetadata(0x1234, 0)
	nd.setkey(nd.metadata().sizeof(), key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.gtkey(otherkey)
	}

	mpool.free(ptr)
}

func BenchmarkCompareGekey(b *testing.B) {
	minblock, maxblock := int64(96), int64(1024*1024*10)
	capacity, pcapacity := int64(1024*1024*1024), int64(1024*1024)
	marena := newmemarena(minblock, maxblock, capacity, pcapacity)
	blocksize, key := int64(1024), make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*Llrbnode)(ptr)
	nd.pool = mpool
	nd.metadata().initMetadata(0x1234, 0)
	nd.setkey(nd.metadata().sizeof(), key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.gekey(otherkey)
	}

	mpool.free(ptr)
}
