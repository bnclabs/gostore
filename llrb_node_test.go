package llrb

import "testing"
import "bytes"

func Testnode(t *testing.T) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 512)
	copy(key, "hello world")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	nd.setblocksize(blocksize).setkeysize(len(key))
	if nd.blocksize() != blocksize {
		t.Errorf("expected %v, got %v", blocksize, nd.blocksize())
	} else if nd.keysize() != len(key) {
		t.Errorf("expected %v, got %v", len(key), nd.keysize())
	}

	vptr, mpool := marena.alloc(20)
	nv := (*nodevalue)(vptr)
	nv.pool = mpool

	if nd.setnodevalue(nv); nd.nodevalue() != nv {
		t.Errorf("expected %v, got %v", nv, nd.nodevalue())
	}

	if nd.setblack(); nd.isred() == true || nd.isblack() == false {
		t.Errorf("expected black")
	} else if nd.setred(); nd.isred() == false || nd.isblack() == true {
		t.Errorf("expected red")
	} else if nd.togglelink(); nd.isred() == true || nd.isblack() == false {
		t.Errorf("expected black")
	} else if nd.setdirty(); nd.isdirty() == false {
		t.Errorf("expected dirty")
	} else if nd.cleardirty(); nd.isdirty() == true {
		t.Errorf("unexpected dirty")
	}
	nd.settimestamp(0x1234567812345678)
	if nd.timestamp() != 0x123456781234 {
		t.Errorf("expected %v, got %v", 0x123456781234, nd.timestamp())
	}

	if nd.setkey(key); bytes.Compare(nd.key(), key) != 0 {
		t.Errorf("expected %v, got %v", key, nd.key())
	}

	mpool.free(ptr)
}

func TestLtkey(t *testing.T) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	// check with empty key
	nd.setblocksize(blocksize).setkey([]byte(""))
	if nd.ltkey([]byte("a")) != true {
		t.Errorf("expected true")
	} else if nd.ltkey([]byte("")) != false {
		t.Errorf("expected false")
	}
	// check with valid key
	nd.setkey(key)
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
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	// check with empty key
	nd.setblocksize(blocksize).setkey([]byte(""))
	if nd.lekey([]byte("a")) != true {
		t.Errorf("expected true")
	} else if nd.lekey([]byte("")) != true {
		t.Errorf("expected true")
	}
	// check with valid key
	nd.setkey(key)
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
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	// check with empty key
	nd.setblocksize(blocksize).setkey([]byte(""))
	if nd.gtkey([]byte("a")) != false {
		t.Errorf("expected false")
	} else if nd.gtkey([]byte("")) != false {
		t.Errorf("expected false")
	}
	// check with valid key
	nd.setkey(key)
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
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	// check with empty key
	nd.setblocksize(blocksize).setkey([]byte(""))
	if nd.gekey([]byte("a")) != false {
		t.Errorf("expected false")
	} else if nd.gekey([]byte("")) != true {
		t.Errorf("expected true")
	}
	// check with valid key
	nd.setkey(key)
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
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, []byte("abcdef")

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd = nd.setblocksize(blocksize).setkeysize(len(key))
		nd = nd.setdirty().cleardirty()
		nd = nd.setred().setblack().togglelink().settimestamp(0x1234567788)
		nd.setnodevalue(nil)
		nd.blocksize()
		nd.keysize()
		nd.isred()
		nd.isblack()
		nd.isdirty()
		nd.timestamp()
		nd.nodevalue()
	}

	mpool.free(ptr)
}

func BenchmarkNodeSetKey(b *testing.B) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 215)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd = nd.setblocksize(blocksize).setkey(key)
	}

	mpool.free(ptr)
}

func BenchmarkNodeGetKey(b *testing.B) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool
	nd = nd.setblocksize(blocksize).setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.key()
	}
}

func BenchmarkCompareLtkey(b *testing.B) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool
	nd = nd.setblocksize(blocksize).setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.ltkey(otherkey)
	}

	mpool.free(ptr)
}

func BenchmarkCompareLekey(b *testing.B) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool
	nd = nd.setblocksize(blocksize).setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.lekey(otherkey)
	}

	mpool.free(ptr)
}

func BenchmarkCompareGtkey(b *testing.B) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool
	nd = nd.setblocksize(blocksize).setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.gtkey(otherkey)
	}

	mpool.free(ptr)
}

func BenchmarkCompareGekey(b *testing.B) {
	minblock, maxblock, capacity := 96, 1024*1024*10, 1024*1024*1024
	marena := newmemarena(minblock, maxblock, capacity)
	blocksize, key := 1024, make([]byte, 512)
	otherkey := make([]byte, 512)

	ptr, mpool := marena.alloc(blocksize)
	nd := (*node)(ptr)
	nd.pool = mpool
	nd = nd.setblocksize(blocksize).setkey(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nd.gekey(otherkey)
	}

	mpool.free(ptr)
}
