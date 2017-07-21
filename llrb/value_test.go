package llrb

import "testing"
import "bytes"
import "fmt"

import "github.com/prataprc/gostore/malloc"
import s "github.com/prataprc/gosettings"

var _ = fmt.Sprintf("dummy")

func TestNodeValue(t *testing.T) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024 * 10),
		"allocator": "flist",
	})
	blocksize, value := int64(1024), []byte("hello world")

	ptr, mpool := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv = nv.setvalsize(int64(len(value))).setvalue(value)
	if x := nv.valsize(); x != len(value) {
		t.Errorf("expected %v, got %v", len(value), x)
	} else if v := nv.value(); bytes.Compare(value, v) != 0 {
		t.Errorf("expected %v, got %v", value, v)
	} else if nv.sizeof() != 32 {
		t.Errorf("expected %v, got %v", 8, nv.sizeof())
	}
	mpool.Free(ptr)
}

func BenchmarkValueSize(b *testing.B) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024 * 10),
		"allocator": "flist",
	})
	blocksize, value := int64(1024), []byte("hello world")
	ptr, mpool := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv.pool = mpool
	nv = nv.setvalsize(int64(len(value))).setvalue(value)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nv.setvalsize(1024)
		nv.valsize()
	}
	mpool.Free(ptr)
}

func BenchmarkSetValue(b *testing.B) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024 * 10),
		"allocator": "flist",
	})
	blocksize, value := int64(20*1024), make([]byte, 10*1024)
	ptr, mpool := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv.pool = mpool
	nv = nv.setvalsize(int64(len(value))).setvalue(value)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nv.setvalue(value)
	}
	mpool.Free(ptr)
}

func BenchmarkGetValue(b *testing.B) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, s.Settings{
		"minblock":  int64(96),
		"maxblock":  int64(1024 * 1024 * 10),
		"allocator": "flist",
	})
	blocksize, value := int64(20*1024), make([]byte, 10*1024)
	ptr, mpool := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv.pool = mpool
	nv = nv.setvalsize(int64(len(value))).setvalue(value)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nv.value()
	}
	mpool.Free(ptr)
}
