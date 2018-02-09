package llrb

import "testing"
import "bytes"
import "fmt"

import "github.com/bnclabs/gostore/malloc"

var _ = fmt.Sprintf("dummy")

func TestNodeValue(t *testing.T) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, "flist")
	blocksize, value := int64(1024), []byte("hello world")

	ptr := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv = nv.setvalsize(int64(len(value))).setvalue(value)
	if x := nv.valsize(); x != len(value) {
		t.Errorf("expected %v, got %v", len(value), x)
	} else if v := nv.value(); bytes.Compare(value, v) != 0 {
		t.Errorf("expected %v, got %v", value, v)
	} else if nv.sizeof() != 16 {
		t.Errorf("expected %v, got %v", 16, nv.sizeof())
	}
	marena.Free(ptr)
}

func BenchmarkValueSize(b *testing.B) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, "flist")
	blocksize, value := int64(1024), []byte("hello world")
	ptr := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv = nv.setvalsize(int64(len(value))).setvalue(value)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nv.setvalsize(1024)
		nv.valsize()
	}
	marena.Free(ptr)
}

func BenchmarkSetValue(b *testing.B) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, "flist")
	blocksize, value := int64(20*1024), make([]byte, 10*1024)
	ptr := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv = nv.setvalsize(int64(len(value))).setvalue(value)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nv.setvalue(value)
	}
	marena.Free(ptr)
}

func BenchmarkGetValue(b *testing.B) {
	capacity := int64(1024 * 1024 * 1024)
	marena := malloc.NewArena(capacity, "flist")
	blocksize, value := int64(20*1024), make([]byte, 10*1024)
	ptr := marena.Alloc(blocksize)
	nv := (*nodevalue)(ptr)
	nv = nv.setvalsize(int64(len(value))).setvalue(value)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		nv.value()
	}
	marena.Free(ptr)
}
