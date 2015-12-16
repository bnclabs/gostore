package storage

import "testing"
import "unsafe"
import "math/rand"
import "bytes"
import "sort"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestNewmempool(t *testing.T) {
	size, n := int64(96), int64(1024*1024)
	mpool := newmempool(size, n)
	if mpool.capacity != size*n {
		t.Errorf("expected %v, got %v", size*n, mpool.capacity)
	} else if int64(len(mpool.freelist)) != (n / 8) {
		t.Errorf("expected %v, got %v", n/8, len(mpool.freelist))
	} else if mpool.freeoff != 0 {
		t.Errorf("expected %v, got %v", 0, mpool.freeoff)
	} else if mpool.size != size {
		t.Errorf("expected %v, got %v", size, mpool.size)
	}
}

func TestMpoolAlloc(t *testing.T) {
	size, n := int64(96), int64(56)
	ptrs := make([]unsafe.Pointer, 0, n)
	mpool := newmempool(size, n)
	flref := []byte{255, 255, 255, 255, 255, 255, 255}
	if bytes.Compare(mpool.freelist, flref) != 0 {
		t.Errorf("expected %v, got %v", flref, mpool.freelist)
	}
	// allocate
	for i := int64(0); i < n; i++ {
		ptr, ok := mpool.alloc()
		if ok == false {
			t.Errorf("unable to allocate even first block")
		} else if x, y := mpool.allocated(), (i+1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		} else if x, y = mpool.available(), (n-i-1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		}
		ptrs = append(ptrs, ptr)
	}
	if _, ok := mpool.alloc(); ok {
		t.Errorf("expected pool to be exhausted")
	}
	// free
	for i, ptr := range ptrs {
		j := int64(i)
		mpool.free(ptr)
		if x, y := mpool.allocated(), (n-j-1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		} else if x, y = mpool.available(), (j+1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		}
	}
	// done
	if bytes.Compare(mpool.freelist, flref) != 0 {
		t.Errorf("expected %v, got %v", flref, mpool.freelist)
	}
	mpool.release()

	size, n = 96, 1024*1024
	ptrs = make([]unsafe.Pointer, 0, n)
	mpool = newmempool(size, n)
	// allocate all of them
	ptrs = make([]unsafe.Pointer, 0, n)
	for i := int64(0); i < n; i++ {
		if ptr, ok := mpool.alloc(); ok {
			ptrs = append(ptrs, ptr)
			continue
		}
		t.Errorf("unxpected allocation failure")
	}
	// randmly free 70% of the block
	for i := 0; i < int(float64(n)*0.99); i++ {
		mpool.free(ptrs[rand.Intn(int(n))])
	}
	if mpool.freeoff == -1 {
		t.Errorf("unexpected -1 %v", mpool.freeoff)
	} else if x := mpool.available() + mpool.allocated(); x != mpool.capacity {
		t.Errorf("expected %v, got %v", mpool.capacity, x)
	}

	// release
	mpool.release()
}

func TestPoolMemory(t *testing.T) {
	size, n := int64(96), int64(1024*1024)
	mpool := newmempool(size, n)
	if x, y := mpool.memory(); x != 131136 {
		t.Errorf("expected %v, got %v", 131136, x)
	} else if y != 100663296 {
		t.Errorf("expected %v, got %v", 100663296, y)
	}
}

func TestMpools(t *testing.T) {
	size, n := int64(96), int64(8)
	mpools := make(mempools, 0)
	for i := 0; i < 1024*1024; i++ {
		mpool := newmempool(size, n)
		mpools = append(mpools, mpool)
	}
	sort.Sort(mpools)
	if len(mpools) != 1024*1024 {
		t.Errorf("unexpected")
	}
}

func TestCheckAllocated(t *testing.T) {
	size, n := int64(96), int64(56)
	mpool := newmempool(size, n)
	// allocate
	for i := int64(0); i < n; i++ {
		mpool.alloc()
	}
	if x, y := mpool.allocated(), mpool.checkallocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func BenchmarkNewmempool(b *testing.B) {
	size, n := int64(96), int64(1024*1024)
	for i := 0; i < b.N; i++ {
		newmempool(size, n)
	}
}

func BenchmarkMpoolAllocX(b *testing.B) {
	size, n := int64(96), int64(1024*1024)
	mpool := newmempool(size, n)
	// set first byte in free list to 0x1 and last byte to 0x1,
	// emulates the worst case performance.
	for i := int64(0); i < n/8; i++ {
		mpool.freelist[i] = 0
	}
	mpool.freelist[0] = 0x1
	mpool.freelist[len(mpool.freelist)-1] = 0x1
	mpool.freeoff = 0

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := mpool.alloc(); !ok {
			b.Errorf("unexpected failure in alloc")
		} else if x := int64(len(mpool.freelist) - 1); mpool.freeoff != x {
			b.Errorf("expected %v, got %v", x, mpool.freeoff)
		}
		mpool.freelist[0] = 0x1
		mpool.freelist[len(mpool.freelist)-1] = 0x1
		mpool.freeoff = 0
	}
}

func BenchmarkMpoolAllocO(b *testing.B) {
	size, n := int64(96), int64(128)
	mpool := newmempool(size, n)
	// set first byte in free list to 0x1 and last byte to 0x1,
	// emulates the worst case performance.
	for i := int64(0); i < n/8; i++ {
		mpool.freelist[i] = 0
	}
	mpool.freelist[0] = 0x1
	mpool.freelist[len(mpool.freelist)-1] = 0x1
	mpool.freeoff = 0

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := mpool.alloc(); !ok {
			b.Errorf("unexpected failure in alloc")
		} else if x := int64(len(mpool.freelist) - 1); mpool.freeoff != x {
			b.Errorf("expected %v, got %v", x, mpool.freeoff)
		}
		mpool.freelist[0] = 0x1
		mpool.freelist[len(mpool.freelist)-1] = 0x1
		mpool.freeoff = 0
	}
}

func BenchmarkMpoolFree(b *testing.B) {
	size, n := int64(96), int64(128)
	mpool := newmempool(size, n)
	ptr, ok := mpool.alloc()
	if !ok {
		b.Errorf("unexpected failure in alloc")
	}
	for i := 0; i < b.N; i++ {
		mpool.free(ptr)
	}
}
