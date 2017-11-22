package malloc

import "testing"
import "unsafe"
import "math/rand"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestNewpoolflist(t *testing.T) {
	size, n := int64(96), int64(Maxchunks)
	mpool := newpoolflist(size, n, nil)
	if mpool.capacity != size*n {
		t.Errorf("expected %v, got %v", size*n, mpool.capacity)
	} else if mpool.size != size {
		t.Errorf("expected %v, got %v", size, mpool.size)
	}
}

func TestMpoolAlloc(t *testing.T) {
	size, n := int64(96), int64(56)
	pools := newFlistPool()
	ptrs := make([]unsafe.Pointer, 0, n)
	mpool := newpoolflist(size, n, pools)
	if x := mpool.checkallocated(); x != 0 {
		t.Errorf("expected %v, got %v", 0, x)
	}
	// allocate
	for i := int64(0); i < n; i++ {
		ptr, ok := mpool.allocchunk()
		capacity, _, alloc, _ := mpool.info()
		available := capacity - alloc
		if ok == false {
			t.Errorf("unable to allocate even first block")
		} else if y := (i + 1) * size; alloc != y {
			t.Errorf("expected %v, got %v", y, alloc)
		} else if y = (n - i - 1) * size; available != y {
			t.Errorf("expected %v, got %v", y, available)
		}
		ptrs = append(ptrs, ptr)
	}
	if _, ok := mpool.allocchunk(); ok {
		t.Errorf("expected pool to be exhausted")
	} else if mpool.freeoff != -1 {
		t.Errorf("unexpected %p", mpool.freeoff)
	}

	mpool.free(ptrs[0])
	if mpool.freeoff == -1 {
		t.Errorf("unexpected %p", mpool.freeoff)
	}

	// free
	for i, ptr := range ptrs[1:] {
		j := int64(i) + 1
		mpool.free(ptr)
		_, _, alloc, _ := mpool.info()
		if y := (n - j - 1) * size; alloc != y {
			t.Errorf("expected %v, got %v", y, alloc)
		}
	}
	if x := mpool.checkallocated(); x != 0 {
		t.Errorf("unexpected %v", x)
	}

	size, n = 96, int64(Maxchunks)
	ptrs = make([]unsafe.Pointer, 0, n)
	mpool = newpoolflist(size, n, pools)
	// allocate all of them
	ptrs = make([]unsafe.Pointer, 0, n)
	for i := int64(0); i < n; i++ {
		if ptr, ok := mpool.allocchunk(); ok {
			ptrs = append(ptrs, ptr)
			continue
		}
		t.Errorf("unxpected allocation failure")
	}
	// randmly free 70% of the block
	for i := 0; i < int(float64(n)*0.99); i++ {
		off := rand.Intn(int(n))
		if ptrs[off] != nil {
			mpool.free(ptrs[off])
			ptrs[off] = nil
		}
	}
	capacity, heap, alloc, overhead := mpool.info()
	if _, ok := mpool.allocchunk(); !ok {
		t.Errorf("unexpected false")
	} else if capacity != 1966080 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 1966080 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 733824 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 104 {
		t.Errorf("unexpected overhead %v", overhead)
	}

	// panic case
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		mpool.free(nil)
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		mpool.free(unsafe.Pointer(((uintptr)(ptrs[0])) + 1))
	}()

	// release
	mpool.release()
}

func TestPoolinfo(t *testing.T) {
	size, n := int64(96), int64(1024)
	mpool := newpoolflist(size, n, nil)
	capacity, heap, alloc, overhead := mpool.info()
	if capacity != 98304 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 98304 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 0 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 104 {
		t.Errorf("unexpected overhead %v", overhead)
	}
}

func TestCheckAllocated(t *testing.T) {
	size, n := int64(96), int64(56)
	mpool := newpoolflist(size, n, nil)
	// allocate
	for i := int64(0); i < n; i++ {
		mpool.allocchunk()
	}
	_, _, alloc, _ := mpool.info()
	if y := mpool.checkallocated(); alloc != y {
		t.Errorf("expected %v, got %v", alloc, y)
	}
}

func BenchmarkNewpoolflist(b *testing.B) {
	size, n := int64(96), int64(1024)
	for i := 0; i < b.N; i++ {
		newpoolflist(size, n, nil)
	}
}

func BenchmarkMpoolAllocX(b *testing.B) {
	size, n := int64(96), int64(Maxchunks)
	pools := newFlistPool()
	mpool := newpoolflist(size, n, pools)
	for i := 0; i < int(n-1); i++ {
		mpool.allocchunk()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ptr, _ := mpool.allocchunk()
		mpool.free(ptr)
	}
}

func BenchmarkFlistAlloc(b *testing.B) {
	capacity := int64(10 * 1024 * 1024 * 1024)
	marena := NewArena(capacity, "flist")
	pools := newFlistPool()

	size, n := int64(96), int64(65536)
	for i := 0; i < int(n-1); i++ {
		pools.allocchunk(marena, size)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		pools.allocchunk(marena, size)
	}

	marena.Release()
}
