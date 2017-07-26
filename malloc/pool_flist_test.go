package malloc

import "testing"
import "unsafe"
import "math/rand"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestNewpoolflist(t *testing.T) {
	size, n := int64(96), int64(65536)
	mpool := newpoolflist(size, n, nil, nil, nil)
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
	mpool := newpoolflist(size, n, pools, nil, nil)
	if len(mpool.freelist) != int(n) {
		t.Errorf("expected %v, got %v", n, len(mpool.freelist))
	}
	// allocate
	for i := int64(0); i < n; i++ {
		ptr, ok := mpool.Allocchunk()
		capacity, _, alloc, _ := mpool.Info()
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
	if _, ok := mpool.Allocchunk(); ok {
		t.Errorf("expected pool to be exhausted")
	} else if len(mpool.freelist) != 0 || mpool.freeoff != -1 {
		t.Errorf("unexpected %v %v", len(mpool.freelist), mpool.freeoff)
	}

	mpool.Free(ptrs[0])
	if len(mpool.freelist) != 1 || mpool.freeoff != 0 {
		t.Errorf("unexpected %v %v", len(mpool.freelist), mpool.freeoff)
	}

	// free
	for i, ptr := range ptrs[1:] {
		j := int64(i) + 1
		mpool.Free(ptr)
		_, _, alloc, _ := mpool.Info()
		if y := (n - j - 1) * size; alloc != y {
			t.Errorf("expected %v, got %v", y, alloc)
		}
	}
	if len(mpool.freelist) != int(n) || mpool.freeoff != int(n-1) {
		t.Errorf("unexpected %v, %v", len(mpool.freelist), mpool.freeoff)
	}

	size, n = 96, 65536
	ptrs = make([]unsafe.Pointer, 0, n)
	mpool = newpoolflist(size, n, pools, nil, nil)
	// allocate all of them
	ptrs = make([]unsafe.Pointer, 0, n)
	for i := int64(0); i < n; i++ {
		if ptr, ok := mpool.Allocchunk(); ok {
			ptrs = append(ptrs, ptr)
			continue
		}
		t.Errorf("unxpected allocation failure")
	}
	// randmly free 70% of the block
	for i := 0; i < int(float64(n)*0.99); i++ {
		mpool.Free(ptrs[rand.Intn(int(n))])
	}
	capacity, heap, alloc, overhead := mpool.Info()
	if _, ok := mpool.Allocchunk(); !ok {
		t.Errorf("unexpected false")
	} else if capacity != 6291456 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 6291456 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 62976 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 112 {
		t.Errorf("unexpected overhead %v", overhead)
	}

	// panic case
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		mpool.Free(nil)
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		mpool.Free(unsafe.Pointer(((uintptr)(ptrs[0])) + 1))
	}()

	// release
	mpool.Release()
}

func TestPoolInfo(t *testing.T) {
	size, n := int64(96), int64(1024)
	mpool := newpoolflist(size, n, nil, nil, nil)
	capacity, heap, alloc, overhead := mpool.Info()
	if capacity != 98304 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 98304 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 0 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 112 {
		t.Errorf("unexpected overhead %v", overhead)
	}
}

func TestCheckAllocated(t *testing.T) {
	size, n := int64(96), int64(56)
	mpool := newpoolflist(size, n, nil, nil, nil)
	// allocate
	for i := int64(0); i < n; i++ {
		mpool.Allocchunk()
	}
	_, _, alloc, _ := mpool.Info()
	if y := mpool.checkallocated(); alloc != y {
		t.Errorf("expected %v, got %v", alloc, y)
	}
}

func BenchmarkNewpoolflist(b *testing.B) {
	size, n := int64(96), int64(65536)
	for i := 0; i < b.N; i++ {
		newpoolflist(size, n, nil, nil, nil)
	}
}

func BenchmarkMpoolAllocX(b *testing.B) {
	size, n := int64(96), int64(65536)
	pools := newFlistPool()
	mpool := newpoolflist(size, n, pools, nil, nil)
	for i := 0; i < int(n-1); i++ {
		mpool.Allocchunk()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ptr, _ := mpool.Allocchunk()
		mpool.Free(ptr)
	}
}
