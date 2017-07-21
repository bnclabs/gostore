// +build !fbit

package malloc

import "testing"
import "unsafe"
import "math/rand"
import "sort"
import "fmt"

import "github.com/prataprc/gostore/api"

var _ = fmt.Sprintf("dummy")

func TestNewpoolflist(t *testing.T) {
	size, n := int64(96), int64(65536)
	mpool := flistfactory()(size, n).(*poolflist)
	if mpool.capacity != size*n {
		t.Errorf("expected %v, got %v", size*n, mpool.capacity)
	} else if mpool.size != size {
		t.Errorf("expected %v, got %v", size, mpool.size)
	}
}

func TestMpoolAlloc(t *testing.T) {
	size, n := int64(96), int64(56)
	ptrs := make([]unsafe.Pointer, 0, n)
	mpool := newpoolflist(size, n).(*poolflist)
	if len(mpool.freelist) != int(n) {
		t.Errorf("expected %v, got %v", n, len(mpool.freelist))
	}
	// allocate
	for i := int64(0); i < n; i++ {
		ptr, ok := mpool.Allocchunk()
		if ok == false {
			t.Errorf("unable to allocate even first block")
		} else if x, y := mpool.Allocated(), (i+1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		} else if x, y = mpool.Available(), (n-i-1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
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
		if x, y := mpool.Allocated(), (n-j-1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		} else if x, y = mpool.Available(), (j+1)*size; x != y {
			t.Errorf("expected %v, got %v", y, x)
		}
	}
	if len(mpool.freelist) != int(n) || mpool.freeoff != int(n-1) {
		t.Errorf("unexpected %v, %v", len(mpool.freelist), mpool.freeoff)
	}

	// done
	mpool.Release()

	size, n = 96, 65536
	ptrs = make([]unsafe.Pointer, 0, n)
	mpool = newpoolflist(size, n).(*poolflist)
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
	if _, ok := mpool.Allocchunk(); !ok {
		t.Errorf("unexpected false")
	} else if x := mpool.Available() + mpool.Allocated(); x != mpool.capacity {
		t.Errorf("expected %v, got %v", mpool.capacity, x)
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

func TestPoolMemory(t *testing.T) {
	size, n := int64(96), int64(65536)
	mpool := newpoolflist(size, n).(*poolflist)
	_, useful := mpool.Memory()
	if useful != 6291456 {
		t.Errorf("expected %v, got %v", 25165824, useful)
	}
}

func TestMpools(t *testing.T) {
	size, n := int64(96), int64(8)
	mpools := make(api.MemoryPools, 0)
	for i := 0; i < 1024*1024; i++ {
		mpool := newpoolflist(size, n).(*poolflist)
		mpools = append(mpools, mpool)
	}
	sort.Sort(mpools)
	if len(mpools) != 1024*1024 {
		t.Errorf("unexpected")
	}
}

func TestCheckAllocated(t *testing.T) {
	size, n := int64(96), int64(56)
	mpool := newpoolflist(size, n).(*poolflist)
	// allocate
	for i := int64(0); i < n; i++ {
		mpool.Allocchunk()
	}
	if x, y := mpool.Allocated(), mpool.checkallocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func BenchmarkNewpoolflist(b *testing.B) {
	size, n := int64(96), int64(65536)
	for i := 0; i < b.N; i++ {
		newpoolflist(size, n)
	}
}

func BenchmarkMpoolAllocX(b *testing.B) {
	size, n := int64(96), int64(65536)
	mpool := newpoolflist(size, n).(*poolflist)
	for i := 0; i < int(n-1); i++ {
		mpool.Allocchunk()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ptr, _ := mpool.Allocchunk()
		mpool.Free(ptr)
	}
}
