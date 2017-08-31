// +build fbit

package malloc

import "testing"
import "unsafe"
import "math/rand"
import "reflect"
import "sort"
import "fmt"

import "github.com/prataprc/gostore/api"

var _ = fmt.Sprintf("dummy")

func TestNewpoolfbit(t *testing.T) {
	size, n := int64(96), int64(512*512)
	mpool := newpoolfbit(size, n, nil, nil, nil)
	if mpool.capacity != size*n {
		t.Errorf("expected %v, got %v", size*n, mpool.capacity)
	} else if x := int64(mpool.fbits.freeblocks()); x != n {
		t.Errorf("expected %v, got %v", n, x)
	} else if mpool.size != size {
		t.Errorf("expected %v, got %v", size, mpool.size)
	}
	// panic case
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		newpoolfbit(size, 9, nil, nil, nil)
	}()
}

func TestMpoolAlloc(t *testing.T) {
	size, n := int64(96), int64(56)
	pools := newFlistPool()
	ptrs := make([]unsafe.Pointer, 0, n)
	mpool := newpoolfbit(size, n, pools, nil, nil)
	flref := [][]uint8{[]uint8{255, 255, 255, 255, 255, 255, 255}}
	if reflect.DeepEqual(mpool.fbits.bitmaps, flref) == false {
		t.Errorf("expected %v, got %v", flref, mpool.fbits.bitmaps)
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
	if reflect.DeepEqual(mpool.fbits.bitmaps, flref) == false {
		t.Errorf("expected %v, got %v", flref, mpool.fbits.bitmaps)
	}
	mpool.release()

	size, n = 96, 512*512
	ptrs = make([]unsafe.Pointer, 0, n)
	mpool = newpoolfbit(size, n, pools, nil, nil)
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
	if _, ok := mpool.alloc(); !ok {
		t.Errorf("unexpected false")
	} else if x := mpool.available() + mpool.allocated(); x != mpool.capacity {
		t.Errorf("expected %v, got %v", mpool.capacity, x)
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

func TestPoolMemory(t *testing.T) {
	size, n := int64(96), int64(512*512)
	mpool := newpoolfbit(size, n, nil, nil, nil)
	capacity, heap, alloc, overhead := mpool.info()
	if capacity != 0 {
		t.Errorf("unexpected capacity %v", capacity)
	} else if heap != 0 {
		t.Errorf("unexpected heap %v", heap)
	} else if alloc != 0 {
		t.Errorf("unexpected alloc %v", alloc)
	} else if overhead != 0 {
		t.Errorf("unexpected overhead %v", overhead)
	}
}

func TestCheckAllocated(t *testing.T) {
	size, n := int64(96), int64(56)
	mpool := newpoolfbit(size, n, nil, nil, nil)
	// allocate
	for i := int64(0); i < n; i++ {
		mpool.alloc()
	}
	_, _, alloc, _ := mpool.info()
	if y := mpool.checkallocated(); alloc != y {
		t.Errorf("expected %v, got %v", alloc, y)
	}
}

func BenchmarkNewpoolfbit(b *testing.B) {
	size, n := int64(96), int64(512*512)
	for i := 0; i < b.N; i++ {
		newpoolfbit(size, n, nil, nil, nil)
	}
}

func BenchmarkMpoolAllocX(b *testing.B) {
	size, n := int64(96), int64(512*512)
	mpool := newpoolfbit(size, n, nil, nil, nil)
	for i := 0; i < int(n-1); i++ {
		mpool.alloc()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ptr, _ := mpool.alloc()
		mpool.free(ptr)
	}
}
