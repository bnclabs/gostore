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
	mpool := fbitfactory()(size, n)
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
		newpoolfbit(size, 9)
	}()
}

func TestMpoolAlloc(t *testing.T) {
	size, n := int64(96), int64(56)
	ptrs := make([]unsafe.Pointer, 0, n)
	mpool := newpoolfbit(size, n)
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
	mpool = newpoolfbit(size, n)
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
	mpool := newpoolfbit(size, n)
	_, useful := mpool.memory()
	if useful != 25165824 {
		t.Errorf("expected %v, got %v", 25165824, useful)
	}
}

func TestMpools(t *testing.T) {
	size, n := int64(96), int64(8)
	mpools := make(mempools, 0)
	for i := 0; i < 1024*1024; i++ {
		mpool := newpoolfbit(size, n)
		mpools = append(mpools, mpool)
	}
	sort.Sort(mpools)
	if len(mpools) != 1024*1024 {
		t.Errorf("unexpected")
	}
}

func TestCheckAllocated(t *testing.T) {
	size, n := int64(96), int64(56)
	mpool := newpoolfbit(size, n)
	// allocate
	for i := int64(0); i < n; i++ {
		mpool.alloc()
	}
	if x, y := mpool.allocated(), mpool.checkallocated(); x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
}

func BenchmarkNewpoolfbit(b *testing.B) {
	size, n := int64(96), int64(512*512)
	for i := 0; i < b.N; i++ {
		newpoolfbit(size, n)
	}
}

func BenchmarkMpoolAllocX(b *testing.B) {
	size, n := int64(96), int64(512*512)
	mpool := newpoolfbit(size, n)
	for i := 0; i < int(n-1); i++ {
		mpool.alloc()
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ptr, _ := mpool.alloc()
		mpool.free(ptr)
	}
}
