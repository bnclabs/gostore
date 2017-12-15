// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "fmt"
import "unsafe"

import "runtime"
import "sync/atomic"

// poolflist manages a memory block sliced up into equal sized chunks.
type poolflist struct {
	// 64-bit aligned stats
	mallocated int64
	spinlock   int64

	capacity int64 // memory managed by this pool
	n        int64
	size     int64          // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	pools    *flistPools
	freelist []uint16
	freeoff  int64
	next     unsafe.Pointer // *poolflist free list of pool
	nextnode unsafe.Pointer // *poolflist full list of pool
}

// size of each chunk in the block and no. of chunks in the block.
func newpoolflist(size, n int64, pools *flistPools) *poolflist {
	capacity := size * n
	pool := &poolflist{
		capacity: capacity,
		n:        n,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		pools:    pools,
		freelist: make([]uint16, n),
		freeoff:  -1,
	}
	zeropoolblock((uintptr)(pool.base), capacity)
	for i := int64(0); i < n; i++ {
		pool.freelist[i] = uint16(i)
		ptr := unsafe.Pointer(uintptr(pool.base) + uintptr(i*pool.size))
		*((*uintptr)(ptr)) = (uintptr)(unsafe.Pointer(pool))
	}
	pool.freeoff = n - 1
	return pool
}

func (pool *poolflist) slabsize() int64 {
	return pool.size
}

func (pool *poolflist) chunklen() int64 {
	return pool.size - 8
}

// this pool is the first pool in flistPools' free-list.
func (pool *poolflist) allocchunk() (ptr unsafe.Pointer, ok bool) {
	shiftup := func() bool {
		if pools := pool.pools; pools != nil {
			old := unsafe.Pointer(pool)
			new := atomic.LoadPointer(&pool.next)
			return atomic.CompareAndSwapPointer(&pools.free, old, new)
		}
		return true
	}

	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			if pool.freeoff < 0 {
				shiftup()
				atomic.StoreInt64(&pool.spinlock, 0)
				return nil, false
			}
			if (pool.mallocated + pool.size) == pool.capacity {
				if !shiftup() {
					atomic.StoreInt64(&pool.spinlock, 0)
					return nil, false
				}
			}
			nth := int64(pool.freelist[pool.freeoff])
			ptr = unsafe.Pointer(uintptr(pool.base) + uintptr(nth*pool.size))
			pool.mallocated += pool.size
			pool.freeoff--
			atomic.StoreInt64(&pool.spinlock, 0)
			//fmt.Printf("allocchunk %p %v \n", pool, pool.size)
			return ptr, true
		}
		runtime.Gosched()
	}
	return
}

func (pool *poolflist) free(ptr unsafe.Pointer) {
	// TODO: panic checks can be removed once this implementation
	// becomes stable.
	if ptr == nil {
		panic("poolflist.free(): nil pointer")
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		fmsg := "poolflist.free(): unaligned pointer: %x,%v"
		panic(fmt.Errorf(fmsg, diffptr, pool.size))
	}
	nth := uint16(diffptr / uint64(pool.size))
	n := pool.capacity / pool.size
	initblock((uintptr)(ptr)+8, pool.size-8)
	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			pool.freeoff++
			if pool.freeoff >= n {
				panic("impossible situation")
			}
			pool.freelist[pool.freeoff] = nth
			pool.mallocated -= pool.size
			if pool.mallocated == (pool.capacity - pool.size) {
				//fmt.Printf("tofreelist %p %v\n", pool, pool.size)
				pool.pools.addtofree(pool)
			}
			atomic.StoreInt64(&pool.spinlock, 0)
			break
		}
		runtime.Gosched()
	}
}

func (pool *poolflist) info() (capacity, heap, alloc, overhead int64) {
	self := int64(unsafe.Sizeof(*pool))
	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			mallocated := pool.mallocated
			atomic.StoreInt64(&pool.spinlock, 0)
			return pool.capacity, pool.capacity, mallocated, self
		}
		runtime.Gosched()
	}
}

func (pool *poolflist) release() {
	C.free(pool.base)
	pool.mallocated = 0
	pool.capacity, pool.base, pool.pools, pool.freeoff = 0, nil, nil, -1
}

//---- local functions

// Not for production purpose only for testing.
func (pool *poolflist) checkallocated() int64 {
	allocated := (pool.freeoff + 1) * pool.size
	return pool.capacity - allocated
}

type flistPools struct {
	free     unsafe.Pointer // *poolflist
	listhead unsafe.Pointer // *poolflist
	npools   int64
}

func newFlistPool() *flistPools {
	return &flistPools{}
}

func (pools *flistPools) addtolist(head *poolflist) {
	new := unsafe.Pointer(head)
	for {
		old := atomic.LoadPointer(&pools.listhead)
		head.nextnode = old
		if atomic.CompareAndSwapPointer(&pools.listhead, old, new) {
			return
		}
	}
}

func (pools *flistPools) addtofree(head *poolflist) {
	new := unsafe.Pointer(head)
	for {
		old := atomic.LoadPointer(&pools.free)
		atomic.StorePointer(&head.next, old)
		if atomic.CompareAndSwapPointer(&pools.free, old, new) {
			return
		}
	}
}

func (pools *flistPools) allocchunk(arena *Arena, size int64) unsafe.Pointer {
	free := atomic.LoadPointer(&pools.free)
	//fmt.Printf("all %p\n", free)
	if free == nil {
		npools := atomic.AddInt64(&pools.npools, 1)
		numchunks := arena.adaptiveNumchunks(size, npools)
		pool := newpoolflist(size, numchunks, pools)
		pools.addtolist(pool)
		//fmt.Printf("newpoolflist %p %p %10d %10d %10d\n", pool, pools, size, numchunks, npools)
		pools.addtofree(pool)
		free = atomic.LoadPointer(&pools.free)
	}
	pool := (*poolflist)(free)
	ptr, ok := pool.allocchunk()
	if !ok { // full
		return pools.allocchunk(arena, size)
	}
	return unsafe.Pointer(uintptr(ptr) + 8)
}

func (pools *flistPools) release() {
	for p := pools.listhead; p != nil; p = (*poolflist)(p).nextnode {
		(*poolflist)(p).release()
	}
}

func (pools *flistPools) info() (capacity, heap, alloc, overh int64) {
	for p := pools.listhead; p != nil; p = (*poolflist)(p).nextnode {
		c, h, a, o := (*poolflist)(p).info()
		capacity, heap, alloc, overh = capacity+c, heap+h, alloc+a, overh+o
	}
	return
}
