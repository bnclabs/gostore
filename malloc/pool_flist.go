// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "fmt"
import "unsafe"
import "sync/atomic"

type block struct {
	ptr       unsafe.Pointer
	blocknext unsafe.Pointer // *block
}

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
	for i := int64(0); i < n; i++ {
		pool.freelist[i] = uint16(i)
		ptr := unsafe.Pointer(uintptr(pool.base) + uintptr(i*pool.size))
		*((**poolflist)(ptr)) = pool
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

func (pool *poolflist) allocchunk() (ptr unsafe.Pointer, ok bool) {
	if pool.size == 1264 {
		//fmt.Printf("alloc %p %v\n", pool, pool.size)
	}
	shiftup := false
	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			if pool.freeoff < 0 {
				atomic.StoreInt64(&pool.spinlock, 0)
				return
			}
			nth := int64(pool.freelist[pool.freeoff])
			ptr = unsafe.Pointer(uintptr(pool.base) + uintptr(nth*pool.size))
			pool.mallocated += pool.size
			if pool.mallocated == pool.capacity {
				shiftup = true
			}
			pool.freeoff--
			atomic.StoreInt64(&pool.spinlock, 0)
			ok = true
			break
		}
	}
	if shiftup && pool.pools != nil {
		if pool.size == 1264 {
			//fmt.Printf("shiftup %p %v\n", pool, pool.size)
		}
		pool.pools.shiftup()
	}
	return
}

func (pool *poolflist) free(ptr unsafe.Pointer) {
	if ptr == nil {
		panic("poolflist.free(): nil pointer")
	}
	if pool.size == 1264 {
		//fmt.Printf("free %p %v\n", pool, pool.size)
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		fmsg := "poolflist.free(): unaligned pointer: %x,%v"
		panic(fmt.Errorf(fmsg, diffptr, pool.size))
	}
	nth := uint16(diffptr / uint64(pool.size))
	n := pool.capacity / pool.size
	initblock((uintptr)(ptr)+8, pool.size-8)
	tofreelist := false
	for {
		if atomic.CompareAndSwapInt64(&pool.spinlock, 0, 1) {
			pool.freeoff++
			if pool.freeoff >= n {
				panic("impossible situation")
			}
			if pool.mallocated == pool.capacity {
				tofreelist = true
			}
			pool.freelist[pool.freeoff] = nth
			pool.mallocated -= pool.size
			atomic.StoreInt64(&pool.spinlock, 0)
			break
		}
	}
	if tofreelist {
		//fmt.Println("tofreelist")
		pool.pools.addtofree(pool)
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
	spinlock int64
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
	if head.size == 1264 {
		//fmt.Printf("addtofree %p %v\n", head, head.size)
	}
	for {
		old := atomic.LoadPointer(&pools.free)
		head.next = old
		if atomic.CompareAndSwapPointer(&pools.free, old, new) {
			return
		}
	}
}

func (pools *flistPools) shiftup() {
	for {
		old := atomic.LoadPointer(&pools.free)
		if old == nil {
			return
		}
		head := (*poolflist)(old)
		new := head.next
		head.next = nil
		if atomic.CompareAndSwapPointer(&pools.free, old, new) {
			return
		}
	}
}

func (pools *flistPools) allocchunk(arena *Arena, size int64) unsafe.Pointer {
	free := atomic.LoadPointer(&pools.free)
	if free == nil {
		npools := atomic.AddInt64(&pools.npools, 1)
		numchunks := arena.adaptiveNumchunks(size, npools)
		pool := newpoolflist(size, numchunks, pools)
		pools.addtolist(pool)
		//fmt.Printf("newpoolflist %10d %10d %10d\n", size, numchunks, npools)
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
