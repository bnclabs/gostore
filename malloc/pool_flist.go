// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "unsafe"
import "fmt"

// poolflist manages a memory block sliced up into equal sized chunks.
type poolflist struct {
	// 64-bit aligned stats
	mallocated int64

	capacity int64          // memory managed by this pool
	size     int64          // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	freelist []uint16
	freeoff  int
	prev     **poolflist
	next     *poolflist
	pools    *flistPools
}

// size of each chunk in the block and no. of chunks in the block.
func newpoolflist(
	size, n int64, pools *flistPools,
	prev **poolflist, next *poolflist) *poolflist {

	capacity := size * n
	pool := &poolflist{
		capacity: capacity,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		freelist: make([]uint16, n),
		freeoff:  int(n - 1),
		pools:    pools,
	}
	pool.prev, pool.next = prev, next
	for i := 0; i < int(n); i++ {
		pool.freelist[i] = uint16(i)
	}
	return pool
}

func (pool *poolflist) slabsize() int64 {
	return pool.size
}

func (pool *poolflist) chunklen() int64 {
	return pool.size - 8
}

func (pool *poolflist) less(other interface{}) bool {
	oth := other.(*poolflist)
	return uintptr(pool.base) < uintptr(oth.base)
}

func (pool *poolflist) allocchunk() (unsafe.Pointer, bool) {
	if pool.mallocated == pool.capacity {
		return nil, false
	}
	nthblock := int64(pool.freelist[pool.freeoff])
	pool.freelist = pool.freelist[:pool.freeoff]
	pool.freeoff--
	ptr := uintptr(pool.base) + uintptr(nthblock*pool.size)
	initblock(ptr, pool.size)
	pool.mallocated += pool.size
	mask := uintptr(Alignment - 1)
	if (ptr & mask) != 0 {
		fmsg := "allocated pointer is not %v byte aligned"
		panic(fmt.Errorf(fmsg, Alignment))
	}
	return unsafe.Pointer(ptr), true
}

func (pool *poolflist) free(ptr unsafe.Pointer) {
	if ptr == nil {
		panic("poolflist.free(): nil pointer")
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		fmsg := "poolflist.free(): unaligned pointer: %x,%v"
		panic(fmt.Errorf(fmsg, diffptr, pool.size))
	}
	nthblock := uint16(diffptr / uint64(pool.size))
	pool.freelist = append(pool.freelist, nthblock)
	pool.freeoff++
	pool.mallocated -= pool.size
	if pool.mallocated == 0 { // release to OS.
		pool.pools.unlink(pool)
		C.free(pool.base)
		pool.pools.npools--
	} else { // or unlink and re-link to free pool
		pool.pools.unlink(pool).toheadfree(pool)
	}
}

func (pool *poolflist) info() (capacity, heap, alloc, overhead int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(unsafe.Sizeof(pool.freelist))
	return pool.capacity, pool.capacity, pool.mallocated, slicesz + self
}

func (pool *poolflist) release() {
	C.free(pool.base)
	pool.freelist, pool.freeoff = nil, -1
	pool.capacity, pool.base = 0, nil
	pool.mallocated = 0
}

//---- local functions

func (pool *poolflist) checkallocated() int64 {
	return pool.capacity - int64(len(pool.freelist))*pool.size
}

type flistPools struct {
	full   *poolflist
	free   *poolflist
	npools int64 // number of active pools
}

func newFlistPool() *flistPools {
	return &flistPools{}
}

// shift next free to head
func (pools *flistPools) shiftupFree() *flistPools {
	pools.free = pools.free.next
	if pools.free != nil {
		pools.free.prev = &pools.free
	}
	return pools
}

// move head of free to head of full
func (pools *flistPools) movetofull() *flistPools {
	tempfull, tempfree := pools.full, pools.free
	// unlink from head of free list
	pools.free = pools.free.next
	if pools.free != nil {
		pools.free.prev = &pools.free
	}
	// link to head of full list
	tempfree.prev, tempfree.next = &pools.full, tempfull
	pools.full = tempfree
	if tempfull != nil {
		tempfull.prev = &pools.full.next
	}
	return pools
}

// unlink pool from this list, can be from full or free list.
func (pools *flistPools) unlink(pool *poolflist) *flistPools {
	if pool.prev != nil {
		(*(pool.prev)) = pool.next
	}
	if pool.next != nil {
		pool.next.prev = pool.prev
	}
	return pools
}

// insert pool to the head of the free list.
func (pools *flistPools) toheadfree(pool *poolflist) *flistPools {
	next := pools.free
	pools.free, pool.next = pool, next
	pool.prev = &pools.free
	if pool.next != nil {
		pool.next.prev = &pool.next
	}
	return pools
}

func (pools *flistPools) allocchunk(arena *Arena, size int64) unsafe.Pointer {
	if pools.free == nil {
		numchunks := arena.adaptiveNumchunks(size, pools.npools)
		pools.free = newpoolflist(size, numchunks, pools, &pools.free, nil)
		pools.npools++
	}
	ptr, ok := pools.free.allocchunk()
	if !ok { // full
		return pools.movetofull().allocchunk(arena, size)
	}
	*((**poolflist)(ptr)) = pools.free
	return unsafe.Pointer(uintptr(ptr) + 8)
}

func (pools *flistPools) release() {
	for pool := pools.full; pool != nil; pool = pool.next {
		pool.release()
	}
	for pool := pools.free; pool != nil; pool = pool.next {
		pool.release()
	}
}

func (pools *flistPools) info() (capacity, heap, alloc, overhead int64) {
	for pool := pools.full; pool != nil; pool = pool.next {
		c, h, a, o := pool.info()
		capacity, heap, alloc, overhead = capacity+c, heap+h, alloc+a, overhead+o
	}
	for pool := pools.free; pool != nil; pool = pool.next {
		c, h, a, o := pool.info()
		capacity, heap, alloc, overhead = capacity+c, heap+h, alloc+a, overhead+o
	}
	return
}
