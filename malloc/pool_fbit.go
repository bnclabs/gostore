// +build fbit
// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "fmt"
import "unsafe"

import "github.com/prataprc/gostore/api"

//import "fmt"

// poolfbit manages a memory block sliced up into equal sized chunks.
type poolfbit struct {
	// 64-bit aligned stats
	mallocated int64

	capacity int64          // memory managed by this pool
	size     int64          // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	fbits    *freebits
	prev     **poolfbit
	next     *poolfbit
	pools    *fbitPools
}

// size of each chunk in the block and no. of chunks in the block.
func newpoolfbit(
	size, n int64, pools *fbitPools,
	prev **poolfbit, next *poolfbit) *poolfbit {

	capacity := size * n
	pool := &poolfbit{
		capacity: capacity,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		fbits:    newfreebits(cacheline, n),
	}
	pool.prev, pool.next = prev, next
	return pool
}

func (pool *poolfbit) slabsize() int64 {
	return pool.size
}

func (pool *poolfbit) less(other interface{}) bool {
	oth := other.(*poolfbit)
	return uintptr(pool.base) < uintptr(oth.base)
}

func (pool *poolfbit) allocchunk() (unsafe.Pointer, bool) {
	if pool.base == nil {
		panic(fmt.Errorf("pool already released"))
	} else if pool.mallocated == pool.capacity {
		return nil, false
	}
	nthblock, _ := pool.fbits.alloc()
	if nthblock < 0 {
		return nil, false
	}
	ptr := uintptr(pool.base) + uintptr(nthblock*pool.size)
	//initblock(ptr, pool.size)
	pool.mallocated += pool.size
	mask := uintptr(Alignment - 1)
	if (ptr & mask) != 0 {
		fmsg := "allocated pointer is not %v byte aligned"
		panic(fmt.Errorf(fmsg, Alignment))
	}
	return unsafe.Pointer(ptr), true
}

func (pool *poolfbit) free(ptr unsafe.Pointer) {
	if ptr == nil {
		panic("poolfbit.free(): nil pointer")
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		panic("poolfbit.free(): unaligned pointer")
	}
	pool.fbits.free(int64(diffptr / uint64(pool.size)))
	pool.mallocated -= pool.size
	// unlink and re-link.
	pool.pools.unlink(pool).toheadfree(pool)
}

// Info implement api.MemoryPool{} interface.
func (pool *poolfbit) info() (capacity, heap, alloc, overhead int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(pool.fbits.sizeof())
	return pool.capacity, pool.capacity, pool.mallocated, slicesz + self
}

// release implement api.MemoryPool{} interface.
func (pool *poolfbit) release() {
	C.free(pool.base)
	pool.fbits = nil
	pool.capacity, pool.base = 0, nil
	pool.mallocated = 0
}

//---- local functions

// can be costly operation.
func (pool *poolfbit) checkallocated() int64 {
	return pool.capacity - (pool.fbits.freeblocks() * pool.size)
}

type fbitPools struct {
	full   *poolfbit
	free   *poolfbit
	npools int64 // number of active pools
	cpools int64 // number of created pools, including the ones released to OS.
}

func newFbitPool() *fbitPools {
	return &fbitPools{}
}

// shift next free to head
func (pools *fbitPools) shiftupFree() *fbitPools {
	pools.free = pools.free.next
	if pools.free != nil {
		pools.free.prev = &pools.free
	}
	return pools
}

// move head of free to head of full
func (pools *fbitPools) movetofull() *fbitPools {
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
func (pools *fbitPools) unlink(pool *poolfbit) *fbitPools {
	if pool.prev != nil {
		(*(pool.prev)) = pool.next
	}
	if pool.next != nil {
		pool.next.prev = pool.prev
	}
	return pools
}

// insert pool to the head of the free list.
func (pools *fbitPools) toheadfree(pool *poolfbit) *fbitPools {
	next := pools.free
	pools.free, pool.next = pool, next
	pool.prev = &pools.free
	if pool.next != nil {
		pool.next.prev = &pool.next
	}
	return pools
}

func (pools *fbitPools) allocchunk(arena *Arena, size int64) unsafe.Pointer {
	if pools.free == nil {
		numchunks := arena.adaptiveNumchunks(size, pools.cpools)
		pools.free = newpoolfbit(size, numchunks, pools, &pools.free, nil)
		pools.npools++
		pools.cpools++

	} else if pools.npools > 5 && pools.free.mallocated == 0 {
		if (pools.free.capacity / size) < 64 { // release pool to OS
			C.free(pools.free.base)
			pools.npools--
			return pools.shiftupFree().allocchunk(arena, size)
		}
	}
	ptr, ok := pools.free.allocchunk()
	if !ok { // full
		return pools.movetofull().allocchunk(arena, size)
	}
	return ptr, pools.free
}

func (pools *fbitPools) release() {
	for pool := pools.full; pool != nil; pool = pool.next {
		pool.release()
	}
	for pool := pools.free; pool != nil; pool = pool.next {
		pool.release()
	}
}

func (pools *fbitPools) info() (capacity, heap, alloc, overhead int64) {
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
