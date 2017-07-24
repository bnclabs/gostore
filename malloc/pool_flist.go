// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "unsafe"
import "fmt"

import "github.com/prataprc/gostore/api"

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

// Chunksize implement api.MemoryPool{} interface.
func (pool *poolflist) Slabsize() int64 {
	return pool.size
}

// Less import api.MemoryPool{} interface.
func (pool *poolflist) Less(other interface{}) bool {
	oth := other.(*poolflist)
	return uintptr(pool.base) < uintptr(oth.base)
}

// Allocchunk implement api.MemoryPool{} interface.
func (pool *poolflist) Allocchunk() (unsafe.Pointer, bool) {
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

// Free implement api.MemoryPool{} interface.
func (pool *poolflist) Free(ptr unsafe.Pointer) {
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
	// unlink and re-link.
	pool.pools.unlink(pool).toheadfree(pool)
}

// Info implement api.MemoryPool{} interface.
func (pool *poolflist) Info() (capacity, heap, alloc, overhead int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(unsafe.Sizeof(pool.freelist))
	return pool.capacity, pool.capacity, pool.mallocated, slicesz + self
}

// Release implement api.MemoryPool{} interface.
func (pool *poolflist) Release() {
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
	cpools int64 // number of created pools, including the ones released to OS.
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

// Allocchunk implement MemoryPools interface.
func (pools *flistPools) Allocchunk(
	mallocer api.Mallocer, size int64) (unsafe.Pointer, api.MemoryPool) {

	arena := mallocer.(*Arena)
	if pools.free == nil {
		numchunks := arena.adaptiveNumchunks(size, pools.cpools)
		pools.free = newpoolflist(size, numchunks, pools, &pools.free, nil)
		pools.npools++
		pools.cpools++

	} else if pools.npools > 5 && pools.free.mallocated == 0 {
		if (pools.free.capacity / size) < 64 { // release pool to OS
			C.free(pools.free.base)
			pools.npools--
			return pools.shiftupFree().Allocchunk(arena, size)
		}
	}
	ptr, ok := pools.free.Allocchunk()
	if !ok { // full
		return pools.movetofull().Allocchunk(arena, size)
	}
	return ptr, pools.free
}

// Release implement MemoryPools interface.
func (pools *flistPools) Release() {
	for pool := pools.full; pool != nil; pool = pool.next {
		pool.Release()
	}
	for pool := pools.free; pool != nil; pool = pool.next {
		pool.Release()
	}
}

// Info implement MemoryPools interface.
func (pools *flistPools) Info() (capacity, heap, alloc, overhead int64) {
	for pool := pools.full; pool != nil; pool = pool.next {
		c, h, a, o := pool.Info()
		capacity, heap, alloc, overhead = capacity+c, heap+h, alloc+a, overhead+o
	}
	for pool := pools.free; pool != nil; pool = pool.next {
		c, h, a, o := pool.Info()
		capacity, heap, alloc, overhead = capacity+c, heap+h, alloc+a, overhead+o
	}
	return
}
