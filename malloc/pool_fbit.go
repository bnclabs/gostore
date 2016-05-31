// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "unsafe"

//import "fmt"

// poolfbit manages a memory block sliced up into equal sized chunks.
type poolfbit struct {
	// 64-bit aligned stats
	mallocated int64

	capacity int64          // memory managed by this pool
	size     int64          // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	fbits    *freebits
}

func fbitfactory() func(size, n int64) Mpooler {
	return newpoolfbit
}

// size of each chunk in the block and no. of chunks in the block.
func newpoolfbit(size, n int64) Mpooler {
	if (n & 0x7) != 0 {
		panic("number of blocks in a pool should be multiple of 8")
	}
	capacity := size * n
	pool := &poolfbit{
		capacity: capacity,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		fbits:    newfreebits(cacheline /*TODO*/, n),
	}
	return pool
}

// Chunksize implement Mpooler{} interface.
func (pool *poolfbit) Chunksize() int64 {
	return pool.size
}

// Less implement Mpooler{} interface.
func (pool *poolfbit) Less(other Mpooler) bool {
	if oth, ok := other.(*poolfbit); ok {
		return uintptr(pool.base) < uintptr(oth.base)
	}
	panicerr("unexpected type pool %T", other)
	return false
}

// Alloc implement Mpooler{} interface.
func (pool *poolfbit) Alloc() (unsafe.Pointer, bool) {
	if pool.base == nil {
		panicerr("pool already released")
	} else if pool.mallocated == pool.capacity {
		return nil, false
	}
	nthblock, _ := pool.fbits.alloc()
	if nthblock < 0 {
		return nil, false
	}
	ptr := uintptr(pool.base) + uintptr(nthblock*pool.size)
	initblock(ptr, pool.size)
	pool.mallocated += pool.size
	if ptr&0x3 != 0 { // TODO: this check can be removed later.
		panic("allocated pointer is not 8 byte aligned")
	}
	return unsafe.Pointer(ptr), true
}

// Free implement Mpooler{} interface.
func (pool *poolfbit) Free(ptr unsafe.Pointer) {
	if ptr == nil {
		panic("poolfbit.free(): nil pointer")
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		panic("poolfbit.free(): unaligned pointer")
	}
	pool.fbits.free(int64(diffptr / uint64(pool.size)))
	pool.mallocated -= pool.size
}

// Memory implement Mpooler{} interface.
func (pool *poolfbit) Memory() (overhead, useful int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(pool.fbits.sizeof())
	return slicesz + self, pool.capacity
}

// Allocated implement Mpooler{} interface.
func (pool *poolfbit) Allocated() int64 {
	return pool.mallocated
}

// Available implement Mpooler{} interface.
func (pool *poolfbit) Available() int64 {
	return pool.capacity - pool.Allocated()
}

// Release implement Mpooler{} interface.
func (pool *poolfbit) Release() {
	C.free(pool.base)
	pool.fbits = nil
	pool.capacity, pool.base = 0, nil
	pool.mallocated = 0
}

func (pool *poolfbit) Utilization() {
	panicerr("call this method on arena object")
}

//---- local functions

// can be costly operation.
func (pool *poolfbit) checkallocated() int64 {
	return pool.capacity - (pool.fbits.freeblocks() * pool.size)
}

// mempools sortable based on base-pointer.
type mempools []*poolfbit

func (pools mempools) Len() int {
	return len(pools)
}

func (pools mempools) Less(i, j int) bool {
	return uintptr(pools[i].base) < uintptr(pools[j].base)
}

func (pools mempools) Swap(i, j int) {
	pools[i], pools[j] = pools[j], pools[i]
}