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
}

func fbitfactory() func(size, n int64) api.MemoryPool {
	return newpoolfbit
}

// size of each chunk in the block and no. of chunks in the block.
func newpoolfbit(size, n int64) api.MemoryPool {
	capacity := size * n
	pool := &poolfbit{
		capacity: capacity,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		fbits:    newfreebits(cacheline, n),
	}
	return pool
}

// Chunksize implement api.MemoryPool{} interface.
func (pool *poolfbit) Slabsize() int64 {
	return pool.size
}

// Less implement api.MemoryPool{} interface.
func (pool *poolfbit) Less(other interface{}) bool {
	oth := other.(*poolfbit)
	return uintptr(pool.base) < uintptr(oth.base)
}

// Allocchunk implement api.MemoryPool{} interface.
func (pool *poolfbit) Allocchunk() (unsafe.Pointer, bool) {
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

// Info implement api.MemoryPool{} interface.
func (pool *poolfbit) Info() (capacity, heap, alloc, overhead int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(pool.fbits.sizeof())
	return pool.capacity, pool.capacity, pool.mallocated, slicesz + self
}

// Release implement api.MemoryPool{} interface.
func (pool *poolfbit) Release() {
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
