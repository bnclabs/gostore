// Functions and methods are not thread safe.

package malloc

//#include <stdlib.h>
import "C"

import "unsafe"

// poolflist manages a memory block sliced up into equal sized chunks.
type poolflist struct {
	// 64-bit aligned stats
	mallocated int64

	capacity int64          // memory managed by this pool
	size     int64          // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	freelist []uint16
	freeoff  int
}

func flistfactory() func(size, n int64) Mpooler {
	return newpoolflist
}

// size of each chunk in the block and no. of chunks in the block.
func newpoolflist(size, n int64) Mpooler {
	if (n & 0x7) != 0 {
		panic("number of chunks in a pool should be multiple of 8")
	}
	capacity := size * n
	pool := &poolflist{
		capacity: capacity,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		freelist: make([]uint16, n),
		freeoff:  int(n - 1),
	}
	for i := 0; i < int(n); i++ {
		pool.freelist[i] = uint16(i)
	}
	return pool
}

// Chunksize implement Mpooler{} interface.
func (pool *poolflist) Chunksize() int64 {
	return pool.size
}

// Less import Mpooler{} interface.
func (pool *poolflist) Less(other Mpooler) bool {
	if oth, ok := other.(*poolflist); ok {
		return uintptr(pool.base) < uintptr(oth.base)
	}
	panicerr("unexpected type pool %T", other)
	return false
}

// Alloc implement Mpooler{} interface.
func (pool *poolflist) Alloc() (unsafe.Pointer, bool) {
	if pool.mallocated == pool.capacity {
		return nil, false
	}
	nthblock := int64(pool.freelist[pool.freeoff])
	pool.freelist = pool.freelist[:pool.freeoff]
	pool.freeoff--
	ptr := uintptr(pool.base) + uintptr(nthblock*pool.size)
	initblock(ptr, pool.size)
	pool.mallocated += pool.size
	if ptr&0x3 != 0 { // TODO: this check can be removed later.
		panic("allocated pointer is not 8 byte aligned")
	}
	return unsafe.Pointer(ptr), true
}

// Free implement Mpooler{} interface.
func (pool *poolflist) Free(ptr unsafe.Pointer) {
	if ptr == nil {
		panic("poolflist.free(): nil pointer")
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		panic("poolflist.free(): unaligned pointer")
	}
	nthblock := uint16(diffptr / uint64(pool.size))
	pool.freelist = append(pool.freelist, nthblock)
	pool.freeoff++
	pool.mallocated -= pool.size
}

// Memory implement Mpooler{} interface.
func (pool *poolflist) Memory() (overhead, useful int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(unsafe.Sizeof(pool.freelist))
	return slicesz + self, pool.capacity
}

// Allocated implement Mpooler{} interface.
func (pool *poolflist) Allocated() int64 {
	return pool.mallocated
}

// Available implement Mpooler{} interface.
func (pool *poolflist) Available() int64 {
	return pool.capacity - pool.Allocated()
}

// Release implement Mpooler{} interface.
func (pool *poolflist) Release() {
	C.free(pool.base)
	pool.freelist, pool.freeoff = nil, -1
	pool.capacity, pool.base = 0, nil
	pool.mallocated = 0
}

func (pool *poolflist) Utilization() {
	panicerr("call this method on arena object")
}

//---- local functions

func (pool *poolflist) checkallocated() int64 {
	return pool.capacity - int64(len(pool.freelist))*pool.size
}
