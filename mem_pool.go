// +build !fbit

// functions and objects method are not re-entrant.
package storage

//#include <stdlib.h>
import "C"

import "unsafe"

import "fmt"

var poolblkinit = make([]byte, 1024)
var maxpoolblocks = int64(65536)

func init() {
	for i := 0; i < len(poolblkinit); i++ {
		poolblkinit[i] = 0xff
	}
}

// mempool manages a memory block sliced up into equal sized chunks.
type mempool struct {
	// 64-bit aligned stats
	mallocated int64

	capacity int64          // memory managed by this pool
	size     int64          // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	freelist []uint16
	freeoff  int
}

// size of each chunk in the block and no. of chunks in the block.
func newmempool(size, n int64) *mempool {
	if (n & 0x7) != 0 {
		panic("number of blocks in a pool should be multiple of 8")
	} else if n > maxpoolblocks {
		panic(fmt.Errorf("cannot have more than %v blocks", maxpoolblocks))
	}
	capacity := size * n
	pool := &mempool{
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

// O(1)
func (pool *mempool) alloc() (unsafe.Pointer, bool) {
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

// O(1)
func (pool *mempool) free(ptr unsafe.Pointer) {
	if ptr == nil {
		panic("mempool.free(): nil pointer")
	}
	diffptr := uint64(uintptr(ptr) - uintptr(pool.base))
	if (diffptr % uint64(pool.size)) != 0 {
		panic("mempool.free(): unaligned pointer")
	}
	nthblock := uint16(diffptr / uint64(pool.size))
	pool.freelist = append(pool.freelist, nthblock)
	pool.freeoff++
	pool.mallocated -= pool.size
}

func (pool *mempool) release() {
	C.free(pool.base)
	pool.freelist, pool.freeoff = nil, -1
	pool.capacity, pool.base = 0, nil
	pool.mallocated = 0
}

// compare whether pool's base ptr is less than other pool's base ptr.
func (pool *mempool) less(other *mempool) bool {
	return uintptr(pool.base) < uintptr(other.base)
}

//---- local functions

func (pool *mempool) memory() (overhead, useful int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(unsafe.Sizeof(pool.freelist))
	return slicesz + self, pool.capacity
}

func (pool *mempool) allocated() int64 {
	return pool.mallocated
}

func (pool *mempool) available() int64 {
	return pool.capacity - pool.allocated()
}

func (pool *mempool) checkallocated() int64 {
	return pool.capacity - int64(len(pool.freelist))*pool.size
}

// mempools sortable based on base-pointer.
type mempools []*mempool

func (pools mempools) Len() int {
	return len(pools)
}

func (pools mempools) Less(i, j int) bool {
	return pools[i].less(pools[j])
}

func (pools mempools) Swap(i, j int) {
	pools[i], pools[j] = pools[j], pools[i]
}
