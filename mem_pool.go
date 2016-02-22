// functions and objects method are not re-entrant.

package storage

//#include <stdlib.h>
import "C"

import "unsafe"
import "reflect"
import "sync/atomic"
import "encoding/binary"

var poolblkinit = make([]byte, 1024)

func init() {
	for i := 0; i < len(poolblkinit); i++ {
		poolblkinit[i] = 0xff
	}
}

// mempool manages a memory block sliced up into equal sized chunks.
type mempool struct {
	capacity   int64          // memory managed by this pool
	size       int64          // fixed size blocks in this pool
	base       unsafe.Pointer // pool's base pointer
	freelist   []uint8        // free block book-keeping
	freeoff    int64
	mallocated int64
}

// size of each chunk in the block and no. of chunks in the block.
func newmempool(size, n int64) *mempool {
	if (n & 0x7) != 0 {
		panic("number of blocks in a pool should be multiple of 8")
	}
	capacity := size * n
	freelist := make([]uint8, n/8)
	for i := range freelist {
		freelist[i] = 0xff // every block is free to begin with.
	}
	pool := &mempool{
		capacity: capacity,
		size:     size,
		base:     C.malloc(C.size_t(capacity)),
		freelist: freelist,
		freeoff:  0,
	}
	return pool
}

// allocate and amortize
func (pool *mempool) alloc() (unsafe.Pointer, bool) {
	var safeoff int64

	if pool.freeoff < 0 {
		return nil, false
	}
	byt := pool.freelist[pool.freeoff]
	if byt == 0 {
		panic("mempool.alloc(): invalid free-offset")
	}
	sz, k := pool.size, int64(findfirstset8(byt))
	ptr := uintptr(pool.base) + uintptr(((pool.freeoff*8)*sz)+(k*sz))
	pool.freelist[pool.freeoff] = clearbit8(byt, uint8(k))
	// recompute freeoff - cost of memmgnt is amortized here
	safeoff, pool.freeoff = pool.freeoff, -1
	for i := safeoff; i < int64(len(pool.freelist)); i++ {
		if pool.freelist[i] != 0 {
			pool.freeoff = i
			break
		}
	}
	atomic.AddInt64(&pool.mallocated, pool.size)
	pool.initblock(ptr)
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
	nthblock := diffptr / uint64(pool.size)
	nthoff, k := int64(nthblock/8), uint8(nthblock%8)
	pool.freelist[nthoff] = setbit8(pool.freelist[nthoff], k)
	if pool.freeoff == -1 || nthoff < pool.freeoff {
		pool.freeoff = nthoff
	}
	atomic.AddInt64(&pool.mallocated, -pool.size)
}

func (pool *mempool) release() {
	C.free(pool.base)
	pool.freelist, pool.freeoff = nil, -1
	pool.capacity, pool.base = 0, nil
	atomic.StoreInt64(&pool.mallocated, 0)
}

// compare whether pool's base ptr is less than other pool's base ptr.
func (pool *mempool) less(other *mempool) bool {
	return uintptr(pool.base) < uintptr(other.base)
}

//---- local functions

func (pool *mempool) memory() (overhead, useful int64) {
	self := int64(unsafe.Sizeof(*pool))
	slicesz := int64(cap(pool.freelist))
	return slicesz + self, pool.capacity
}

func (pool *mempool) allocated() int64 {
	return atomic.LoadInt64(&pool.mallocated)
}

func (pool *mempool) available() int64 {
	return pool.capacity - pool.allocated()
}

// can be costly operation.
func (pool *mempool) checkallocated() int64 {
	blocks := int64(0)
	q, r := int64(len(pool.freelist)/4), int64(len(pool.freelist)%4)
	for i := int64(1); i <= q; i++ {
		v := binary.BigEndian.Uint32(pool.freelist[(i-1)*4 : i*4])
		blocks += int64(zerosin32(uint32(v & 0xffffffff)))
	}
	for i := q * 4; i < (q*4)+r; i++ {
		blocks += int64(zerosin8(pool.freelist[i]))
	}
	return blocks * pool.size
}

func (pool *mempool) initblock(block uintptr) {
	var dst []byte
	initsz := len(poolblkinit)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Data, sl.Len = block, initsz
	for i := int64(0); i < pool.size/int64(initsz); i++ {
		copy(dst, poolblkinit)
		sl.Data = (uintptr)(uint64(sl.Data) + uint64(initsz))
	}
	if sl.Len = int(pool.size) % len(poolblkinit); sl.Len > 0 {
		copy(dst, poolblkinit)
	}
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
