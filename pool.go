package llrb

//#include <stdlib.h>
import "C"

import "unsafe"

// Pool manages a memory block sliced up into equal sized chunks.
type Pool struct {
	capacity int            // memory managed by this pool
	size     int            // fixed size blocks in this pool
	base     unsafe.Pointer // pool's base pointer
	freelist []byte         // free block book-keeping
	freeoff  int
}

// NewPool will create a new pool of contiguous `n` blocks, each
// `size` bytes in length.
func NewPool(size, n int) *Pool {
	if (n & 0x7) != 0 { // n must be a multiple of 8
		n = ((n >> 3) + 1) << 3
	}
	capacity := size * n
	base := C.malloc(C.size_t(capacity))
	freelist := make([]byte, n/8)
	for i := range freelist {
		freelist[i] = 0xff // every block is free to begin with.
	}
	pool := &Pool{
		capacity: capacity,
		size:     size,
		base:     base,
		freelist: freelist,
		freeoff:  0,
	}
	return pool
}

func (pool *Pool) Alloc() (unsafe.Pointer, bool) {
	var safeoff int

	if pool.freeoff < 0 {
		return nil, false
	}
	byt := pool.freelist[pool.freeoff]
	if byt == 0 {
		panic("Pool.Alloc(): invalid free-offset")
	}
	sz, k := pool.size, findfirstset8(byt)
	ptr := uintptr(pool.base) + uintptr(((pool.freeoff*8)*sz)+(int(k)*sz))
	pool.freelist[pool.freeoff] = clearbit8(byt, k)
	safeoff, pool.freeoff = pool.freeoff, -1
	for i := safeoff; i < len(pool.freelist); i++ {
		if pool.freelist[i] > 0 {
			pool.freeoff = i
			break
		}
	}
	return unsafe.Pointer(ptr), true
}

func (pool *Pool) Free(ptr unsafe.Pointer) {
	nthblock := uint64(uintptr(ptr)-uintptr(pool.base)) / uint64(pool.size)
	nthoff := (nthblock / 8)
	pool.freelist[nthoff] = setbit8(pool.freelist[nthoff], uint8(nthblock%8))
	if int(nthoff) < pool.freeoff {
		pool.freeoff = int(nthoff)
	}
}

// Close to release memory to OS.
func (pool *Pool) Close() {
	C.free(pool.base)
}

// Less compare whether pool's base ptr is less than other pool's base ptr.
func (pool *Pool) Less(other *Pool) bool {
	return uintptr(pool.base) < uintptr(other.base)
}

// Pools sortable based on base-pointer.
type Pools []*Pool

func (pools Pools) Len() int {
	return len(pools)
}

func (pools Pools) Less(i, j int) bool {
	return pools[i].Less(pools[j])
}

func (pools Pools) Swap(i, j int) {
	pools[i], pools[j] = pools[j], pools[i]
}
