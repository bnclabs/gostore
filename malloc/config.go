package malloc

import "unsafe"

// Alignment blocks and chunks should be multiples of alignment.
const Alignment = int64(16)

// MEMUtilization is the ratio between allocated memory to application
// and useful memory allocated from OS.
const MEMUtilization = float64(0.95)

// Maxarenasize maximum size of a memory arena. Can be used as default
// capacity for NewArena()
const Maxarenasize = int64(1024 * 1024 * 1024 * 1024)

// Maxpools maximum number of pools allowed in an arena.
const Maxpools = int64(512)

// Maxchunks maximum number of chunks allowed in a pool.
const Maxchunks = int64(1024)

type memoryPools interface {
	// Allocate a new chunk from pool, no more memory available get a
	// new pool from OS for numchunks for slab size.
	allocchunk(arena *Arena, numchunks int64) unsafe.Pointer

	// Release all memory pools for this slab.
	release()

	// Info return memory accouting for this slab.
	info() (capacity, heap, alloc, overhead int64)
}
