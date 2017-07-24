package api

import "unsafe"

// Mallocer interface for custom memory management. Typical library
// implementing this interface will
type Mallocer interface {
	// Slabs allocatable slab of sizes.
	Slabs() (sizes []int64)

	// Alloc allocate a chunk of `n` bytes from `pool`. Allocated
	// memory is always 64-bit aligned.
	Alloc(n int64) (ptr unsafe.Pointer, pool MemoryPool)

	// Release arena, all its pools and resources.
	Release()

	// Info of memory accounting for this arena.
	Info() (capacity, heap, alloc, overhead int64)

	// Utilization map of slab-size and its utilization
	Utilization() ([]int, []float64)
}

type MemoryPool interface {
	// Allocate a chunk from pool
	Allocchunk() (ptr unsafe.Pointer, ok bool)

	// Free pointer back to the pool.
	Free(ptr unsafe.Pointer)

	// Release this pool and all its resources.
	Release()

	// Slabsize return size of memory chunks managed by this pool.
	Slabsize() int64

	// Info return memory accounting for this pool.
	Info() (capacity, heap, alloc, overhead int64)

	// Less ordering between pools
	Less(pool interface{}) bool
}

type MemoryPools interface {
	// Allocate a new chunk from pool, no more memory available get a
	// new pool from OS for numchunks for slab size.
	Allocchunk(arena Mallocer, numchunks int64) (unsafe.Pointer, MemoryPool)

	// Release all memory pools for this slab.
	Release()

	// Info return memory accouting for this slab.
	Info() (capacity, heap, alloc, overhead int64)
}
