package api

import "unsafe"

// Mallocer interface for custom memory management.
type Mallocer interface {
	// Slabs allocatable slab of sizes.
	Slabs() (sizes []int64)

	// Alloc allocate a chunk of `n` bytes from `pool`. Allocated
	// memory is always 64-bit aligned.
	Alloc(n int64) unsafe.Pointer

	// Allocslab allocate a chunk from slab. Use this only if slab size
	// is known to exist with mallocer.
	Allocslab(slab int64) unsafe.Pointer

	// Slabsize return the size of the chunk's slab size.
	Slabsize(ptr unsafe.Pointer) int64

	// Chunklen return the length of the chunk usable by application.
	Chunklen(ptr unsafe.Pointer) int64

	// Free chunk from arena/pool.
	Free(ptr unsafe.Pointer)

	// Release arena, all its pools and resources.
	Release()

	// Info of memory accounting for this arena.
	Info() (capacity, heap, alloc, overhead int64)

	// Utilization map of slab-size and its utilization
	Utilization() ([]int, []float64)
}
