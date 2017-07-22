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

	// Info of counts.
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

	// Info of counts.
	Info() (capacity, heap, alloc, overhead int64)

	// Less ordering between pools
	Less(pool interface{}) bool
}

// MemoryPools sortable based on base-pointer.
type MemoryPools []MemoryPool

// Len implement sort.Sort interface.
func (pools MemoryPools) Len() int {
	return len(pools)
}

// Less implement sort.Sort interface.
func (pools MemoryPools) Less(i, j int) bool {
	return pools[i].Less(pools[j])
}

// Swap implement sort.Sort interface.
func (pools MemoryPools) Swap(i, j int) {
	pools[i], pools[j] = pools[j], pools[i]
}
