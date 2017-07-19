package api

import "unsafe"

// Mallocer interface for custom memory management. Typical library
// implementing this interface will
type Mallocer interface {
	// Alloc allocate a chunk of `n` bytes from `pool`.
	Alloc(n int64) (ptr unsafe.Pointer, pool MemoryPool)

	// Release arena, all its pools and resources.
	Release()

	// Free pointer back to the pool.
	Free(ptr unsafe.Pointer)

	// Chunksizes allocatable chunk-sizes.
	Chunksizes() (sizes []int64)

	// Allocated from `useful` memory.
	Allocated() (allocated int64)

	// Available memory from arena.
	Available() (available int64)

	// Memory allocated from OS and overhead of managing it.
	Memory() (overhead, useful int64)

	// Utilization map of chunk-size and its utilization
	Utilization() ([]int, []float64)
}

type MemoryPool interface {
	// Allocate a chunk from pool
	Allocchunk() (ptr unsafe.Pointer, ok bool)

	// Free pointer back to the pool.
	Free(ptr unsafe.Pointer)

	// Release this pool and all its resources.
	Release()

	// Chunksize return size of memory chunks managed by this pool.
	Chunksize() int64

	// Allocated return memory allocated from `useful` memory.
	Allocated() (allocated int64)

	// Available return memory allocated from `useful` memory.
	Available() (available int64)

	// Memory return memory allocated from OS an overhead of managing it.
	Memory() (overhead, useful int64)

	// Less ordering between pools
	Less(pool interface{}) bool
}

// MemoryPools sortable based on base-pointer.
type MemoryPools []MemoryPool

func (pools MemoryPools) Len() int {
	return len(pools)
}

func (pools MemoryPools) Less(i, j int) bool {
	return pools[i].Less(pools[j])
}

func (pools MemoryPools) Swap(i, j int) {
	pools[i], pools[j] = pools[j], pools[i]
}
