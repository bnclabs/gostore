package api

import "unsafe"

// Mallocer interface for custom memory management. Typical library
// implementing this interface will
type Mallocer interface {
	// Allocate a chunk of `n` bytes from `pool`.
	Alloc(n int64) (ptr unsafe.Pointer, pool Mallocer)

	// Free pointer back to the pool.
	Free(ptr unsafe.Pointer)

	// Memory allocated from OS and overhead of managing it.
	Memory() (overhead, useful int64)

	// Allocated from `useful` memory.
	Allocated() (allocated int64)

	// Available memory from arena.
	Available() (available int64)

	// Chunksizes allocatable chunk-sizes.
	Chunksizes() (sizes []int64)

	// Utilization map of chunk-size and its utilization
	Utilization() ([]int, []float64)

	// Release arena, all its pools and resources.
	Release()

	// Chunksize alias for alloc:Mpooler{} interface.
	Chunksize() int64

	// Less alias for alloc:Mpooler interface.
	Less(pool interface{}) bool

	// Allocate alias for alloc:Mpooler interface.
	Allocchunk() (ptr unsafe.Pointer, ok bool)
}
