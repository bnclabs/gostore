package malloc

import "unsafe"

import "github.com/prataprc/gostore/api"

// Mpooler to manage chunk pool
type Mpooler interface {
	// Chunksize managed by this pool.
	Chunksize() int64

	// Less ordering between pools
	Less(pool interface{}) bool

	// Allocate a chunk from pool
	Allocchunk() (ptr unsafe.Pointer, ok bool)

	// Free chunk back to pool.
	Free(ptr unsafe.Pointer)

	// Memory return memory allocated from OS an overhead of managing it.
	Memory() (overhead, useful int64)

	// Allocated return memory allocated from `useful` memory.
	Allocated() (allocated int64)

	// Available return memory allocated from `useful` memory.
	Available() (available int64)

	// Release this pool and all its resources.
	Release()

	// Chunksize alias for Mallocer
	Chunksizes() []int64

	// Alloc alias for Mallocer
	Alloc(size int64) (ptr unsafe.Pointer, m api.Mallocer)

	// Utilization map of chunk-size and its pool utilization
	Utilization() ([]int, []float64)
}
