package malloc

import "unsafe"

// Mpooler to manage chunk pool
type Mpooler interface {
	// Chunksize managed by this pool..
	Chunksize() int64

	// Less ordering between pools
	Less(pool Mpooler) bool

	// Allocate a chunk from pool
	Alloc() (ptr unsafe.Pointer, ok bool)

	// Memory return memory allocated from OS an overhead of managing it.
	Memory() (overhead, useful int64)

	// Allocated return memory allocated from `useful` memory.
	Allocated() (allocated int64)

	// Available return memory allocated from `useful` memory.
	Available() (available int64)

	// Release this pool and all its resources.
	Release()
}
