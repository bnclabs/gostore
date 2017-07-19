package malloc

import "sort"
import "unsafe"
import "errors"

import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"

// TODO: is this error really required ?
var ErrorExceedCapacity = errors.New("malloc.exceedCapacity")

// ErrorOutofMemory when arena's capacity is exhausted and it cannot
// manage new allocations.
var ErrorOutofMemory = errors.New("malloc.outofmemory")

// Arena of memory.
type Arena struct {
	blocksizes []int64                   // sorted list of block-sizes in this arena
	mpools     map[int64]api.MemoryPools // size -> list of api.MemoryPool
	poolmaker  func(size, numblocks int64) api.MemoryPool

	// settings
	capacity  int64  // memory capacity to be managed by this arena
	minblock  int64  // minimum block size allocatable by arena
	maxblock  int64  // maximum block size allocatable by arena
	pcapacity int64  // maximum capacity for a single pool
	maxpools  int64  // maximum number of pools
	maxchunks int64  // maximum number of chunks allowed in a pool
	allocator string // allocator algorithm
}

// NewArena create a new memory arena.
func NewArena(setts s.Settings) *Arena {
	arena := (&Arena{}).readsettings(setts)
	arena.blocksizes = Blocksizes(arena.minblock, arena.maxblock)
	arena.mpools = make(map[int64]api.MemoryPools)

	if int64(len(arena.blocksizes)) > arena.maxpools {
		panicerr("number of pools in arena exeeds %v", arena.maxpools)
	} else if cp := arena.capacity; cp > Maxarenasize {
		panicerr("arena cannot exceed %v bytes (%v)", cp, Maxarenasize)
	}
	switch arena.allocator {
	case "flist":
		arena.poolmaker = flistfactory()
	case "fbit":
		arena.poolmaker = fbitfactory()
	}
	for _, size := range arena.blocksizes {
		arena.mpools[size] = make(api.MemoryPools, 0, arena.maxpools/2)
	}
	return arena
}

func (arena *Arena) readsettings(setts s.Settings) *Arena {
	arena.capacity = setts.Int64("capacity")
	arena.minblock = setts.Int64("minblock")
	arena.maxblock = setts.Int64("maxblock")
	arena.pcapacity = setts.Int64("pool.capacity")
	arena.maxpools = setts.Int64("maxpools")
	arena.maxchunks = setts.Int64("maxchunks")
	arena.allocator = setts.String("allocator")
	return arena
}

//---- operations

// Alloc implement api.Mallocer{} interface.
func (arena *Arena) Alloc(n int64) (unsafe.Pointer, api.MemoryPool) {
	// check argument
	if largest := arena.blocksizes[len(arena.blocksizes)-1]; n > largest {
		panicerr("Alloc size %v exceeds maxblock size %v", n, largest)
	}
	// try to get from existing pool
	size := SuitableSize(arena.blocksizes, n)
	for _, mpool := range arena.mpools[size] {
		if ptr, ok := mpool.Allocchunk(); ok {
			return ptr, mpool
		}
	}
	// pool exhausted, figure the dimensions and create a new pool.
	if arena.pcapacity <= size {
		panic(ErrorExceedCapacity)
	}
	numblocks := (arena.capacity / int64(len(arena.blocksizes))) / size
	if int64(numblocks*size) > arena.pcapacity {
		numblocks = arena.pcapacity / size
	}
	if numblocks > arena.maxchunks {
		numblocks = arena.maxchunks
	}
	if numblocks < Alignment {
		numblocks = Alignment
	}
	if mod := numblocks % Alignment; mod != 0 {
		numblocks += Alignment - mod
	}
	// check whether we are exceeding memory.
	allocated := int64(numblocks * size)
	for _, mpools := range arena.mpools {
		if len(mpools) == 0 {
			continue
		}
		allocated += mpools[0].Chunksize() * int64(len(mpools))
	}
	if allocated > arena.capacity {
		panic(ErrorOutofMemory)
	}
	// go ahead, create a new pool.
	mpool := arena.poolmaker(size, numblocks)
	ln := len(arena.mpools[size])
	arena.mpools[size] = append(arena.mpools[size], nil)
	copy(arena.mpools[size][1:], arena.mpools[size][:ln])
	arena.mpools[size][0] = mpool
	ptr, _ := mpool.Allocchunk()
	return ptr, mpool
}

// Release implement Mallocer{} interface.
func (arena *Arena) Release() {
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			mpool.Release()
		}
	}
	arena.blocksizes, arena.mpools = nil, nil
}

// Free does not implement Mallocer{} interface. Use api.MemoryPool
// to free allocated memory chunk.
func (arena *Arena) Free(ptr unsafe.Pointer) {
	panicerr("Free cannot be called on arena, use api.MemoryPool")
}

//---- statistics and maintenance

// Chunksizes implement Mallocer{} interface.
func (arena *Arena) Chunksizes() []int64 {
	return arena.blocksizes
}

// Allocated implement Mallocer{} interface.
func (arena *Arena) Allocated() int64 {
	allocated := int64(0)
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			allocated += mpool.Allocated()
		}
	}
	return allocated
}

// Available implement Mallocer{} interface.
func (arena *Arena) Available() int64 {
	return arena.capacity - arena.Allocated()
}

// Memory implement Mallocer{} interface.
func (arena *Arena) Memory() (overhead, useful int64) {
	self := int64(unsafe.Sizeof(*arena))
	slicesz := int64(cap(arena.blocksizes) * int(unsafe.Sizeof(int64(1))))
	overhead += self + slicesz
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			x, y := mpool.Memory()
			overhead += x
			useful += y
		}
	}
	return
}

// Utilization implement Mallocer{} interface.
func (arena *Arena) Utilization() ([]int, []float64) {
	var sizes []int
	for _, size := range arena.blocksizes {
		sizes = append(sizes, int(size))
	}
	sort.Ints(sizes)

	ss, zs := make([]int, 0), make([]float64, 0)
	for _, size := range sizes {
		capacity, allocated := float64(0), float64(0)
		for _, mpool := range arena.mpools[int64(size)] {
			_, useful := mpool.Memory()
			capacity += float64(useful)
			allocated += float64(mpool.Allocated())
		}
		if capacity > 0 {
			ss = append(ss, size)
			zs = append(zs, (allocated/capacity)*100)
		}
	}
	return ss, zs
}
