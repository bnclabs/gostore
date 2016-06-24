// Package malloc supplies custom memory management for in-memory data
// structures. Note that Types and Functions exported by this package are not
// necessarily thread safe.
//
// Arena is a single block of memory that will be utilized by the
// algorithm/data-structure during its existence. To improve memory
// utilization arenas are divided into pools of fixed sized blocks, where each
// pool will supply memory blocks of same size. Arenas can be created with
// following parameters:
//
//   capacity  : size of arena in bytes.
//   minblock  : blocks less than minblock sizes cannot be allocated.
//   maxblock  : blocks greater than maxblock sizes cannot be allocated.
//   pcapacity : pool's capacity, in this arena, cannot exceed this limit.
//   maxpools  : maximum number of pool-sizes allowed.
//   maxchunks : maximum number of block-chunks allowed in a pool.
//   allocator : allocator algorithm to use supports `flist` or `fbit`.
package malloc

import "unsafe"
import "sort"

import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/api"

// MEMUtilization expected in an arenas.
const MEMUtilization = float64(0.95)

// Sizeinterval minblock and maxblocks should be multiples of Sizeinterval.
const Sizeinterval = int64(32)

// Maxarenasize maximum size of a memory arena. Can be used as default for
// config-parameter `capacity`.
const Maxarenasize = int64(1024 * 1024 * 1024 * 1024) // 1TB

// Maxpools maximum number of pools allowed in an arena. Can be used as
// default for config-parameter `maxpools`.
const Maxpools = int64(256)

// Maxchunks maximum number of chunks allowed in a pool. Can be used as
// default for config-parameter `maxchunks`.
const Maxchunks = int64(65536)

// Arena defines a large memory block that can be divided into memory pools.
type Arena struct {
	blocksizes []int64            // sorted list of block-sizes in this arena
	mpools     map[int64]Mpoolers // size -> list of Mpooler
	poolmaker  func(size, numblocks int64) Mpooler

	// configuration
	capacity  int64  // memory capacity to be managed by this arena
	minblock  int64  // minimum block size allocatable by arena
	maxblock  int64  // maximum block size allocatable by arena
	pcapacity int64  // maximum capacity for a single pool
	maxpools  int64  // maximum number of pools
	maxchunks int64  // maximum number of chunks allowed in a pool
	allocator string // allocator algorithm
}

// NewArena create a new memory arena.
func NewArena(config lib.Config) *Arena {
	minblock, maxblock := config.Int64("minblock"), config.Int64("maxblock")
	arena := &Arena{
		blocksizes: Blocksizes(minblock, maxblock),
		mpools:     make(map[int64]Mpoolers),
		// configuration
		minblock:  minblock,
		maxblock:  maxblock,
		capacity:  config.Int64("capacity"),
		pcapacity: config.Int64("pool.capacity"),
		maxpools:  config.Int64("maxpools"),
		maxchunks: config.Int64("maxchunks"),
		allocator: config.String("allocator"),
	}
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
		arena.mpools[size] = make(Mpoolers, 0, arena.maxpools/2)
	}
	return arena
}

//---- operations

// Alloc implement api.Mallocer{} interface.
func (arena *Arena) Alloc(n int64) (unsafe.Pointer, api.Mallocer) {
	if arena.mpools == nil {
		panicerr("arena released")
	}

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
	numblocks := (arena.capacity / int64(len(arena.blocksizes))) / size
	if int64(numblocks*size) > arena.pcapacity {
		numblocks = arena.pcapacity / size
	}
	if numblocks > arena.maxchunks {
		numblocks = arena.maxchunks
	}
	if (numblocks & 0x7) > 0 {
		numblocks = (numblocks >> 3) << 3
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

// Free object.
func (arena *Arena) Free(ptr unsafe.Pointer) {
	panicerr("Free cannot be called on arena, use Mpooler")
}

//---- statistics and maintenance

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

// Chunksizes implement Mallocer{} interface.
func (arena *Arena) Chunksizes() []int64 {
	return arena.blocksizes
}

// Utilization implement Mallocer{} interface.
func (arena *Arena) Utilization() ([]int, []float64) {
	var sizes []int
	for _, size := range arena.blocksizes {
		sizes = append(sizes, int(size))
	}
	sort.Ints(sizes)

	ss, zs := make([]int, 0), make([]float64, 0)
	capacity, allocated := float64(0), float64(0)
	for _, size := range sizes {
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

// Chunksize alias for Mpooler{} interface.
func (arena *Arena) Chunksize() int64 {
	panicerr("Chunksize() cannot be applied on arena")
	return 0
}

// Less alias for Mpooler{} interface.
func (arena *Arena) Less(pool interface{}) bool {
	panicerr("Less() cannot be applied on arena")
	return false
}

// Allocchunk alias for Mpooler{} interface.
func (arena *Arena) Allocchunk() (ptr unsafe.Pointer, ok bool) {
	panicerr("Allocchunk() cannot be applied on arena")
	return nil, false
}

// Mpoolers sortable based on base-pointer.
type Mpoolers []Mpooler

func (pools Mpoolers) Len() int {
	return len(pools)
}

func (pools Mpoolers) Less(i, j int) bool {
	return pools[i].Less(pools[j])
}

func (pools Mpoolers) Swap(i, j int) {
	pools[i], pools[j] = pools[j], pools[i]
}
