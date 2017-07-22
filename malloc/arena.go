package malloc

import "fmt"
import "sort"
import "unsafe"
import "errors"

import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"

var _ api.Mallocer = &Arena{}

// ErrorOutofMemory when arena's capacity is exhausted and it cannot
// manage new allocations.
var ErrorOutofMemory = errors.New("malloc.outofmemory")

// Arena of memory.
type Arena struct {
	slabs     []int64                   // sorted list of block-sizes in this arena
	mpools    map[int64]api.MemoryPools // size -> list of api.MemoryPool
	poolmaker func(size, numchunks int64) api.MemoryPool

	// settings
	capacity   int64  // memory capacity to be managed by this arena
	minblock   int64  // minimum block size allocatable by arena
	maxblock   int64  // maximum block size allocatable by arena
	allocator  string // allocator algorithm
	fairchunks int64  // fair number of chunks per pool.
}

// NewArena create a new memory arena.
func NewArena(capacity int64, setts s.Settings) *Arena {
	arena := (&Arena{capacity: capacity}).readsettings(setts)
	arena.slabs = Computeslabs(arena.minblock, arena.maxblock)
	arena.mpools = make(map[int64]api.MemoryPools)
	arena.fairchunks = ChunksPerPool(arena.slabs, capacity)

	if int64(len(arena.slabs)) > Maxpools {
		panic(fmt.Errorf("number of pools in arena exeeds %v", Maxpools))
	} else if cp := arena.capacity; cp > Maxarenasize {
		fmsg := "capacity cannot exceed %v bytes (%v)"
		panic(fmt.Errorf(fmsg, cp, Maxarenasize))
	}
	switch arena.allocator {
	case "flist":
		arena.poolmaker = flistfactory()
	case "fbit":
		arena.poolmaker = fbitfactory()
	}
	for _, size := range arena.slabs {
		arena.mpools[size] = make(api.MemoryPools, 0, Maxpools/2)
	}
	return arena
}

func (arena *Arena) readsettings(setts s.Settings) *Arena {
	arena.minblock = setts.Int64("minblock")
	arena.maxblock = setts.Int64("maxblock")
	arena.allocator = setts.String("allocator")
	return arena
}

//---- operations

// Alloc implement api.Mallocer{} interface.
func (arena *Arena) Alloc(n int64) (unsafe.Pointer, api.MemoryPool) {
	// check argument
	if largest := arena.slabs[len(arena.slabs)-1]; n > largest {
		panic(fmt.Errorf("Alloc size %v exceeds maxblock size %v", n, largest))
	}
	// try to get from existing pool
	size := SuitableSlab(arena.slabs, n)
	for _, mpool := range arena.mpools[size] {
		if ptr, ok := mpool.Allocchunk(); ok {
			return ptr, mpool
		}
	}
	// check whether we are exceeding memory.
	allocated := int64(arena.fairchunks * size)
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
	mpool := arena.poolmaker(size, arena.fairchunks)
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
	arena.slabs, arena.mpools = nil, nil
}

//---- statistics and maintenance

// Slabs implement Mallocer{} interface.
func (arena *Arena) Slabs() []int64 {
	return arena.slabs
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
	slicesz := int64(cap(arena.slabs) * int(unsafe.Sizeof(int64(1))))
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
	for _, size := range arena.slabs {
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
