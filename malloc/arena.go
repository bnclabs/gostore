package malloc

import "fmt"
import "sort"
import "unsafe"
import "errors"

import "github.com/prataprc/gostore/api"
import s "github.com/prataprc/gosettings"

//#include <stdlib.h>
import "C"

var _ api.Mallocer = &Arena{}

// ErrorOutofMemory when arena's capacity is exhausted and it cannot
// manage new allocations.
var ErrorOutofMemory = errors.New("malloc.outofmemory")

// Arena of memory.
type Arena struct {
	slabs     []int64                   // sorted list of slabs in this arena
	maxchunks [6]int64                  // 0-512,1-1K,16K,128K,1M,16M
	mpools    map[int64]api.MemoryPools // size -> list of api.MemoryPool

	// settings
	capacity  int64  // memory capacity to be managed by this arena
	minblock  int64  // minimum block size allocatable by arena
	maxblock  int64  // maximum block size allocatable by arena
	allocator string // allocator algorithm
}

// NewArena create a new memory arena.
func NewArena(capacity int64, setts s.Settings) *Arena {
	arena := (&Arena{capacity: capacity}).readsettings(setts)
	arena.slabs = Computeslabs(arena.minblock, arena.maxblock)
	arena.mpools = make(map[int64]api.MemoryPools)
	// validate inputs
	if int64(len(arena.slabs)) > Maxpools {
		panic(fmt.Errorf("number of pools in arena exeeds %v", Maxpools))
	} else if cp := arena.capacity; cp > Maxarenasize {
		fmsg := "capacity cannot exceed %v bytes (%v)"
		panic(fmt.Errorf(fmsg, cp, Maxarenasize))
	}
	// memory-pools
	for _, slab := range arena.slabs {
		switch arena.allocator {
		case "flist":
			arena.mpools[slab] = newFlistPool()
		case "fbit":
			arena.mpools[slab] = newFbitPool()
		}
	}
	// lookup table for adaptive numchunks
	arena.maxchunks = [6]int64{
		arena.maxchunksSize(capacity, 512),
		arena.maxchunksSize(capacity, 1024),
		arena.maxchunksSize(capacity, 16*1024),
		arena.maxchunksSize(capacity, 128*1024),
		arena.maxchunksSize(capacity, 1024*1024),
		arena.maxchunksSize(capacity, 16*1024*1024),
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
	return arena.mpools[size].Allocchunk(arena, size)
}

// Release implement Mallocer{} interface.
func (arena *Arena) Release() {
	for _, mpools := range arena.mpools {
		mpools.Release()
	}
	arena.slabs, arena.mpools = nil, nil
}

//---- statistics and maintenance

// Slabs implement Mallocer{} interface.
func (arena *Arena) Slabs() []int64 {
	return arena.slabs
}

// Info implement Mallocer{} interface.
func (arena *Arena) Info() (capacity, heap, alloc, overhead int64) {
	self := int64(unsafe.Sizeof(*arena))
	slicesz := int64(cap(arena.slabs) * int(unsafe.Sizeof(int64(1))))
	capacity, overhead = arena.capacity, self+slicesz
	for _, mpools := range arena.mpools {
		_, h, a, o := mpools.Info()
		heap, alloc, overhead = heap+h, alloc+a, overhead+o
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
		_, heap, alloc, _ := arena.mpools[int64(size)].Info()
		if heap == 0 {
			continue
		}
		ss = append(ss, size)
		zs = append(zs, (float64(alloc)/float64(heap))*100)
	}
	return ss, zs
}

func (arena *Arena) adaptiveNumchunks(size, npools int64) int64 {
	maxchunk, numchunks := Maxchunks, int64(1)<<uint64(npools)
	if size < 512 {
		maxchunk = arena.maxchunks[0]
	} else if size < 1024 {
		maxchunk = arena.maxchunks[1]
	} else if size < (16 * 1024) {
		maxchunk = arena.maxchunks[2]
	} else if size < (128 * 1024) {
		maxchunk = arena.maxchunks[3]
	} else if size < (1024 * 1024) {
		maxchunk = arena.maxchunks[4]
	} else if size < (16 * 1024 * 1024) {
		maxchunk = arena.maxchunks[5]
	} else {
		return 1
	}
	if npools > 16 || numchunks > maxchunk {
		return maxchunk
	}
	return numchunks
}

// marker is typically 512, 1K, 16K, 128K, 1M, 16M
func (arena *Arena) maxchunksSize(capacity, marker int64) int64 {
	if marker > capacity {
		return -1
	}
	maxchunks := capacity / (marker * 1000)
	if maxchunks == 0 {
		maxchunks = 1
	} else if maxchunks > Maxchunks {
		maxchunks = Maxchunks
	}
	return maxchunks
}

func osmalloc(size int) uintptr {
	return (uintptr)(C.malloc(C.size_t(size)))
}
