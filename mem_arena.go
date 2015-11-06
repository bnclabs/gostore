package llrb

import "unsafe"
import "sort"
import "fmt"

// MEMUtilization expected.
const MEMUtilization = 0.95

const sizeinterval = 32
const maxpools = 256 // len(arena.blocksizes) == len(arena.mpools)

type memarena struct {
	minblock   int              // minimum block size allocatable by arena
	maxblock   int              // maximum block size allocatable by arena
	numblocks  int              // number of blocks in a pool
	blocksizes []int            // sorted list of block-sizes in this arena
	mpools     map[int]mempools // size -> sorted list of mempool
}

func newmemarena(minblock, maxblock, numblocks int) *memarena {
	arena := &memarena{
		minblock:  minblock,
		maxblock:  maxblock,
		numblocks: numblocks,
		mpools:    make(map[int]mempools),
	}
	arena.blocksizes = Blocksizes(arena.minblock, arena.maxblock)
	if len(arena.blocksizes) > 256 {
		panic(fmt.Errorf("number of pools in arena exeeds %v", maxpools))
	}
	for _, size := range arena.blocksizes {
		arena.mpools[size] = make(mempools, 0)
	}
	return arena
}

func (arena *memarena) alloc(n int) (ptr unsafe.Pointer, mpool *mempool) {
	var ok bool

	if largest := arena.blocksizes[len(arena.blocksizes)-1]; n > largest {
		return nil, nil
	}
	size := SuitableSize(arena.blocksizes, n)
	for _, mpool = range arena.mpools[size] {
		if ptr, ok = mpool.alloc(); ok {
			return ptr, mpool
		}
	}
	mpool = newmempool(size, arena.numblocks)
	arena.mpools[size] = append(arena.mpools[size], mpool)
	sort.Sort(arena.mpools[size])
	ptr, _ = mpool.alloc()
	return ptr, mpool
}

func (arena *memarena) memory() int64 {
	mem := int64(0)
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			mem += mpool.capacity
		}
	}
	return mem
}

func (arena *memarena) allocated() int64 {
	allocated := int64(0)
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			allocated += mpool.allocated()
		}
	}
	return allocated
}

func (arena *memarena) available() int64 {
	return arena.memory() - arena.allocated()
}

func (arena *memarena) release() {
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			mpool.release()
		}
	}
}

func SuitableSize(sizes []int, minsize int) int {
	switch len(sizes) {
	case 1:
		return sizes[0]

	case 2:
		if minsize < sizes[0] {
			return sizes[0]
		}
		return sizes[1]

	default:
		pivot := len(sizes) / 2
		if sizes[pivot] < minsize {
			return SuitableSize(sizes[pivot+1:], minsize)
		}
		return SuitableSize(sizes[:pivot+1], minsize)
	}
}

func Blocksizes(minblock, maxblock int) []int {
	if maxblock < minblock { // validate and cure the input params
		panic("minblock < maxblock")
	} else if (maxblock % sizeinterval) != 0 {
		maxblock = maxblock + (sizeinterval - (maxblock % sizeinterval))
	}

	nextsize := func(from int) int {
		size := from + 32
		for (float64(from+size)/2.0)/float64(size) > MEMUtilization {
			size += 32
		}
		return size
	}

	sizes := []int{}
	for size := minblock; size <= maxblock; {
		sizes = append(sizes, size)
		size = nextsize(size)
	}
	return sizes
}
