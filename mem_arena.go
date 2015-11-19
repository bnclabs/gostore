package llrb

import "unsafe"
import "sort"
import "fmt"

// MEMUtilization expected.
const MEMUtilization = 0.95

const sizeinterval = 32
const maxpools = 256                           // len(arena.blocksizes)
const maxarenasize = 1024 * 1024 * 1024 * 1024 // 1TB

type memarena struct {
	minblock   int64              // minimum block size allocatable by arena
	maxblock   int64              // maximum block size allocatable by arena
	capacity   int64              // memory capacity to be managed by this arena
	pcapacity  int64              // maximum capacity for a single pool
	blocksizes []int64            // sorted list of block-sizes in this arena
	mpools     map[int64]mempools // size -> sorted list of mempool
}

func newmemarena(minblock, maxblock, capacity, pcapacity int64) *memarena {
	arena := &memarena{
		minblock:  minblock,
		maxblock:  maxblock,
		capacity:  capacity,
		pcapacity: pcapacity,
		mpools:    make(map[int64]mempools),
	}
	arena.blocksizes = Blocksizes(arena.minblock, arena.maxblock)
	if len(arena.blocksizes) > 256 || capacity > maxarenasize {
		panic(fmt.Errorf("number of pools in arena exeeds %v", maxpools))
	}
	for _, size := range arena.blocksizes {
		arena.mpools[size] = make(mempools, 0, maxpools)
	}
	return arena
}

//---- operations

func (arena *memarena) alloc(n int64) (ptr unsafe.Pointer, mpool *mempool) {
	var ok bool
	// check argument
	if largest := arena.blocksizes[len(arena.blocksizes)-1]; n > largest {
		return nil, nil
	}
	// try to get from existing pool
	size := SuitableSize(arena.blocksizes, n)
	for _, mpool = range arena.mpools[size] {
		if ptr, ok = mpool.alloc(); ok {
			return ptr, mpool
		}
	}
	// okay create a new pool, figure the dimensions.
	numblocks := (arena.capacity / int64(len(arena.blocksizes))) / size
	if int64(numblocks*size) > arena.pcapacity {
		numblocks = arena.pcapacity / size
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
		allocated += mpools[0].capacity * int64(len(mpools))
	}
	if allocated > arena.capacity {
		panic(ErrorOutofMemory)
	}
	// go ahead, create a new pool.
	mpool = newmempool(size, numblocks)
	arena.mpools[size] = append(arena.mpools[size], mpool)
	sort.Sort(arena.mpools[size])
	ptr, _ = mpool.alloc()
	return ptr, mpool
}

func (arena *memarena) release() {
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			mpool.release()
		}
	}
	arena.blocksizes, arena.mpools = nil, nil
}

//---- statistics and maintenance

func (arena *memarena) memory() (overhead, useful int64) {
	self := int64(unsafe.Sizeof(*arena))
	slicesz := int64(cap(arena.blocksizes) * int(unsafe.Sizeof(int64(1))))
	overhead += self + slicesz
	for _, mpools := range arena.mpools {
		for _, mpool := range mpools {
			x, y := mpool.memory()
			overhead += x
			useful += y
		}
	}
	return
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
	return arena.capacity - arena.allocated()
}

func SuitableSize(sizes []int64, size int64) int64 {
	switch len(sizes) {
	case 1:
		return sizes[0]

	case 2:
		if size <= sizes[0] {
			return sizes[0]
		}
		return sizes[1]

	default:
		pivot := len(sizes) / 2
		if sizes[pivot] < size {
			return SuitableSize(sizes[pivot+1:], size)
		}
		return SuitableSize(sizes[:pivot+1], size)
	}
}

func Blocksizes(minblock, maxblock int64) []int64 {
	if maxblock < minblock { // validate and cure the input params
		panic("minblock < maxblock")
	} else if (minblock % sizeinterval) != 0 {
		panic(fmt.Errorf("minblock is not multiple of %v", sizeinterval))
	} else if (maxblock % sizeinterval) != 0 {
		panic(fmt.Errorf("maxblock is not multiple of %v", sizeinterval))
	}

	nextsize := func(from int64) int64 {
		addby := int64(float64(from) * (1.0 - MEMUtilization))
		if addby <= 32 {
			addby = 32
		}
		size := from + addby
		for (float64(from+size)/2.0)/float64(size) > MEMUtilization {
			size += addby
		}
		return size
	}

	sizes := make([]int64, 0, maxpools)
	for size := minblock; size < maxblock; {
		sizes = append(sizes, size)
		size = nextsize(size)
	}
	sizes = append(sizes, maxblock)
	return sizes
}
