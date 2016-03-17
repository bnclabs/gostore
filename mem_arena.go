// hard limits:
//
// sizeinterval						   - 32
// maximum no. of pools per chunk size - 256
// maximum size of an arena            - 2^40

package storage

import "unsafe"
import "fmt"

const MEMUtilization = 0.95

const sizeinterval = 32
const maxpools = 256                           // len(arena.blocksizes)
const maxarenasize = 1024 * 1024 * 1024 * 1024 // 1TB

// each arena manages mem-pool of different chunk-size,
// for each chunk-size can have zero or more mem-pools
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
	if len(arena.blocksizes) > 256 {
		panic(fmt.Errorf("number of pools in arena exeeds %v", maxpools))
	} else if capacity > maxarenasize {
		fmsg := "arena cannot exceed %v bytes (%v)"
		panic(fmt.Errorf(fmsg, capacity, maxarenasize))
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
		fmsg := "alloc size %v exceeds maxblock size %v"
		panic(fmt.Errorf(fmsg, n, largest))
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
	if numblocks > maxpoolblocks {
		numblocks = maxpoolblocks
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
	mpools := arena.mpools[size]
	mpools = append(mpools, mpool) // make more space
	mpools[0], mpools[len(mpools)-1] = mpools[len(mpools)-1], mpools[0]
	arena.mpools[size] = mpools

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

// SuitableSize for size, pick the optimal block-size among the
// list of blocksizes to acheive MEMUtilization.
func SuitableSize(blocksizes []int64, size int64) int64 {
	for {
		switch len(blocksizes) {
		case 1:
			return blocksizes[0]

		case 2:
			if size <= blocksizes[0] {
				return blocksizes[0]
			} else if size <= blocksizes[1] {
				return blocksizes[1]
			}
			panic("size greater than configured")

		default:
			pivot := len(blocksizes) / 2
			if blocksizes[pivot] < size {
				blocksizes = blocksizes[pivot+1:]
			} else {
				blocksizes = blocksizes[0 : pivot+1]
			}
		}
	}
}

// Blocksizes for allocation size that can range between minblock
// to maxblock, compute the minimum number of block sizes to manage
// without degrading below MEMUtilization.
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
		} else if addby&0x1f != 0 {
			addby = (addby >> 5) << 5
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
