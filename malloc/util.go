package malloc

import "fmt"
import "errors"

var ErrorOutofMemory = errors.New("llrb.outofmemory")

// SuitableSize picks an optimal block-size for given size,
// to achieve MEMUtilization.
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

// Blocksizes generate suitable block-sizes between minblock-size and
// maxblock-size, to acheive MEMUtilization.
func Blocksizes(minblock, maxblock int64) []int64 {
	if maxblock < minblock { // validate and cure the input params
		panic("minblock < maxblock")
	} else if (minblock % Sizeinterval) != 0 {
		fmsg := "minblock %v is not multiple of %v"
		panic(fmt.Errorf(fmsg, minblock, Sizeinterval))
	} else if (maxblock % Sizeinterval) != 0 {
		panic(fmt.Errorf("maxblock is not multiple of %v", Sizeinterval))
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

	sizes := make([]int64, 0, 64)
	for size := minblock; size < maxblock; {
		sizes = append(sizes, size)
		size = nextsize(size)
	}
	sizes = append(sizes, maxblock)
	return sizes
}

func panicerr(fmsg string, args ...interface{}) {
	panic(fmt.Errorf(fmsg, args...))
}

var poolblkinit = make([]byte, 1024)

func init() {
	for i := 0; i < len(poolblkinit); i++ {
		poolblkinit[i] = 0xff
	}
}
