package malloc

import "fmt"
import "errors"

var ErrorExceedCapacity = errors.New("malloc.exceedCapacity")

var ErrorOutofMemory = errors.New("malloc.outofmemory")

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
	} else if (minblock % Alignment) != 0 {
		fmsg := "minblock %v is not multiple of %v"
		panic(fmt.Errorf(fmsg, minblock, Alignment))
	} else if (maxblock % Alignment) != 0 {
		fmsg := "maxblock %v is not multiple of %v"
		panic(fmt.Errorf(fmsg, maxblock, Alignment))
	}

	nextsize := func(from int64) int64 {
		addby := int64(float64(from) * (1.0 - MEMUtilization))
		if addby <= Alignment {
			addby = Alignment
		} else if mod := (addby % Alignment); mod != 0 {
			addby += Alignment - mod
		}
		size := from + addby
		// NOTE: TestBlocksizes() experiment indicates that addby is
		// sufficiently close to MEMUtilization.
		//for (float64(from+size)/2.0)/float64(size) > MEMUtilization {
		//	fmt.Println("nextsize", from, size,
		//		(float64(from+size)/2.0)/float64(size))
		//	size += Alignment
		//}
		return size
	}

	sizes := make([]int64, 0, 64)
	for from := minblock + Alignment; from < maxblock; {
		sizes = append(sizes, from)
		from = nextsize(from)
	}
	sizes = append(sizes, maxblock)
	return sizes
}

func panicerr(fmsg string, args ...interface{}) {
	panic(fmt.Errorf(fmsg, args...))
}

var poolblkinit = make([]byte, 1024)
var zeroblkinit = make([]byte, 1024)

func init() {
	for i := 0; i < len(poolblkinit); i++ {
		poolblkinit[i] = 0xff
	}
	for i := 0; i < len(zeroblkinit); i++ {
		zeroblkinit[i] = 0x00
	}
}
