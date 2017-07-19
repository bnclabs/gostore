package malloc

import "fmt"

// TODO: remove panicerr.
// TODO: create a slab structure that maintains the blocksizes
// and provides suitable-size method.

// SuitableSize return an optimal block-size for required size.
// Argument blocksizes should be sorted array of int64. Will panic
// if requested size is greated than configured range of size.
func SuitableSize(blocksizes []int64, size int64) int64 {
	if size > blocksizes[len(blocksizes)-1] {
		panic("size greater than configured")
	}

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
// maxblock-size. This is to achieve optimal memory-utilization. If
// minblock is less than or equal to maxblock, or minblock and maxblock
// are not aligned, this function will panic.
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
