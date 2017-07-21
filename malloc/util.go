package malloc

import "fmt"

// TODO: remove panicerr.
// TODO: create a slab structure that maintains the all slabs
// and provides suitable-size method.

// SuitableSlab return an optimal block-size for required size.
// Argument slabs should be sorted array of int64. Will panic
// if requested size is greated than configured range of size.
func SuitableSlab(slabs []int64, size int64) int64 {
	if size > slabs[len(slabs)-1] {
		panic("size greater than configured")
	}

	for {
		switch len(slabs) {
		case 1:
			return slabs[0]

		case 2:
			if size <= slabs[0] {
				return slabs[0]
			} else if size <= slabs[1] {
				return slabs[1]
			}
			panic("size greater than configured")

		default:
			pivot := len(slabs) / 2
			if slabs[pivot] < size {
				slabs = slabs[pivot+1:]
			} else {
				slabs = slabs[0 : pivot+1]
			}
		}
	}
}

// Computeslabs generate suitable block-sizes between minblock-size and
// maxblock-size. This is to achieve optimal memory-utilization. If
// minblock is less than or equal to maxblock, or minblock and maxblock
// are not aligned, this function will panic.
func Computeslabs(minblock, maxblock int64) []int64 {
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

// ChunksPerPool calculates the fair number of entries to each slab.
func ChunksPerPool(slabs []int64, capacity int64) int64 {
	sum := int64(0)
	for _, x := range slabs {
		sum += x
	}
	fairchunks := capacity / sum
	if fairchunks > Maxchunks {
		fairchunks = Maxchunks
	}
	if fairchunks < Alignment {
		fairchunks = Alignment
	}
	if mod := fairchunks % Alignment; mod != 0 {
		fairchunks += Alignment - mod
	}
	return fairchunks
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
