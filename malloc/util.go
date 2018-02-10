package malloc

import "unsafe"
import "reflect"

// SuitableSlab return an optimal block-size for required size.
// Argument slabs should be sorted array of int64.
func SuitableSlab(slabs []int64, size int64) int64 {
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
			panic("unreachable code")

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

// Computeslabs generate suitable block-sizes between 0 bytes to 1TB.
// This is to achieve optimal MEMUtilization.
func Computeslabs() []int64 {
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

	minblock, maxblock := int64(0), int64(1024*1024*1024*1024)
	sizes := make([]int64, 0, 64)
	for from := minblock + Alignment; from < maxblock; {
		sizes = append(sizes, from)
		from = nextsize(from)
	}
	sizes = append(sizes, maxblock)
	return sizes
}

var zeroblkinit = make([]byte, 1024)

func init() {
	for i := range zeroblkinit {
		zeroblkinit[i] = 0x00
	}
}

func zeropoolblock(block uintptr, size int64) {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Data, sl.Len, sl.Cap = block, int(size), int(size)
	for i := range dst {
		dst[i] = 0
	}
}

func initblock(block uintptr, size int64) {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	if size > 64 { // TODO: make sure that it is alteast metadata size.
		size = 64
	}
	sl.Data, sl.Len = block, int(size)
	copy(dst, zeroblkinit)
}
