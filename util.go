package llrb

import "fmt"
import "sort"

func findfirstset8(b byte) int8 { // move this to ASM.
	for i := uint8(0); i < 8; i++ {
		if (b & (1 << i)) != 0 {
			return int8(i)
		}
	}
	return -1
}

func clearbit8(b byte, n uint8) byte { // move this to ASM.
	return b & (0xff ^ (1 << n))
}

func setbit8(b byte, n uint8) byte { // move this to ASM.
	return b | (1 << n)
}

func onesin8(v byte) (c int8) { // move this to ASM.
	for c = 0; v != 0; v >>= 1 { // count set bits
		c += int8(v & 1)
	}
	return c
}

func zerosin8(v byte) int8 { // move this to ASM.
	return 8 - onesin8(v)
}

func onesin32(v uint32) int8 { // move this to ASM.
	v = v - ((v >> 1) & 0x55555555)
	v = (v & 0x33333333) + ((v >> 2) & 0x33333333)
	return int8((((v + (v >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24)
}

func zerosin32(v uint32) int8 {
	return 32 - onesin32(v)
}

func poolutilz(prefix string, arenapools map[int64]mempools) {
	sizes := []int{}
	for size := range arenapools {
		sizes = append(sizes, int(size))
	}
	sort.Ints(sizes)

	for _, size := range sizes {
		mpools := arenapools[int64(size)]
		allocated, capacity := int64(0), int64(0)
		if len(mpools) > 0 {
			for _, mpool := range mpools {
				allocated += mpool.allocated()
				capacity += mpool.capacity
			}
			utilz := (float64(allocated) / float64(capacity)) * 100
			fmt.Printf("%v%v %v %2.2f\n", prefix, size, capacity, utilz)
		}
	}
}
