// +build debug

package malloc

import "reflect"
import "unsafe"

func initblock(block uintptr, size int64) {
	var dst []byte
	initsz := len(poolblkinit)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Data, sl.Len = block, initsz
	for i := int64(0); i < size/int64(initsz); i++ {
		copy(dst, poolblkinit)
		sl.Data = (uintptr)(uint64(sl.Data) + uint64(initsz))
	}
	if sl.Len = int(size) % len(poolblkinit); sl.Len > 0 {
		copy(dst, poolblkinit)
	}
}
