// +build !debug

package malloc

import "reflect"
import "unsafe"

func initblock(block uintptr, size int64) {
	var dst []byte
	initsz := len(zeroblkinit)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Data, sl.Len = block, initsz
	for i := int64(0); i < size/int64(initsz); i++ {
		copy(dst, zeroblkinit)
		sl.Data = (uintptr)(uint64(sl.Data) + uint64(initsz))
	}
	if sl.Len = int(size) % len(zeroblkinit); sl.Len > 0 {
		copy(dst, zeroblkinit)
	}
}
