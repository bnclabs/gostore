// +build !debug

package malloc

import "reflect"
import "unsafe"

func initblock(block uintptr, size int64) {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	if size > 80 { // TODO: make sure that it is alteast metadata size.
		size = 80
	}
	sl.Data, sl.Len = block, int(size)
	copy(dst, zeroblkinit)
}
