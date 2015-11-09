package llrb

import "unsafe"
import "reflect"

const nvaluesize = 8 // plus value size
type nodevalue struct {
	hdr1     uint64 // vsize(32), blksize(20)
	pool     *mempool
	valstart unsafe.Pointer // just a place-holder
}

func (v *nodevalue) setblocksize(size int) *nodevalue {
	v.hdr1 = (v.hdr1 & 0xfffffffffff00000) | (uint64(size) & 0xfffff)
	return v
}

func (v *nodevalue) blocksize() int {
	return int(v.hdr1 & 0xfffff)
}

func (v *nodevalue) setvalsize(size int) *nodevalue {
	v.hdr1 = (v.hdr1 & 0xfff00000000fffff) | ((uint64(size) & 0xffffffff) << 20)
	return v
}

func (v *nodevalue) valsize() int {
	return int((v.hdr1 & 0xffffffff00000) >> 20)

}
func (v *nodevalue) setvalue(val []byte) *nodevalue {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Len = len(val)
	sl.Cap = len(val)
	sl.Data = (uintptr)(unsafe.Pointer(&v.valstart))
	return v.setvalsize(copy(dst, val))
}

func (v *nodevalue) value() (val []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&val))
	sl.Len = v.valsize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&v.valstart))
	return
}
