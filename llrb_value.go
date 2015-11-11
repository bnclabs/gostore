package llrb

import "unsafe"
import "reflect"

const nvaluesize = 8 // plus value size
type nodevalue struct {
	hdr1     uint64 // vsize(32), blksize(20)
	pool     *mempool
	valstart unsafe.Pointer // just a place-holder
}

func (nv *nodevalue) setblocksize(size int) *nodevalue {
	nv.hdr1 = (nv.hdr1 & 0xfffffffffff00000) | (uint64(size) & 0xfffff)
	return nv
}

func (nv *nodevalue) blocksize() int {
	return int(nv.hdr1 & 0xfffff)
}

func (nv *nodevalue) setvalsize(size int) *nodevalue {
	nv.hdr1 = (nv.hdr1 & 0xfff00000000fffff) | ((uint64(size) & 0xffffffff) << 20)
	return nv
}

func (nv *nodevalue) valsize() int {
	return int((nv.hdr1 & 0xffffffff00000) >> 20)

}
func (nv *nodevalue) setvalue(val []byte) *nodevalue {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Len = len(val)
	sl.Cap = len(val)
	sl.Data = (uintptr)(unsafe.Pointer(&nv.valstart))
	return nv.setvalsize(copy(dst, val))
}

func (nv *nodevalue) value() (val []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&val))
	sl.Len = nv.valsize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nv.valstart))
	return
}
