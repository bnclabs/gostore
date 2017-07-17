// hardlimits:
//
// maximum size of value : 2^40 bytes

package llrb

import "unsafe"
import "reflect"

import "github.com/prataprc/gostore/api"

const nvaluesize = int(unsafe.Sizeof(nodevalue{})) - 8 // + valuesize

type nodevalue struct {
	hdr1     uint64 // valuesize[39:]
	pool     api.MemoryPool
	valstart unsafe.Pointer // just a place-holder
}

func (nv *nodevalue) sizeof() int {
	return int(unsafe.Sizeof(*nv))
}

func (nv *nodevalue) setvalsize(size int64) *nodevalue {
	if nv != nil {
		nv.hdr1 = (nv.hdr1 & 0xffffff0000000000) | (uint64(size) & 0xffffffffff)
	}
	return nv
}

func (nv *nodevalue) valsize() int {
	return int(nv.hdr1 & 0xffffffffff)

}

func (nv *nodevalue) setvalue(val []byte) *nodevalue {
	if nv != nil {
		var dst []byte
		sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
		sl.Len = len(val)
		sl.Cap = len(val)
		sl.Data = (uintptr)(unsafe.Pointer(&nv.valstart))
		return nv.setvalsize(int64(copy(dst, val)))
	}
	return nv
}

func (nv *nodevalue) value() (val []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&val))
	sl.Len = nv.valsize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nv.valstart))
	return
}
