package llrb

import "unsafe"
import "bytes"
import "reflect"

const nodesize = 56 // plus key size.
type node struct {
	hdr1     uint64 // dirty, black, ksize(12), blksize(20)
	pool     *mempool
	left     *node
	right    *node
	mvalue   unsafe.Pointer
	fpos     int64          // file-position
	stat1    uint64         // ts[:48]
	keystart unsafe.Pointer // just a place-holder
}

//---- field operations

func (nd *node) setblocksize(size int) *node {
	nd.hdr1 = (nd.hdr1 & 0xfffffffffff00000) | (uint64(size) & 0xfffff)
	return nd
}

func (nd *node) blocksize() int {
	return int(nd.hdr1 & 0xfffff)
}

func (nd *node) setkeysize(size int) *node {
	nd.hdr1 = (nd.hdr1 & 0xffffffff000fffff) | ((uint64(size) & 0x3ff) << 20)
	return nd
}

func (nd *node) keysize() int {
	return int((nd.hdr1 & 0x3ff00000) >> 20)
}

func (nd *node) isred() bool {
	return (nd.hdr1 & 0x100000000) == 0
}

func (nd *node) isblack() bool {
	return !nd.isred()
}

func (nd *node) setblack() *node {
	nd.hdr1 = nd.hdr1 | 0x100000000
	return nd
}

func (nd *node) setred() *node {
	nd.hdr1 = nd.hdr1 & 0xfffffffeffffffff
	return nd
}

func (nd *node) togglelink() *node {
	nd.hdr1 = nd.hdr1 ^ 0x100000000
	return nd
}

func (nd *node) isdirty() bool {
	return (nd.hdr1 & 0x200000000) == 1
}

func (nd *node) setdirty() *node {
	nd.hdr1 = nd.hdr1 | 0x200000000
	return nd
}

func (nd *node) cleardirty() *node {
	nd.hdr1 = nd.hdr1 & 0xfffffffdffffffff
	return nd
}

func (nd *node) settimestamp(ts int64) *node { // ts is time.Now().UnixNano()
	ts = ts >> 16
	nd.stat1 = (nd.hdr1 & 0xffff000000000000) | (uint64(ts) & 0xffffffffffff)
	return nd
}

func (nd *node) timestamp() int64 {
	return int64(nd.stat1 & 0xffffffffffff)
}

func (nd *node) setkey(key []byte) *node {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Len = len(key)
	sl.Cap = len(key)
	sl.Data = (uintptr)(unsafe.Pointer(&nd.keystart))
	return nd.setkeysize(copy(dst, key))
}

func (nd *node) key() (k []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nd.keystart))
	return
}

func (nd *node) setvalue(val []byte) *node {
	if nd.mvalue != nil {
		v := (*nodevalue)(nd.mvalue)
		v.setvalue(val)
	}
	return nd
}

func (nd *node) value() (val []byte) {
	// TODO: if mvalue is nil, get the value from fpos
	if nd.mvalue != nil {
		v := (*nodevalue)(nd.mvalue)
		val = v.value()
	}
	return
}

func (nd *node) ltkey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nd.keystart))
	cmp := bytes.Compare(key, other)
	return cmp == -1
}

func (nd *node) lekey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nd.keystart))
	cmp := bytes.Compare(key, other)
	return cmp == -1 || cmp == 0
}

func (nd *node) gtkey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nd.keystart))
	cmp := bytes.Compare(key, other)
	return cmp == 1
}

func (nd *node) gekey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	sl.Data = (uintptr)(unsafe.Pointer(&nd.keystart))
	cmp := bytes.Compare(key, other)
	return cmp == 0 || cmp == 1
}

//---- tree operations

func rotateleft(nd *node) *node {
	x := nd.right
	if x.isblack() {
		panic("rotating a black link")
	}
	nd.right = x.left
	x.left = nd
	if nd.isblack() {
		x.setblack()
	} else {
		x.setred()
	}
	nd.setred()
	return x
}

func rotateright(nd *node) *node {
	x := nd.left
	if x.isblack() {
		panic("rotating a black link")
	}
	nd.left = x.right
	x.right = nd
	if nd.isblack() {
		x.setblack()
	} else {
		x.setred()
	}
	nd.setred()
	return x
}

func flip(nd *node) {
	nd.togglelink()
	nd.left.togglelink()
	nd.right.togglelink()
}

func moveredleft(nd *node) *node {
	flip(nd)
	if nd.right.left.isred() {
		nd.right = rotateright(nd.right)
		nd = rotateleft(nd)
		flip(nd)
	}
	return nd
}

func moveredright(nd *node) *node {
	flip(nd)
	if nd.left.left.isred() {
		nd = rotateright(nd)
		flip(nd)
	}
	return nd
}

func fixup(nd *node) *node {
	if nd.right.isred() {
		nd = rotateleft(nd)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd = rotateright(nd)
	}
	if nd.left.isred() && nd.right.isred() {
		flip(nd)
	}
	return nd
}
