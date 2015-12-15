package llrb

import "unsafe"
import "bytes"
import "reflect"
import "fmt"

const nodesize = 24 // plus + metadata-size + key-size
type node struct {
	left     *node // TODO: unsafe.Pointer ???
	right    *node // TODO: unsafe.Pointer ???
	pool     *mempool
	mdmarker unsafe.Pointer
}

func (nd *node) repr() string {
	bnseqno, ddseqno := int64(-1), int64(-1)
	if nd.metadata().isbnseq() {
		bnseqno = int64(nd.metadata().bnseq())
	}
	if nd.metadata().isddseq() {
		ddseqno = int64(nd.metadata().ddseq())
	}
	return fmt.Sprintf(
		"%v %v {%v,%v} %v",
		nd.metadata().isdirty(), nd.metadata().isblack(),
		bnseqno, ddseqno, string(nd.key()))
}

func (nd *node) pprint(prefix string) {
	if nd == nil {
		fmt.Printf("%v\n", nd)
		return
	}
	fmt.Printf("%v%v\n", prefix, nd.repr())
	prefix += "  "
	fmt.Printf("%vleft: ", prefix)
	nd.left.pprint(prefix)
	fmt.Printf("%vright: ", prefix)
	nd.right.pprint(prefix)
}

func (nd *node) metadata() *metadata {
	return (*metadata)(unsafe.Pointer(&nd.mdmarker))
}

//---- field operations

func (nd *node) vbno() uint16 {
	return nd.metadata().vbno()
}

func (nd *node) setkeysize(size int) *node {
	nd.metadata().setkeysize(size)
	return nd
}

func (nd *node) keysize() int {
	return nd.metadata().keysize()
}

func (nd *node) setkey(key []byte) *node {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Len = len(key)
	sl.Cap = len(key)
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	return nd.setkeysize(copy(dst, key))
}

func (nd *node) key() (k []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	return
}

func (nd *node) setnodevalue(nv *nodevalue) *node {
	arg := (uintptr)(unsafe.Pointer(nv))
	nd.metadata().setmvalue(uint64(arg), 0)
	return nd
}

func (nd *node) nodevalue() *nodevalue {
	nv, _ := nd.metadata().mvalue()
	return (*nodevalue)(unsafe.Pointer(uintptr(nv)))
}

func (nd *node) ltkey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == -1
}

func (nd *node) lekey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == -1 || cmp == 0
}

func (nd *node) gtkey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == 1
}

func (nd *node) gekey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == 0 || cmp == 1
}

//---- tree operations

func rotateleft(nd *node) *node {
	x := nd.right
	if x.metadata().isblack() {
		panic("rotating a black link")
	}
	nd.right = x.left
	x.left = nd
	if nd.metadata().isblack() {
		x.metadata().setblack()
	} else {
		x.metadata().setred()
	}
	nd.metadata().setred()
	return x
}

func rotateright(nd *node) *node {
	x := nd.left
	if x.metadata().isblack() {
		panic("rotating a black link")
	}
	nd.left = x.right
	x.right = nd
	if nd.metadata().isblack() {
		x.metadata().setblack()
	} else {
		x.metadata().setred()
	}
	nd.metadata().setred()
	return x
}

func flip(nd *node) {
	nd.metadata().togglelink()
	nd.left.metadata().togglelink()
	nd.right.metadata().togglelink()
}

func moveredleft(nd *node) *node {
	flip(nd)
	if isred(nd.right.left) {
		nd.right = rotateright(nd.right)
		nd = rotateleft(nd)
		flip(nd)
	}
	return nd
}

func moveredright(nd *node) *node {
	flip(nd)
	if isred(nd.left.left) {
		nd = rotateright(nd)
		flip(nd)
	}
	return nd
}

func fixup(nd *node) *node {
	if isred(nd.right) {
		nd = rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = rotateright(nd)
	}
	if isred(nd.left) && isred(nd.right) {
		flip(nd)
	}
	return nd
}

func isred(nd *node) bool {
	if nd == nil {
		return false
	}
	return nd.metadata().isred()
}

func isblack(nd *node) bool {
	return !isred(nd)
}
