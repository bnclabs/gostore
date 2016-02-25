package storage

import "unsafe"
import "bytes"
import "reflect"
import "strings"
import "fmt"
import "io"

const llrbnodesize = 24 // + metadatasize + keysize

type Llrbnode struct {
	left     *Llrbnode // TODO: unsafe.Pointer ???
	right    *Llrbnode // TODO: unsafe.Pointer ???
	pool     *mempool
	mdmarker unsafe.Pointer
}

//---- Exported methods on metadata.

// Setvbno implements NodeSetter{}
func (nd *Llrbnode) Setvbno(vbno uint16) *Llrbnode {
	if nd != nil {
		nd.metadata().setvbno(vbno)
	}
	return nd
}

// SetBornseqno implements NodeSetter{}
func (nd *Llrbnode) SetBornseqno(seqno uint64) *Llrbnode {
	if nd != nil {
		nd.metadata().setbnseq(seqno)
	}
	return nd
}

// SetDeadseqno implements NodeSetter{}
func (nd *Llrbnode) SetDeadseqno(seqno uint64) *Llrbnode {
	if nd != nil {
		nd.metadata().setddseq(seqno)
	}
	return nd
}

// SetVbuuid implements NodeSetter{}
func (nd *Llrbnode) SetVbuuid(vbuuid uint64) *Llrbnode {
	if nd != nil {
		nd.metadata().setvbuuid(vbuuid)
	}
	return nd
}

// Vbno implements NodeGetter{}
func (nd *Llrbnode) Vbno() uint16 {
	if nd != nil {
		return nd.metadata().vbno()
	}
	return 0
}

// Bornseqno implements NodeGetter{}
func (nd *Llrbnode) Bornseqno() uint64 {
	if nd != nil {
		return nd.metadata().bnseq()
	}
	return 0
}

// Deadseqno implements NodeGetter{}
func (nd *Llrbnode) Deadseqno() uint64 {
	if nd != nil {
		return nd.metadata().ddseq()
	}
	return 0
}

// Vbuuid implements NodeGetter{}
func (nd *Llrbnode) Vbuuid() uint64 {
	if nd != nil {
		return nd.metadata().vbuuid()
	}
	return 0
}

// Key implements NodeGetter{}
func (nd *Llrbnode) Key() []byte {
	if nd != nil {
		return nd.key()
	}
	return nil
}

// Value implements NodeGetter{}
func (nd *Llrbnode) Value() []byte {
	if nd != nil {
		return nd.nodevalue().value()
	}
	return nil
}

//---- local methods on metadata.

func (nd *Llrbnode) metadata() *metadata {
	return (*metadata)(unsafe.Pointer(&nd.mdmarker))
}

func (nd *Llrbnode) setkey(key []byte) *Llrbnode {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Len = len(key)
	sl.Cap = len(key)
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	return nd.setkeysize(copy(dst, key))
}

func (nd *Llrbnode) setkeysize(size int) *Llrbnode {
	nd.metadata().setkeysize(size)
	return nd
}

func (nd *Llrbnode) setnodevalue(nv *nodevalue) *Llrbnode {
	arg := (uintptr)(unsafe.Pointer(nv))
	nd.metadata().setmvalue(uint64(arg), 0)
	return nd
}

func (nd *Llrbnode) key() (k []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	return
}

func (nd *Llrbnode) keysize() int {
	return nd.metadata().keysize()
}

func (nd *Llrbnode) nodevalue() *nodevalue {
	nv, _ := nd.metadata().mvalue()
	return (*nodevalue)(unsafe.Pointer(uintptr(nv)))
}

//---- maintanence methods.

func (nd *Llrbnode) repr() string {
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

func (nd *Llrbnode) pprint(prefix string) {
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

func (nd *Llrbnode) dotdump(buffer io.Writer) {
	key := string(nd.key())
	mddot := nd.metadata().dotdump()
	lines := []string{
		fmt.Sprintf("  %v [label=\"{%v|%v}\"];\n", key, key, mddot),
	}
	if nd.left != nil {
		line := fmt.Sprintf("  %v -> %v;\n", key, string(nd.left.key()))
		lines = append(lines, line)
	}
	if nd.right != nil {
		line := fmt.Sprintf("  %v -> %v;\n", key, string(nd.right.key()))
		lines = append(lines, line)
	}
	buffer.Write([]byte(strings.Join(lines, "")))
	if nd.left != nil {
		nd.left.dotdump(buffer)
	}
	if nd.right != nil {
		nd.right.dotdump(buffer)
	}
}

//---- indexer api

func (nd *Llrbnode) ltkey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == -1
}

func (nd *Llrbnode) lekey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == -1 || cmp == 0
}

func (nd *Llrbnode) gtkey(other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(nd.metadata().sizeof())
	cmp := bytes.Compare(key, other)
	return cmp == 1
}

func (nd *Llrbnode) gekey(other []byte) bool {
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

func isred(nd *Llrbnode) bool {
	if nd == nil {
		return false
	}
	return nd.metadata().isred()
}

func isblack(nd *Llrbnode) bool {
	return !isred(nd)
}
