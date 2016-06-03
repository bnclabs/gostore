package llrb

import "unsafe"
import "bytes"
import "reflect"
import "strings"
import "fmt"
import "io"

import "github.com/prataprc/storage.go/api"

const llrbnodesize = 32 // + metadatasize + keysize

// Llrbnode defines a node in LLRB tree.
type Llrbnode struct {
	left     *Llrbnode // TODO: unsafe.Pointer ???
	right    *Llrbnode // TODO: unsafe.Pointer ???
	pool     api.Mallocer
	mdmarker unsafe.Pointer
}

//---- Exported methods for metadata.

// Setvbno implement NodeSetter{}
func (nd *Llrbnode) Setvbno(vbno uint16) api.Node {
	if nd != nil {
		nd.metadata().setvbno(vbno)
	}
	return nd
}

// Setaccess implement NodeSetter{}
func (nd *Llrbnode) Setaccess(access uint64) api.Node {
	if nd != nil {
		nd.metadata().setaccess(access)
	}
	return nd
}

// SetBornseqno implemens NodeSetter{}
func (nd *Llrbnode) SetBornseqno(seqno uint64) api.Node {
	if nd != nil {
		nd.metadata().setbnseq(seqno)
	}
	return nd
}

// SetDeadseqno implement NodeSetter{}
func (nd *Llrbnode) SetDeadseqno(seqno uint64) api.Node {
	if nd != nil {
		nd.metadata().setddseq(seqno)
	}
	return nd
}

// SetVbuuid implement NodeSetter{}
func (nd *Llrbnode) SetVbuuid(vbuuid uint64) api.Node {
	if nd != nil {
		nd.metadata().setvbuuid(vbuuid)
	}
	return nd
}

// SetFpos implement NodeSetter{}
func (nd *Llrbnode) SetFpos(level byte, offset uint64) api.Node {
	if nd != nil {
		nd.metadata().setfpos(level, offset)
	}
	return nd
}

// Vbno implement NodeGetter{}
func (nd *Llrbnode) Vbno() uint16 {
	if nd != nil {
		return nd.metadata().vbno()
	}
	return 0
}

// Access implement NodeGetter{}
func (nd *Llrbnode) Access() uint64 {
	if nd != nil {
		return nd.metadata().access()
	}
	return 0
}

// Key implement NodeGetter{}
func (nd *Llrbnode) Key() []byte {
	if nd != nil {
		return nd.key(nd.metadata().sizeof())
	}
	return nil
}

// Bornseqno implement NodeGetter{}
func (nd *Llrbnode) Bornseqno() uint64 {
	if nd != nil {
		return nd.metadata().bnseq()
	}
	return 0
}

// Deadseqno implement NodeGetter{}
func (nd *Llrbnode) Deadseqno() uint64 {
	if nd != nil {
		return nd.metadata().ddseq()
	}
	return 0
}

// Vbuuid implement NodeGetter{}
func (nd *Llrbnode) Vbuuid() uint64 {
	if nd != nil {
		return nd.metadata().vbuuid()
	}
	return 0
}

// Fpos implement NodeGetter{}
func (nd *Llrbnode) Fpos() (level byte, offset uint64) {
	if nd != nil {
		return nd.metadata().fpos()
	}
	return 0, 0
}

// Value implement NodeGetter{}
func (nd *Llrbnode) Value() []byte {
	if nd != nil && nd.metadata().ismvalue() {
		return nd.nodevalue().value()
	}
	return nil
}

//---- local methods on metadata.

func (nd *Llrbnode) metadata() *metadata {
	return (*metadata)(unsafe.Pointer(&nd.mdmarker))
}

func (nd *Llrbnode) setkey(mdsize int, key []byte) *Llrbnode {
	var dst []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Len = len(key)
	sl.Cap = len(key)
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(mdsize)
	return nd.setkeysize(copy(dst, key))
}

func (nd *Llrbnode) setkeysize(size int) *Llrbnode {
	nd.metadata().setkeysize(size)
	return nd
}

func (nd *Llrbnode) setnodevalue(nv *nodevalue) *Llrbnode {
	arg := (uintptr)(unsafe.Pointer(nv))
	nd.metadata().setmvalue(uint64(arg))
	return nd
}

func (nd *Llrbnode) key(mdsize int) (k []byte) {
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&k))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(mdsize)
	return
}

func (nd *Llrbnode) keysize() int {
	return nd.metadata().keysize()
}

func (nd *Llrbnode) nodevalue() *nodevalue {
	nv := nd.metadata().mvalue()
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
		bnseqno, ddseqno, string(nd.key(nd.metadata().sizeof())))
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
	if nd == nil {
		return
	}

	whatcolor := func(childnd *Llrbnode) string {
		if isred(childnd) {
			return "red"
		}
		return "black"
	}

	mdsize := nd.metadata().sizeof()
	key := string(nd.key(mdsize))
	mddot := nd.metadata().dotdump()
	lines := []string{
		fmt.Sprintf("  %v [label=\"{%v|%v}\"];\n", key, key, mddot),
	}
	fmsg := "  %v -> %v [color=%v];\n"
	if nd.left != nil {
		line := fmt.Sprintf(
			fmsg, key, string(nd.left.key(mdsize)), whatcolor(nd.left),
		)
		lines = append(lines, line)
	}
	if nd.right != nil {
		line := fmt.Sprintf(
			fmsg, key, string(nd.right.key(mdsize)), whatcolor(nd.right),
		)
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

func (nd *Llrbnode) ltkey(mdsize int, other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(mdsize)
	cmp := bytes.Compare(key, other)
	return cmp == -1
}

func (nd *Llrbnode) lekey(mdsize int, other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(mdsize)
	cmp := bytes.Compare(key, other)
	return cmp == -1 || cmp == 0
}

func (nd *Llrbnode) gtkey(mdsize int, other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(mdsize)
	cmp := bytes.Compare(key, other)
	return cmp == 1
}

func (nd *Llrbnode) gekey(mdsize int, other []byte) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Len = nd.keysize()
	sl.Cap = sl.Len
	baseptr := (uintptr)(unsafe.Pointer(&nd.mdmarker))
	sl.Data = baseptr + uintptr(mdsize)
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

func llndornil(nd *Llrbnode) api.Node {
	if nd == nil {
		return nil
	}
	return nd
}
