package llrb

import "io"
import "fmt"
import "unsafe"
import "reflect"
import "strings"
import "sync/atomic"

import "github.com/prataprc/gostore/api"

const nodesize = int(unsafe.Sizeof(Llrbnode{})) - 8 // + metadatasize + keylen

const (
	ndBlack   uint64 = 0x1
	ndDirty   uint64 = 0x2
	ndDeleted uint64 = 0x4
)

// Llrbnode defines a node in LLRB tree.
type Llrbnode struct {
	left     *Llrbnode
	right    *Llrbnode
	seqflags uint64 // seqno[64:4] flags[4:0]
	hdr      uint64 // klen[64:48] access[48:8] reserved[8:0]
	value    unsafe.Pointer
	key      unsafe.Pointer
}

func (nd *Llrbnode) setnodevalue(nv *nodevalue) *Llrbnode {
	nd.value = unsafe.Pointer(nv)
	return nd
}

func (nd *Llrbnode) nodevalue() *nodevalue {
	if nd == nil || nd.value == nil {
		return nil
	}
	return (*nodevalue)(nd.value)
}

//---- header fields

func (nd *Llrbnode) gethdr() uint64 {
	return atomic.LoadUint64(&nd.hdr)
}

func (nd *Llrbnode) sethdr(hdr uint64) *Llrbnode {
	atomic.StoreUint64(&nd.hdr, hdr)
	return nd
}

func (nd *Llrbnode) getkeylen() uint16 {
	return uint16(nd.gethdr() >> 48)
}

func (nd *Llrbnode) setkeylen(klen uint16) *Llrbnode {
	hdr := nd.gethdr()
	nd.sethdr((hdr & 0x0000ffffffffffff) | (uint64(klen) << 48))
	return nd
}

func (nd *Llrbnode) getaccess() uint64 {
	return (nd.gethdr() & 0x0000ffffffffffff) >> 8
}

func (nd *Llrbnode) setaccess(access uint64) *Llrbnode {
	hdr := nd.gethdr()
	nd.sethdr((hdr & 0xffff0000000000ff) | (access << 8))
	return nd
}

func (nd *Llrbnode) getkey() (key []byte) {
	klen := nd.getkeylen()
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	return
}

func (nd *Llrbnode) setkey(key []byte) *Llrbnode {
	var dst []byte
	klen := len(key)
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	copy(dst, key)
	nd.setkeylen(uint16(len(key)))
	return nd
}

//----- seqno and flags

func (nd *Llrbnode) getseqflags() uint64 {
	return atomic.LoadUint64(&nd.seqflags)
}

func (nd *Llrbnode) setseqflags(seqflags uint64) *Llrbnode {
	atomic.StoreUint64(&nd.seqflags, seqflags)
	return nd
}

func (nd *Llrbnode) setseqno(seqno uint64) *Llrbnode {
	nd.setseqflags((nd.getseqflags() & 0xf) | (seqno << 4))
	return nd
}

func (nd *Llrbnode) getseqno() uint64 {
	return nd.getseqflags() >> 4
}

func (nd *Llrbnode) isblack() bool {
	if nd == nil {
		return true
	}
	seqflags := nd.getseqflags()
	return (seqflags & ndBlack) == uint64(ndBlack)
}

func (nd *Llrbnode) isred() bool {
	if nd == nil {
		return false
	}
	return !nd.isblack()
}

func (nd *Llrbnode) setblack() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags | ndBlack)
}

func (nd *Llrbnode) setred() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags & (^ndBlack))
}

func (nd *Llrbnode) togglelink() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags ^ ndBlack)
}

func (nd *Llrbnode) setdirty() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags | ndDirty)
}

func (nd *Llrbnode) cleardirty() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags & (^ndDirty))
}

func (nd *Llrbnode) isdirty() bool {
	seqflags := nd.getseqflags()
	return (seqflags & ndDirty) == ndDirty
}

func (nd *Llrbnode) isdeleted() bool {
	seqflags := nd.getseqflags()
	return (seqflags & ndDeleted) == ndDeleted
}

func (nd *Llrbnode) setdeleted() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags | ndDeleted)
}

func (nd *Llrbnode) cleardeleted() *Llrbnode {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags & (^ndDeleted))
}

// Value return the value byte-slice for this entry.
func (nd *Llrbnode) Value() []byte {
	if nv := nd.nodevalue(); nv != nil {
		return nv.value()
	}
	return nil
}

//---- maintanence methods.

func (nd *Llrbnode) repr() string {
	return fmt.Sprintf("%q %v %v", nd.getkey(), nd.isdirty(), nd.isblack())
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
		if childnd.isred() {
			return "red"
		}
		return "black"
	}

	key := nd.getkey()
	lines := []string{
		fmt.Sprintf("  %s [label=\"{%s}\"];\n", key, key),
	}
	fmsg := "  %s -> %s [color=%v];\n"
	if nd.left != nil {
		line := fmt.Sprintf(fmsg, key, nd.left.getkey(), whatcolor(nd.left))
		lines = append(lines, line)
	}
	if nd.right != nil {
		line := fmt.Sprintf(fmsg, key, nd.right.getkey(), whatcolor(nd.right))
		lines = append(lines, line)
	}
	buffer.Write([]byte(strings.Join(lines, "")))
	nd.left.dotdump(buffer)
	nd.right.dotdump(buffer)
}

func (nd *Llrbnode) ptrdump(prefix string) {
	if nd == nil {
		return
	}
	fmt.Printf("%s%d: %s\n", prefix, (uintptr)(unsafe.Pointer(nd)), nd.getkey())
	nd.left.ptrdump(prefix + "  ")
	nd.right.ptrdump(prefix + "  ")
}

//---- indexer api

func (nd *Llrbnode) ltkey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	return api.Binarycmp(key, other, partial) == -1
}

func (nd *Llrbnode) lekey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	cmp := api.Binarycmp(key, other, partial)
	return cmp == -1 || cmp == 0
}

func (nd *Llrbnode) gtkey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	cmp := api.Binarycmp(key, other, partial)
	return cmp == 1
}

func (nd *Llrbnode) gekey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	cmp := api.Binarycmp(key, other, partial)
	return cmp == 0 || cmp == 1
}
