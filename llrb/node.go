package llrb

import "io"
import "fmt"
import "unsafe"
import "reflect"
import "strings"
import "sync/atomic"

import "github.com/prataprc/gostore/api"

const nodesize1 = int(unsafe.Sizeof(Llrbnode1{})) - 8 // + metadatasize + keylen

const (
	ndBlack   uint64 = 0x1
	ndDirty   uint64 = 0x2
	ndDeleted uint64 = 0x4
)

// Llrbnode1 defines a node in LLRB tree.
type Llrbnode1 struct {
	left     *Llrbnode1
	right    *Llrbnode1
	seqflags uint64 // seqno[64:4] flags[4:0]
	hdr      uint64 // klen[64:48] access[48:8] reserved[8:0]
	value    unsafe.Pointer
	key      unsafe.Pointer
}

func (nd *Llrbnode1) setnodevalue(nv *nodevalue) *Llrbnode1 {
	nd.value = unsafe.Pointer(nv)
	return nd
}

func (nd *Llrbnode1) nodevalue() *nodevalue {
	if nd == nil || nd.value == nil {
		return nil
	}
	return (*nodevalue)(nd.value)
}

//---- header fields

func (nd *Llrbnode1) gethdr() uint64 {
	return atomic.LoadUint64(&nd.hdr)
}

func (nd *Llrbnode1) sethdr(hdr uint64) *Llrbnode1 {
	atomic.StoreUint64(&nd.hdr, hdr)
	return nd
}

func (nd *Llrbnode1) getkeylen() uint16 {
	return uint16(nd.gethdr() >> 48)
}

func (nd *Llrbnode1) setkeylen(klen uint16) *Llrbnode1 {
	hdr := nd.gethdr()
	nd.sethdr((hdr & 0x0000ffffffffffff) | (uint64(klen) << 48))
	return nd
}

func (nd *Llrbnode1) getaccess() uint64 {
	return (nd.gethdr() & 0x0000ffffffffffff) >> 8
}

func (nd *Llrbnode1) setaccess(access uint64) *Llrbnode1 {
	hdr := nd.gethdr()
	nd.sethdr((hdr & 0xffff0000000000ff) | (access << 8))
	return nd
}

func (nd *Llrbnode1) getkey() (key []byte) {
	klen := nd.getkeylen()
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	return
}

func (nd *Llrbnode1) setkey(key []byte) *Llrbnode1 {
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

func (nd *Llrbnode1) getseqflags() uint64 {
	return atomic.LoadUint64(&nd.seqflags)
}

func (nd *Llrbnode1) setseqflags(seqflags uint64) *Llrbnode1 {
	atomic.StoreUint64(&nd.seqflags, seqflags)
	return nd
}

func (nd *Llrbnode1) setseqno(seqno uint64) *Llrbnode1 {
	nd.setseqflags((nd.getseqflags() & 0xf) | (seqno << 4))
	return nd
}

func (nd *Llrbnode1) getseqno() uint64 {
	return nd.getseqflags() >> 4
}

func (nd *Llrbnode1) isblack() bool {
	if nd == nil {
		return true
	}
	seqflags := nd.getseqflags()
	return (seqflags & ndBlack) == uint64(ndBlack)
}

func (nd *Llrbnode1) isred() bool {
	if nd == nil {
		return false
	}
	return !nd.isblack()
}

func (nd *Llrbnode1) setblack() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags | ndBlack)
}

func (nd *Llrbnode1) setred() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags & (^ndBlack))
}

func (nd *Llrbnode1) togglelink() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags ^ ndBlack)
}

func (nd *Llrbnode1) setdirty() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags | ndDirty)
}

func (nd *Llrbnode1) cleardirty() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags & (^ndDirty))
}

func (nd *Llrbnode1) isdirty() bool {
	seqflags := nd.getseqflags()
	return (seqflags & ndDirty) == ndDirty
}

func (nd *Llrbnode1) isdeleted() bool {
	seqflags := nd.getseqflags()
	return (seqflags & ndDeleted) == ndDeleted
}

func (nd *Llrbnode1) setdeleted() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags | ndDeleted)
}

func (nd *Llrbnode1) cleardeleted() *Llrbnode1 {
	seqflags := nd.getseqflags()
	return nd.setseqflags(seqflags & (^ndDeleted))
}

// Value implement NodeGetter method.
func (nd *Llrbnode1) Value() []byte {
	if nv := nd.nodevalue(); nv != nil {
		return nv.value()
	}
	return nil
}

//---- maintanence methods.

func (nd *Llrbnode1) repr() string {
	return fmt.Sprintf("%q %v %v", nd.getkey(), nd.isdirty(), nd.isblack())
}

func (nd *Llrbnode1) pprint(prefix string) {
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

func (nd *Llrbnode1) dotdump(buffer io.Writer) {
	if nd == nil {
		return
	}

	whatcolor := func(childnd *Llrbnode1) string {
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
	if nd.left != nil {
		nd.left.dotdump(buffer)
	}
	if nd.right != nil {
		nd.right.dotdump(buffer)
	}
}

//---- indexer api

func (nd *Llrbnode1) ltkey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	return api.Binarycmp(key, other, partial) == -1
}

func (nd *Llrbnode1) lekey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	cmp := api.Binarycmp(key, other, partial)
	return cmp == -1 || cmp == 0
}

func (nd *Llrbnode1) gtkey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	cmp := api.Binarycmp(key, other, partial)
	return cmp == 1
}

func (nd *Llrbnode1) gekey(other []byte, partial bool) bool {
	var key []byte
	sl := (*reflect.SliceHeader)(unsafe.Pointer(&key))
	klen := nd.getkeylen()
	sl.Data = (uintptr)(unsafe.Pointer(&nd.key))
	sl.Len, sl.Cap = int(klen), int(klen)
	cmp := api.Binarycmp(key, other, partial)
	return cmp == 0 || cmp == 1
}
