package llrb

import "io"
import "fmt"
import "unsafe"
import "reflect"
import "strings"
import "sync/atomic"

import "github.com/prataprc/gostore/api"

const nodesize = int(unsafe.Sizeof(Llrbnode{})) - 8 // + keylen

const (
	ndBlack      uint64 = 0x1
	ndDirty      uint64 = 0x2
	ndDeleted    uint64 = 0x4
	ndValreclaim uint64 = 0x8
)

// Llrbnode defines a node in LLRB tree.
type Llrbnode struct {
	left      *Llrbnode
	right     *Llrbnode
	bornseq   uint64
	deadflags uint64 // deadseqno[64:4] flags[4:0]
	hdr       uint64 // klen[64:48] access[48:8] reserved[8:0]
	value     unsafe.Pointer
	key       unsafe.Pointer
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

func (nd *Llrbnode) getdeadflags() uint64 {
	return atomic.LoadUint64(&nd.deadflags)
}

func (nd *Llrbnode) setdeadflags(deadflags uint64) *Llrbnode {
	atomic.StoreUint64(&nd.deadflags, deadflags)
	return nd
}

func (nd *Llrbnode) setdeadseqno(seqno uint64) *Llrbnode {
	deadflags := nd.getdeadflags()
	deadflags = (deadflags & 0xf) | (seqno << 4)
	deadflags = deadflags | ndDeleted
	return nd.setdeadflags(deadflags)
}

func (nd *Llrbnode) resetdeadseqno() *Llrbnode {
	deadflags := (nd.getdeadflags() & 0xf) & (^ndDeleted)
	return nd.setdeadflags(deadflags)
}

func (nd *Llrbnode) setbornseqno(seqno uint64) *Llrbnode {
	nd.bornseq = seqno
	nd.resetdeadseqno()
	return nd
}

func (nd *Llrbnode) getseqno() uint64 {
	if nd.isdeleted() {
		return nd.getdeadflags() >> 4
	}
	return nd.bornseq
}

func (nd *Llrbnode) isblack() bool {
	if nd == nil {
		return true
	}
	deadflags := nd.getdeadflags()
	return (deadflags & ndBlack) == uint64(ndBlack)
}

func (nd *Llrbnode) isred() bool {
	if nd == nil {
		return false
	}
	return !nd.isblack()
}

func (nd *Llrbnode) setblack() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags | ndBlack)
}

func (nd *Llrbnode) setred() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags & (^ndBlack))
}

func (nd *Llrbnode) togglelink() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags ^ ndBlack)
}

func (nd *Llrbnode) setdirty() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags | ndDirty)
}

func (nd *Llrbnode) cleardirty() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags & (^ndDirty))
}

func (nd *Llrbnode) isdirty() bool {
	deadflags := nd.getdeadflags()
	return (deadflags & ndDirty) == ndDirty
}

func (nd *Llrbnode) isdeleted() bool {
	deadflags := nd.getdeadflags()
	return (deadflags & ndDeleted) == ndDeleted
}

func (nd *Llrbnode) setdeleted() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags | ndDeleted)
}

func (nd *Llrbnode) cleardeleted() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags & (^ndDeleted))
}

func (nd *Llrbnode) setreclaim() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags | ndValreclaim)
}

func (nd *Llrbnode) clearreclaim() *Llrbnode {
	deadflags := nd.getdeadflags()
	return nd.setdeadflags(deadflags & (^ndValreclaim))
}

func (nd *Llrbnode) isreclaim() bool {
	deadflags := nd.getdeadflags()
	return (deadflags & ndValreclaim) == ndValreclaim
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
