package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

type node struct {
	ss    *Snapshot
	ze    zentry
	value []byte // cache value.
}

func (ss *Snapshot) newznode(nd *node, ze zentry) {
	nd.ss, nd.ze = ss, ze
	if nd.value != nil {
		nd.value = nd.value[:0]
	}
}

//---- NodeGetter implementation

// Vbno implement NodeGetter{} interface.
func (nd *node) Vbno() uint16 {
	return nd.ze.getvbno()
}

// Access implement NodeGetter{} interface.
func (nd *node) Access() (ts uint64) {
	return 0
}

// Vbuuid implement NodeGetter{} interface.
func (nd *node) Vbuuid() uint64 {
	if nd.ss.hasvbuuid {
		return nd.ze.getvbuuid()
	}
	return 0 // TODO: should we panic
}

// Bornseqno implement NodeGetter{} interface.
func (nd *node) Bornseqno() uint64 {
	if nd.ss.hasbornseqno {
		return nd.ze.getbornseqno()
	}
	return 0 // TODO: should we panic
}

// Deadseqno implement NodeGetter{} interface.
func (nd *node) Deadseqno() uint64 {
	if nd.ss.hasdeadseqno {
		return nd.ze.getdeadseqno()
	}
	return 0 // TODO: should we panic
}

// IsDeleted implement NodeGetter{} interface.
func (nd *node) IsDeleted() bool {
	return zentryFlags(nd.ze.getheader()).isdeleted()
}

// Key implement NodeGetter{} interface.
func (nd *node) Key() []byte {
	start, end := zentryLen, zentryLen+nd.ze.keylen()
	return nd.ze[start:end]
}

// Value implement NodeGetter{} interface.
func (nd *node) Value() []byte {
	if len(nd.value) > 0 {
		return nd.value
	}

	vlen := nd.ze.valuenum()

	if nd.ss.hasdatafile == false {
		start := uint64(zentryLen + nd.ze.keylen())
		end := start + vlen
		return nd.ze[start:end]
	}

	var vlenbuf [zeoffVlenEnd - zeoffVlenStart]byte

	vpos := int64(vlen)
	if ln, err := nd.ss.datafd.ReadAt(vlenbuf[:], vpos); err != nil {
		panic(fmt.Errorf("bubt node reading value len: %v", err))
	} else if ln != len(vlenbuf) {
		panic(fmt.Errorf("bubt node read %v(%v) bytes", ln, len(vlenbuf)))
	}
	vlen = uint64(binary.BigEndian.Uint64(vlenbuf[:]))
	nd.value = lib.Fixbuffer(nd.value, int64(vlen))
	vpos += int64(len(vlenbuf))
	if ln, err := nd.ss.datafd.ReadAt(nd.value, vpos); err != nil {
		panic(fmt.Errorf("bubt node reading value: %v", err))
	} else if ln != len(nd.value) {
		panic(fmt.Errorf("bubt node read %v(%v) bytes", ln, len(nd.value)))
	}
	return nd.value
}

// Fpos implement NodeGetter{} interface.
func (nd *node) Fpos() (level byte, offset uint64) {
	if nd.ss.hasdatafile == false {
		return nd.ss.level, uint64(zentryLen + nd.ze.keylen())
	}
	vpos := nd.ze.valuenum() + uint64(zeoffVlenEnd-zeoffVlenStart)
	return nd.ss.level, vpos
}

//---- NodeSetter implementation

// Setvbno implement NodeSetter{} interface.
func (nd *node) Setvbno(vbno uint16) api.Node {
	panic("Setvbno(): not implemented for bubt node")
}

// Setaccess implement NodeSetter{} interface.
func (nd *node) Setaccess(access uint64) api.Node {
	panic("Setaccess(): not implemented for bubt node")
}

// SetVbuuid implement NodeSetter{} interface.
func (nd *node) SetVbuuid(uuid uint64) api.Node {
	panic("SetVbuuid(): not implemented for bubt node")
}

// SetFpos implement NodeSetter{} interface.
func (nd *node) SetFpos(level byte, offset uint64) api.Node {
	panic("SetFpos(): not implemented for bubt node")
}

// SetBornseqno implement NodeSetter{} interface.
func (nd *node) SetBornseqno(seqno uint64) api.Node {
	panic("SetBornseqno(): not implemented for bubt node")
}

// SetDeadseqno implement NodeSetter{} interface.
func (nd *node) SetDeadseqno(seqno uint64) api.Node {
	panic("SetDeadseqno(): not implemented for bubt node")
}
