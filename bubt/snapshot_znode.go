package bubt

import "encoding/binary"
import "bytes"

import "github.com/prataprc/storage.go/api"

//---- znode for reading entries.

type znode []byte

func (z znode) getentry(n uint32, entries []byte) zentry {
	off := n * 4
	koff := binary.BigEndian.Uint32(entries[off : off+4])
	return zentry(entries[koff:len(entries)])
}

func (z znode) searchkey(key []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice")
	}

	switch count := len(entries) / 4; count {
	case 1:
		return 0

	default:
		mid := int32(count / 2)
		if bytes.Compare(key, z.getentry(uint32(mid), entries).key()) >= cmp {
			return mid + z.searchkey(key, entries[mid*4:], cmp)
		}
		return z.searchkey(key, entries[:mid*4], cmp)
	}
}

func (z znode) entryslice() []byte {
	count := binary.BigEndian.Uint32(z[:4])
	return z[4 : 4+(count*4)]
}

type zentry []byte

func (z zentry) key() []byte {
	klen := binary.BigEndian.Uint16(z[26 : 26+2])
	return z[26+2 : 26+2+klen]
}

//----- node definition for bubt, implements api.Node

type node struct {
	ss     *Snapshot
	offset int64
	data   []byte
	value  []byte
}

func (ss *Snapshot) newznode(nd *node, data []byte, offset int64) {
	nd.ss = ss
	nd.data = data
	nd.offset = offset
	nd.value = make([]byte, 0)
}

//---- NodeGetter implementation

// Vbno implement NodeGetter{} interface.
func (n *node) Vbno() (vbno uint16) {
	return binary.BigEndian.Uint16(n.data[:2])
}

// Access implement NodeGetter{} interface.
func (n *node) Access() (ts uint64) {
	return 0 // TODO: should we panic ??
}

// Vbuuid implement NodeGetter{} interface.
func (n *node) Vbuuid() (uuid uint64) {
	return binary.BigEndian.Uint64(n.data[2:10])
}

// Bornseqno implement NodeGetter{} interface.
func (n *node) Bornseqno() (seqno uint64) {
	return binary.BigEndian.Uint64(n.data[10:18])
}

// Deadseqno implement NodeGetter{} interface.
func (n *node) Deadseqno() (seqno uint64) {
	return binary.BigEndian.Uint64(n.data[18:26])
}

// Key implement NodeGetter{} interface.
func (n *node) Key() (key []byte) {
	klen := binary.BigEndian.Uint16(n.data[26:28])
	return n.data[28 : 28+klen]
}

// Value implement NodeGetter{} interface.
func (n *node) Value() (value []byte) {
	klen := binary.BigEndian.Uint16(n.data[26:28])
	start := 28 + klen
	if n.ss.hasdatafile() {
		var vbuf [2]byte
		vpos := int64(binary.BigEndian.Uint64(n.data[start : start+8]))
		if ln, err := n.ss.datafd.ReadAt(vbuf[:], vpos); err != nil {
			panic(err)
		} else if ln != len(vbuf) {
			panic("insufficient data")
		}
		vlen := int64(binary.BigEndian.Uint16(vbuf[:]))
		if int64(cap(n.value)) < vlen {
			n.value = make([]byte, 0, vlen)
		}
		n.value = n.value[:vlen]
		if ln, err := n.ss.datafd.ReadAt(n.value, vpos+2); err != nil {
			panic(err)
		} else if ln != len(n.value) {
			panic("insufficient data")
		}
		return n.value
	}
	vlen := binary.BigEndian.Uint16(n.data[start : start+2])
	return n.data[start+2 : start+2+vlen]
}

// Fpos implement NodeGetter{} interface.
func (n *node) Fpos() (level byte, offset int64) {
	klen := binary.BigEndian.Uint16(n.data[26:28])
	start := 28 + klen
	if n.ss.hasdatafile() {
		vpos := binary.BigEndian.Uint64(n.data[start : start+8])
		return n.ss.level, int64(vpos)
	}
	return n.ss.level, n.offset + int64(start)
}

//---- NodeSetter implementation

// Setvbno implement NodeSetter{} interface.
func (n *node) Setvbno(vbno uint16) api.Node {
	panic("not implemented")
}

// Setaccess implement NodeSetter{} interface.
func (n *node) Setaccess(access uint64) api.Node {
	panic("not implemented")
}

// SetVbuuid implement NodeSetter{} interface.
func (n *node) SetVbuuid(uuid uint64) api.Node {
	panic("not implemented")
}

// SetFpos implement NodeSetter{} interface.
func (n *node) SetFpos(level byte, offset uint64) api.Node {
	panic("not implemented")
}

// SetBornseqno implement NodeSetter{} interface.
func (n *node) SetBornseqno(seqno uint64) api.Node {
	panic("not implemented")
}

// SetDeadseqno implement NodeSetter{} interface.
func (n *node) SetDeadseqno(seqno uint64) api.Node {
	panic("not implemented")
}
