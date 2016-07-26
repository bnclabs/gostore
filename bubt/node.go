package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/storage.go/api"

// encapsulates the byte-stream of an entry in bubt's z-node.
// 0:2   - vbno
// 2:10  - vbuuid
// 10:18 - bornseqno
// 18:26 - deadseqno
// 26:28 - length of the key (n)
// 28:28+n - key
type node struct {
	ss     *Snapshot
	offset int64
	data   []byte // byte stream
	value  []byte // cache value.
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
	return 0
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
	if len(value) > 0 {
		return value
	}

	klen := binary.BigEndian.Uint16(n.data[26:28])
	start := 28 + klen
	if n.ss.hasdatafile() == false {
		vlen := binary.BigEndian.Uint16(n.data[start : start+2])
		return n.data[start+2 : start+2+vlen]
	}

	var vbuf [2]byte
	vpos := int64(binary.BigEndian.Uint64(n.data[start : start+8]))
	if ln, err := n.ss.datafd.ReadAt(vbuf[:], vpos); err != nil {
		panic(fmt.Errorf("bubt node reading value len: %v", err))
	} else if ln != len(vbuf) {
		panic(fmt.Errorf("bubt node read %v(%v) bytes", ln, len(vbuf)))
	}
	vlen := int64(binary.BigEndian.Uint16(vbuf[:]))
	if int64(cap(n.value)) < vlen {
		n.value = make([]byte, 0, vlen)
	}
	n.value = n.value[:vlen]
	if ln, err := n.ss.datafd.ReadAt(n.value, vpos+2); err != nil {
		panic(fmt.Errorf("bubt node reading value: %v", err))
	} else if ln != len(n.value) {
		panic(fmt.Errorf("bubt node read %v(%v) bytes", ln, len(n.value)))
	}
	return n.value
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
	panic("Setvbno(): not implemented for bubt node")
}

// Setaccess implement NodeSetter{} interface.
func (n *node) Setaccess(access uint64) api.Node {
	panic("Setaccess(): not implemented for bubt node")
}

// SetVbuuid implement NodeSetter{} interface.
func (n *node) SetVbuuid(uuid uint64) api.Node {
	panic("SetVbuuid(): not implemented for bubt node")
}

// SetFpos implement NodeSetter{} interface.
func (n *node) SetFpos(level byte, offset uint64) api.Node {
	panic("SetFpos(): not implemented for bubt node")
}

// SetBornseqno implement NodeSetter{} interface.
func (n *node) SetBornseqno(seqno uint64) api.Node {
	panic("SetBornseqno(): not implemented for bubt node")
}

// SetDeadseqno implement NodeSetter{} interface.
func (n *node) SetDeadseqno(seqno uint64) api.Node {
	panic("SetDeadseqno(): not implemented for bubt node")
}
