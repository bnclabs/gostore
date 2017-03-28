package dict

import "github.com/prataprc/gostore/api"

type dictnode struct {
	key     []byte
	value   []byte
	vbno    uint16
	vbuuid  uint64
	bornsq  uint64
	deadsq  uint64
	deleted bool
}

func newdictnode(key, value []byte) *dictnode {
	return &dictnode{key: key, value: value}
}

// Vbno implement NodeGetter{} interface.
func (dn *dictnode) Vbno() uint16 {
	return dn.vbno
}

// Access implement NodeGetter{} interface.
func (dn *dictnode) Access() uint64 {
	panic("not implemented")
}

// Vbuuid implement NodeGetter{} interface.
func (dn *dictnode) Vbuuid() uint64 {
	return dn.vbuuid
}

// Fpos implement NodeGetter{} interface.
func (dn *dictnode) Fpos() (level byte, offset uint64) {
	panic("not implemented")
}

// Bornseqno implement NodeGetter{} interface.
func (dn *dictnode) Bornseqno() uint64 {
	return dn.bornsq
}

// Deadseqno implement NodeGetter{} interface.
func (dn *dictnode) Deadseqno() uint64 {
	return dn.deadsq
}

func (dn *dictnode) IsDeleted() bool {
	return dn.deleted
}

// Key implement NodeGetter{} interface.
func (dn *dictnode) Key() []byte {
	return dn.key
}

// Value implement NodeGetter{} interface.
func (dn *dictnode) Value() []byte {
	return dn.value
}

// SetVbno implement NodeSetter{} interface.
func (dn *dictnode) Setvbno(vbno uint16) api.Node {
	if dn != nil {
		dn.vbno = vbno
	}
	return dn
}

// Setaccess implement NodeSetter{} interface.
func (dn *dictnode) Setaccess(access uint64) api.Node {
	return dn
}

// SetVbuuid implement NodeSetter{} interface.
func (dn *dictnode) SetVbuuid(vbuuid uint64) api.Node {
	if dn != nil {
		dn.vbuuid = vbuuid
	}
	return dn
}

// SetFpos implement NodeSetter{} interface.
func (dn *dictnode) SetFpos(level byte, offset uint64) api.Node {
	panic("not implemented")
}

// SetBornseqno implement NodeSetter{} interface.
func (dn *dictnode) SetBornseqno(seqno uint64) api.Node {
	if dn != nil {
		dn.bornsq = seqno
	}
	return dn
}

// SetDeadseqno implement NodeSetter{} interface.
func (dn *dictnode) SetDeadseqno(seqno uint64) api.Node {
	if dn != nil {
		dn.deadsq, dn.deleted = seqno, true
	}
	return dn
}

func (dn *dictnode) clone() *dictnode {
	newdn := *dn
	newdn.key = make([]byte, len(dn.key))
	copy(newdn.key, dn.key)
	newdn.value = make([]byte, len(dn.value))
	copy(newdn.value, dn.value)
	return &newdn
}
