package storage

type dictnode struct {
	key    []byte
	value  []byte
	vbno   uint16
	vbuuid uint64
	bornsq uint64
	deadsq uint64
}

func newdictnode(key, value []byte) *dictnode {
	return &dictnode{key: key, value: value}
}

// Vbno implement NodeGetter{} interface.
func (dn *dictnode) Vbno() uint16 {
	return dn.vbno
}

// Vbuuid implement NodeGetter{} interface.
func (dn *dictnode) Vbuuid() uint64 {
	return dn.vbuuid
}

// Bornseqno implement NodeGetter{} interface.
func (dn *dictnode) Bornseqno() uint64 {
	return dn.bornsq
}

// Deadseqno implement NodeGetter{} interface.
func (dn *dictnode) Deadseqno() uint64 {
	return dn.deadsq
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
func (dn *dictnode) Setvbno(vbno uint16) Node {
	dn.vbno = vbno
	return dn
}

// SetVbuuid implement NodeSetter{} interface.
func (dn *dictnode) SetVbuuid(vbuuid uint64) Node {
	dn.vbuuid = vbuuid
	return dn
}

// SetBornseqno implement NodeSetter{} interface.
func (dn *dictnode) SetBornseqno(seqno uint64) Node {
	dn.bornsq = seqno
	return dn
}

// SetDeadseqno implement NodeSetter{} interface.
func (dn *dictnode) SetDeadseqno(seqno uint64) Node {
	dn.deadsq = seqno
	return dn
}
