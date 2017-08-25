package api

// Keymask 32-bit field mask that can selectively enable or
// disable parameters in key. There can be a maximum of 32 params.
type Keymask uint32

const (
	keyTxn       Keymask = 0x00000001
	keyValue     Keymask = 0x00000002
	keyBornseqno Keymask = 0x00000004
	keyDeadseqno Keymask = 0x00000008
	keyUuid      Keymask = 0x00000010
)

// SetTxn parameter for key.
func (km Keymask) SetTxn() Keymask {
	return km | keyTxn
}

// IsTxn check Txn parameter for key.
func (km Keymask) IsTxn() bool {
	return (km & keyTxn) == keyTxn
}

// SetValue parameter for key.
func (km Keymask) SetValue() Keymask {
	return km | keyValue
}

// IsValue check value parameter for key.
func (km Keymask) IsValue() bool {
	return (km & keyValue) == keyValue
}

// SetBornseqno parameter for key.
func (km Keymask) SetBornseqno() Keymask {
	return km | keyBornseqno
}

// IsBornseqno check bornseqno parameter for key.
func (km Keymask) IsBornseqno() bool {
	return (km & keyBornseqno) == keyBornseqno
}

// SetDeadseqno parameter for key.
func (km Keymask) SetDeadseqno() Keymask {
	return km | keyDeadseqno
}

// IsDeadseqno check deadseqno parameter for key.
func (km Keymask) IsDeadseqno() bool {
	return (km & keyDeadseqno) == keyDeadseqno
}

// SetUuid parameter for key.
func (km Keymask) SetUuid() Keymask {
	return km | keyUuid
}

// IsUuid check uuid parameter for key.
func (km Keymask) IsUuid() bool {
	return (km & keyUuid) == keyUuid
}

var lookupones = [256]byte{
	0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
	4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
}

func paramoffset(mask Keymask) byte {
	offset := lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	mask = mask >> 8
	offset += lookupones[mask&0xff]
	return offset
}
