package bubt

import "encoding/binary"

// mentry represents the binary layout of each entry in the intermediate-block.
// klen  uint64
// vlen  uint64
type mentry []byte // key shall follow.

const mentrysize = 16

func (me mentry) setkeylen(keylen uint64) mentry {
	binary.BigEndian.PutUint64(me[0:8], keylen)
	return me
}

func (me mentry) keylen() uint64 {
	return binary.BigEndian.Uint64(me[0:8])
}

func (me mentry) setvpos(keylen uint64) mentry {
	binary.BigEndian.PutUint64(me[8:16], keylen)
	return me
}

func (me mentry) vpos() uint64 {
	return binary.BigEndian.Uint64(me[8:16])
}
