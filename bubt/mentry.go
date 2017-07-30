package bubt

import "encoding/binary"

const (
	meoffKlenStart = 0
	meoffKlenEnd   = meoffKlenStart + 2
	meoffVlenStart = meoffKlenEnd
	meoffVlenEnd   = meoffVlenStart + 8
	meoffRlenStart = meoffVlenEnd
	meoffRlenEnd   = meoffRlenStart + 8
)

const mnentriesSz = 4
const mentryLen = meoffRlenEnd

// mentry represents the binary layout of each entry in the intermediat-block.
// klen  uint16
// vlen  uint64
// rlen  uint64
type mentry []byte // key shall follow.

//---- get methods

func (me mentry) keylen() uint16 {
	return binary.BigEndian.Uint16(me[meoffKlenStart:meoffKlenEnd])
}

func (me mentry) valuepos() uint64 {
	return binary.BigEndian.Uint64(me[meoffVlenStart:meoffVlenEnd])
}

func (me mentry) reducepos() uint64 {
	return binary.BigEndian.Uint64(me[meoffRlenStart:meoffRlenEnd])
}

func (me mentry) key() []byte {
	klen := me.keylen()
	return me[mentryLen : mentryLen+klen]
}

//---- set methods

func (me mentry) setkeylen(klen uint16) mentry {
	binary.BigEndian.PutUint16(me[meoffKlenStart:meoffKlenEnd], klen)
	return me
}

func (me mentry) setvaluepos(vpos uint64) mentry {
	binary.BigEndian.PutUint64(me[meoffVlenStart:meoffVlenEnd], vpos)
	return me
}

func (me mentry) setreducepos(rpos uint64) mentry {
	binary.BigEndian.PutUint64(me[meoffRlenStart:meoffRlenEnd], rpos)
	return me
}
