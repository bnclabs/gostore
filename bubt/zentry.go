package bubt

import "encoding/binary"

const (
	zeoffHdrStart       = 0
	zeoffHdrEnd         = zeoffHdrStart + 8
	zeoffVbuuidStart    = zeoffHdrEnd
	zeoffVbuuidEnd      = zeoffVbuuidStart + 8
	zeoffBornseqnoStart = zeoffVbuuidEnd
	zeoffBornseqnoEnd   = zeoffBornseqnoStart + 8
	zeoffDeadseqnoStart = zeoffBornseqnoEnd
	zeoffDeadseqnoEnd   = zeoffDeadseqnoStart + 8
	zeoffKlenStart      = zeoffDeadseqnoEnd
	zeoffKlenEnd        = zeoffKlenStart + 2
	zeoffVlenStart      = zeoffKlenEnd
	zeoffVlenEnd        = zeoffVlenStart + 8
)

const zentryLen = zeoffVlenEnd

// zentry represents the binary layout of each entry in the leaf(z) block.
// hdr:
//     flags[32:16] vbno[16:0]
// vbuuid    uint64
// bornseqno uint64
// deadseqno uint64
// klen      uint16
// vlen      uint64 fpos-valuefile, if value is in seperate file
// key
// value, if vlen is not fpos-valuefile.
type zentry []byte

//---- get methods

func (ze zentry) getheader() uint64 {
	return binary.BigEndian.Uint64(ze[zeoffHdrStart:zeoffHdrEnd])
}

func (ze zentry) getflags() zentryFlags {
	hdr := ze.getheader()
	return zentryFlags((hdr & 0xffff0000) >> 16)
}

func (ze zentry) getvbno() uint16 {
	hdr := ze.getheader()
	return uint16(hdr & 0xffff)
}

func (ze zentry) getvbuuid() uint64 {
	return binary.BigEndian.Uint64(ze[zeoffVbuuidStart:zeoffVbuuidEnd])
}

func (ze zentry) getbornseqno() uint64 {
	return binary.BigEndian.Uint64(ze[zeoffBornseqnoStart:zeoffBornseqnoEnd])
}

func (ze zentry) getdeadseqno() uint64 {
	return binary.BigEndian.Uint64(ze[zeoffDeadseqnoStart:zeoffDeadseqnoEnd])
}

func (ze zentry) keylen() uint16 {
	return binary.BigEndian.Uint16(ze[zeoffKlenStart:zeoffKlenEnd])
}

func (ze zentry) valuenum() uint64 {
	return binary.BigEndian.Uint64(ze[zeoffVlenStart:zeoffVlenEnd])
}

//---- set methods

func (ze zentry) setheader(hdr uint64) zentry {
	binary.BigEndian.PutUint64(ze[zeoffHdrStart:zeoffHdrEnd], hdr)
	return ze
}

func (ze zentry) setvbno(vbno uint16) zentry {
	return ze.setheader((ze.getheader() & 0xffffffffffff0000) | uint64(vbno))
}

func (ze zentry) setflags(flags zentryFlags) zentry {
	hdr := (ze.getheader() & 0xffffffff0000ffff) | (uint64(flags) << 16)
	return ze.setheader(hdr)
}

func (ze zentry) setvbuuid(vbuuid uint64) zentry {
	binary.BigEndian.PutUint64(ze[zeoffVbuuidStart:zeoffVbuuidEnd], vbuuid)
	return ze
}

func (ze zentry) setbornseqno(seqno uint64) zentry {
	binary.BigEndian.PutUint64(ze[zeoffBornseqnoStart:zeoffBornseqnoEnd], seqno)
	return ze
}

func (ze zentry) setdeadseqno(seqno uint64) zentry {
	binary.BigEndian.PutUint64(ze[zeoffDeadseqnoStart:zeoffDeadseqnoEnd], seqno)
	return ze
}

func (ze zentry) setkeylen(klen uint16) zentry {
	binary.BigEndian.PutUint16(ze[zeoffKlenStart:zeoffKlenEnd], klen)
	return ze
}

func (ze zentry) setvaluenum(valnum uint64) zentry {
	binary.BigEndian.PutUint64(ze[zeoffVlenStart:zeoffVlenEnd], valnum)
	return ze
}
