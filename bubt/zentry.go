package bubt

import "encoding/binary"

const (
	zflagDeleted byte = 0x10
)

// zentry represents the binary layout of each entry in the leaf(z) block.
// hdr1: flags[64:60] seqno[60:0]
// hdr2: 8 bytes // key-len
// hdr3: 8 bytes // value-len
type zentry []byte // key, and optionally value shall follow.

const zentrysize = 24

func (ze zentry) setdeleted() zentry {
	ze[7] = ze[7] | zflagDeleted
	return ze
}

func (ze zentry) cleardeleted() zentry {
	ze[7] = ze[7] & (^zflagDeleted)
	return ze
}

func (ze zentry) isdeleted() bool {
	return (ze[7] & zflagDeleted) != 0
}

func (ze zentry) setseqno(seqno uint64) zentry {
	hdr1 := binary.BigEndian.Uint64(ze[:8])
	hdr1 = (hdr1 & 0xF000000000000000) | seqno
	binary.BigEndian.PutUint64(ze[:8], hdr1)
	return ze
}

func (ze zentry) seqno() uint64 {
	hdr1 := binary.BigEndian.Uint64(ze[:8])
	return hdr1 & 0x0FFFFFFFFFFFFFFF
}

func (ze zentry) setkeylen(keylen uint64) zentry {
	binary.BigEndian.PutUint64(ze[8:16], keylen)
	return ze
}

func (ze zentry) keylen() uint64 {
	return binary.BigEndian.Uint64(ze[8:16])
}

func (ze zentry) setvaluelen(keylen uint64) zentry {
	binary.BigEndian.PutUint64(ze[16:24], keylen)
	return ze
}

func (ze zentry) valuelen() uint64 {
	return binary.BigEndian.Uint64(ze[16:24])
}
