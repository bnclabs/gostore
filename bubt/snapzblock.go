package bubt

import "bytes"
import "encoding/binary"

//---- znode for reading entries.

type zsnap []byte

func (z zsnap) findkey(
	adjust int, index blkindex,
	key []byte) (idx int, value []byte, seqno uint64, deleted, ok bool) {

	var cmp int
	switch len(index) {
	case 1:
		cmp, value, seqno, deleted = z.compareat(adjust+0, key)
		if cmp == 0 {
			return adjust, value, seqno, deleted, true
		} else if cmp < 0 {
			return adjust, nil, 0, false, false
		}
		return adjust + 1, nil, 0, false, false

	default:
		half := len(index) / 2
		cmp, value, seqno, deleted = z.compareat(adjust+half, key)
		if cmp == 0 {
			return adjust + half, value, seqno, deleted, true
		} else if cmp < 0 {
			return z.findkey(adjust, index[:half], key)
		}
		return z.findkey(adjust+half, index[half:], key)
	}
	panic("unreachable code")
}

func (z zsnap) compareat(i int, key []byte) (int, []byte, uint64, bool) {
	offset := 4 + (i * 4)
	x := int(binary.BigEndian.Uint32(z[offset : offset+4]))
	ze := zentry(z[x : x+zentrysize])
	ln := int(ze.keylen())
	x += zentrysize
	cmp := bytes.Compare(key, z[x:x+ln])
	if cmp == 0 {
		x, ln = x+ln, int(ze.valuelen())
		return 0, z[x : x+ln], ze.seqno(), ze.isdeleted()
	}
	return cmp, nil, 0, false
}

func (z zsnap) getindex(index blkindex) blkindex {
	nums, n := binary.BigEndian.Uint32(z[:4]), 4
	for i := uint32(0); i < nums; i++ {
		index = append(index, binary.BigEndian.Uint32(z[n:n+4]))
		n += 4
	}
	return index
}

func (z zsnap) entryat(
	index int) (key, value []byte, seqno uint64, deleted bool) {

	x := int((index * 4) + 4)
	x = int(binary.BigEndian.Uint32(z[x : x+4]))
	ze := zentry(z[x : x+zentrysize])
	seqno, deleted = ze.seqno(), ze.isdeleted()
	keylen, valuelen := int(ze.keylen()), int(ze.valuelen())
	x += zentrysize
	key = z[x : x+keylen]
	x += keylen
	value = z[x : x+valuelen]
	return
}

func (z zsnap) getnext(
	index int) (key, value []byte, seqno uint64, deleted bool) {

	idxlen := int(binary.BigEndian.Uint32(z[:4]))
	if (index + 1) >= idxlen {
		return nil, nil, 0, false
	}
	return z.entryat(index + 1)
}
