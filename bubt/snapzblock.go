package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/gostore/api"

//---- znode for reading entries.

type zsnap []byte

func (z zsnap) getkey(
	adjust int, index hindex,
	key []byte) (idx int, value []byte, seqno uint64, deleted bool) {

	var cmp int
	switch len(index) {
	case 0:
		return adjust + 1, nil, 0, false

	case 1:
		cmp, value, seqno, deleted = z.compareat(adjust+0, key)
		if cmp == 0 {
			return adjust, value, seqno, deleted
		} else if cmp < 0 {
			return adjust, value, seqno, deleted
		} else {
			return adjust + 1, value, seqno, deleted
		}

	default:
		half := len(index) / 2
		cmp, value, seqno, deleted = z.compareat(adjust+half, key)
		if cmp == 0 {
			return adjust + half, value, seqno, deleted
		} else if cmp < 0 {
			return z.getkey(adjust+half, index[:half], key)
		} else {
			return z.getkey(adjust+half, index[half:], key)
		}
	}
	panic("unreachable code")
}

func (z zsnap) compareat(
	offset int, key []byte) (int, []byte, uint64, bool) {

	offset := 4 + (i * 4)
	x := binary.BigEndian.Uint32(z[offset : offset+4])
	ze := zentry(z[x : x+zentrysize])
	ln := keylen()
	x += zentrysize
	cmp = bytes.Compare(key, ze[x:x+ln])
	if cmp == 0 {
		x, ln = x+ln, valuelen()
		return 0, key, ze[x:ln], ze.seqno(), ze.isdeleted()
	}
	return cmp, nil, nil, 0, false
}

func (z zsnap) getindex(index []uint32) []uint32 {
	nums, n := binary.BigEndian.Uint32(z[:4]), 4
	for i := 0; i < nums; i++ {
		index = append(index, binary.BigEndian.Uint32(z[n:n+4]))
		n += 4
	}
	return index
}
