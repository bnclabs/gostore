package bubt

import "bytes"
import "encoding/binary"

type msnap []byte

func (m msnap) findkey(
	adjust int, index blkindex, key []byte) (level byte, fpos int64) {

	switch len(index) {
	case 1:
		_, vpos := m.compareat(adjust+0, key)
		return byte(vpos >> 56), vpos & 0x0FFFFFFFFFFFFFFF

	case 2:
		cmp, vpos := m.compareat(adjust+1, key)
		if cmp < 0 {
			_, vpos = m.compareat(adjust+0, key)
			return byte(vpos >> 56), vpos & 0x0FFFFFFFFFFFFFFF
		}
		return byte(vpos >> 56), vpos & 0x0FFFFFFFFFFFFFFF

	default:
		half := len(index) / 2
		cmp, vpos := m.compareat(adjust+half, key)
		if cmp == 0 {
			return byte(vpos >> 56), vpos & 0x0FFFFFFFFFFFFFFF
		} else if cmp < 0 {
			return m.findkey(adjust, index[:half], key)
		}
		return m.findkey(adjust+half, index[half:], key)
	}
	panic("unreachable code")
}

func (m msnap) compareat(i int, key []byte) (int, int64) {
	offset := 4 + (i * 4)
	x := binary.BigEndian.Uint32(m[offset : offset+4])
	me := mentry(m[x : x+mentrysize])
	ln, vpos := uint32(me.keylen()), me.vpos()
	x += mentrysize
	return bytes.Compare(key, m[x:x+ln]), int64(vpos)

}

func (m msnap) getindex(index blkindex) blkindex {
	nums, n := binary.BigEndian.Uint32(m[:4]), 4
	for i := uint32(0); i < nums; i++ {
		index = append(index, binary.BigEndian.Uint32(m[n:n+4]))
		n += 4
	}
	return index
}
