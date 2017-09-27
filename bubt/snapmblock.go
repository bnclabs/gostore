package bubt

import "fmt"
import "encoding/binary"

import "github.com/prataprc/gostore/api"

type msnap []byte

func (m msnap) getkey(
	adjust int, index hindex, key []byte) (level byte, fpos int) {

	switch len(index) {
	case 1:
		_, vpos := m.compareat(adjust+0, key)
		return vpos >> 56, vpos & 0x0FFFFFFFFFFFFFFF

	case 2:
		cmp, vpos := m.compareat(adjust+1, key)
		if cmp < 0 {
			_, vpos = m.compareat(adjust+0, key)
			return vpos >> 56, vpos & 0x0FFFFFFFFFFFFFFF
		}
		return vpos >> 56, vpos & 0x0FFFFFFFFFFFFFFF

	default:
		half := len(index) / 2
		cmp, vpos := m.compareat(adjust+half, key)
		if cmp < 0 {
			return m.getkey(adjust, index[:half], key)
		}
		return m.getkey(adjust+half, index[half:], key)
	}
	panic("unreachable code")
}

func (m msnap) compareat(i int, key []byte) (int, int) {
	offset := 4 + (i * 4)
	x := binary.BigEndian.Uint32(m[offset : offset+4])
	me := mentry(m[x : x+mentrysize])
	ln, vpos := me.keylen(), me.vpos()
	x += mentrysize
	return bytes.Compare(key, m[x:x+ln]), vpos

}

func (m msnap) getindex(index []uint32) []uint32 {
	nums, n := binary.BigEndian.Uint32(m[:4]), 4
	for i := 0; i < nums; i++ {
		index = append(index, binary.BigEndian.Uint32(m[n:n+4]))
		n += 4
	}
	return index
}
