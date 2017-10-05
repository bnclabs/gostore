package bubt

import "fmt"
import "bytes"
import "encoding/binary"

var _ = fmt.Sprintf("dummy")

type msnap []byte

func (m msnap) findkey(
	adjust int, index blkindex, key []byte) (level byte, fpos int64) {

	switch len(index) {
	case 0:
		panic("impossible situation")

	case 1:
		cmp, vpos := m.compareat(adjust+0, key)
		if cmp >= 0 {
			//fmt.Printf("mfindkey %x %x\n", 0, vpos)
			return byte(vpos >> 56), int64(vpos & 0x00FFFFFFFFFFFFFF)
		}
		//fmt.Println("mfindkey", -1)
		return 0, -1

	default:
		half := len(index) / 2
		cmp, vpos := m.compareat(adjust+half, key)
		if cmp == 0 {
			//fmt.Println("mfindkey", "default")
			return byte(vpos >> 56), int64(vpos & 0x00FFFFFFFFFFFFFF)
		} else if cmp < 0 {
			return m.findkey(adjust, index[:half], key)
		}
		return m.findkey(adjust+half, index[half:], key)
	}
	panic("unreachable code")
}

func (m msnap) compareat(i int, key []byte) (int, uint64) {
	offset := 4 + (i * 4)
	x := binary.BigEndian.Uint32(m[offset : offset+4])
	me := mentry(m[x : x+mentrysize])
	ln, vpos := uint32(me.keylen()), me.vpos()
	x += mentrysize
	cmp := bytes.Compare(key, m[x:x+ln])
	//fmt.Printf("mcompareat %v %v %s %s\n", cmp, i, key, m[x:x+ln])
	return cmp, vpos
}

func (m msnap) getindex(index blkindex) blkindex {
	nums, n := binary.BigEndian.Uint32(m[:4]), 4
	for i := uint32(0); i < nums; i++ {
		index = append(index, binary.BigEndian.Uint32(m[n:n+4]))
		n += 4
	}
	return index
}
