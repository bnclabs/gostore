package bubt

import "fmt"
import "bytes"
import "encoding/binary"

type msnap []byte

func (m msnap) findkey(
	adjust int, index blkindex, key []byte) (level byte, fpos int64) {

	//fmt.Printf("mfindkey %v %v %q\n", adjust, len(index), key)

	switch len(index) {
	case 0:
		panic(fmt.Errorf("impossible situation"))

	case 1:
		cmp, vpos := m.compareat(adjust, key)
		if cmp >= 0 { // key >= adjust
			level, fpos = byte(vpos>>56), int64(vpos&0x00FFFFFFFFFFFFFF)
			//fmt.Printf("mfindkey %x %x\n", level, fpos)
			return
		}
		//fmt.Println("mfindkey", 0)
		return 0, 0

	default:
		half := len(index) / 2
		cmp, vpos := m.compareat(adjust+half, key)
		if cmp == 0 { // key == adjust+half
			//fmt.Println("mfindkey", "default")
			return byte(vpos >> 56), int64(vpos & 0x00FFFFFFFFFFFFFF)

		} else if cmp > 0 { // key > adjust+half
			return m.findkey(adjust+half, index[half:], key)

		} else if len(index) == 2 || len(index) == 3 {
			_, vpos := m.compareat(adjust, key)
			return byte(vpos >> 56), int64(vpos & 0x00FFFFFFFFFFFFFF)
		}
		return m.findkey(adjust, index[:half], key)
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
	//fmt.Printf("m.compareat %v %s %s %v\n", i, key, m[x:x+ln], cmp)
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
