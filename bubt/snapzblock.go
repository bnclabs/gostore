package bubt

import "fmt"
import "bytes"
import "encoding/binary"

//---- znode for reading entries.

type zsnap []byte

func (z zsnap) findkey(
	adjust int, index blkindex,
	key []byte) (idx int, value []byte, seqno uint64, deleted, ok bool) {

	var cmp int
	switch len(index) {
	case 0:
		panic(fmt.Errorf("impossible situation"))

	case 1:
		cmp, value, seqno, deleted = z.compareat(adjust, key)
		if cmp >= 0 {
			//fmt.Println("zfindkey", adjust, 0)
			return adjust, value, seqno, deleted, true
		}
		// cmp < 0
		//fmt.Println("zfindkey", adjust, -1)
		return adjust + 1, nil, 0, false, false

	default:
		half := len(index) / 2
		cmp, value, seqno, deleted = z.compareat(adjust+half, key)
		if cmp == 0 {
			//fmt.Println("zfindkey", adjust+half, 0)
			return adjust + half, value, seqno, deleted, true
		} else if cmp < 0 {
			return z.findkey(adjust+half, index[half:], key)
		}
		return z.findkey(adjust, index[:half], key)
	}
	panic("unreachable code")
}

func (z zsnap) compareat(i int, key []byte) (int, []byte, uint64, bool) {
	offset := 4 + (i * 4)
	x := int(binary.BigEndian.Uint32(z[offset : offset+4]))
	ze := zentry(z[x : x+zentrysize])
	ln := int(ze.keylen())
	x += zentrysize
	cmp := bytes.Compare(z[x:x+ln], key)
	//fmt.Printf("z.compareat %v %s %s %v\n", i, key, z[x:x+ln], cmp)
	if cmp >= 0 {
		x, ln = x+ln, int(ze.valuelen())
		return cmp, z[x : x+ln], ze.seqno(), ze.isdeleted()
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
	//fmt.Printf("z-entryat %v %v %v\n", index, x, keylen)
	key = z[x : x+keylen]
	x += keylen
	value = z[x : x+valuelen]
	return
}

func (z zsnap) getnext(
	index int) (key, value []byte, seqno uint64, deleted bool) {

	if index >= 0 && z.isbounded(index+1) {
		return z.entryat(index + 1)
	}
	return nil, nil, 0, false
}

func (z zsnap) isbounded(index int) bool {
	idxlen := int(binary.BigEndian.Uint32(z[:4]))
	return (index >= 0) && (index < idxlen)
}
