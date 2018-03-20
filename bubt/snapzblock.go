package bubt

import "fmt"
import "bytes"
import "encoding/binary"

//---- znode for reading entries.

type zsnap []byte

func (z zsnap) findkey(
	adjust int, index blkindex,
	key []byte) (
	idx int, actualkey []byte, lv lazyvalue, seqno uint64, del, ok bool) {

	//fmt.Printf("zfindkey %v %v %q\n", adjust, len(index), key)

	var cmp int
	switch len(index) {
	case 0:
		panic(fmt.Errorf("impossible situation"))

	case 1:
		cmp, actualkey, lv, seqno, del = z.compareat(adjust, key)
		if cmp == 0 { // adjust+half >= key
			//fmt.Printf("zfindkey-1 %v %v %q\n", adjust, 0, actualkey)
			return adjust, actualkey, lv, seqno, del, true
		}
		// cmp < 0
		//fmt.Printf("zfindkey-2 %v %v %q\n", adjust, -1, actualkey)
		return adjust + 1, actualkey, lv, 0, false, false

	default:
		half := len(index) / 2
		arg1 := adjust + half
		cmp, actualkey, lv, seqno, del = z.compareat(arg1, key)
		if cmp == 0 {
			//fmt.Println("zfindkey", adjust+half, 0)
			return adjust + half, actualkey, lv, seqno, del, true

		} else if cmp < 0 { // adjust+half < key
			return z.findkey(adjust+half, index[half:], key)
		}
		return z.findkey(adjust, index[:half], key)
	}
	panic("unreachable code")
}

func (z zsnap) compareat(
	i int, key []byte) (
	cmp int, currkey []byte, lv lazyvalue, cas uint64, deleted bool) {

	offset := 4 + (i * 4)
	x := int(binary.BigEndian.Uint32(z[offset : offset+4]))
	ze := zentry(z[x : x+zentrysize])
	ln := int(ze.keylen())
	x += zentrysize
	currkey, cas, deleted = z[x:x+ln], 0, false
	cmp = bytes.Compare(currkey, key)
	//fmt.Printf("z.compareat %v %s %s %v\n", i, key, z[x:x+ln], cmp)
	lv.setfields(0, 0, nil)
	if cmp >= 0 {
		x, ln = x+ln, int(ze.valuelen())
		cas, deleted = ze.seqno(), ze.isdeleted()
		if ze.isvlog() {
			vlogpos := int64(binary.BigEndian.Uint64(z[x : x+8]))
			lv.setfields(int64(ln), vlogpos, nil)
		} else if ln > 0 {
			lv.setfields(int64(ln), 0, z[x:x+ln])
		}
	}
	return cmp, currkey, lv, cas, deleted
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
	index int) (key []byte, lv lazyvalue, seqno uint64, deleted bool) {

	x := int((index * 4) + 4)
	x = int(binary.BigEndian.Uint32(z[x : x+4]))
	ze := zentry(z[x : x+zentrysize])
	seqno, deleted = ze.seqno(), ze.isdeleted()
	vlogok := ze.isvlog()
	keylen, valuelen := int(ze.keylen()), int(ze.valuelen())
	x += zentrysize
	//fmt.Printf("z-entryat %v %v %v\n", index, x, keylen)
	key = z[x : x+keylen]
	x += keylen
	if vlogok {
		vlogpos := int64(binary.BigEndian.Uint64(z[x : x+8]))
		lv.setfields(int64(valuelen), vlogpos, nil)
	} else if valuelen > 0 {
		lv.setfields(int64(valuelen), 0, z[x:x+valuelen])
	} else {
		lv.setfields(0, 0, nil)
	}
	return
}

func (z zsnap) getnext(
	index int) (key []byte, lv lazyvalue, seqno uint64, deleted bool) {

	if index >= 0 && z.isbounded(index+1) {
		return z.entryat(index + 1)
	}
	return key, lv, 0, false
}

func (z zsnap) isbounded(index int) bool {
	idxlen := int(binary.BigEndian.Uint32(z[:4]))
	return (index >= 0) && (index < idxlen)
}
