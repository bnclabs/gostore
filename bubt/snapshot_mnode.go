package bubt

import "bytes"
import "encoding/binary"

type mnode []byte

func (m mnode) getentry(n uint32, entries []byte) mentry {
	off := n * 4
	koff := binary.BigEndian.Uint32(entries[off : off+4])
	return mentry(entries[koff:len(entries)])
}

func (m mnode) searchkey(key []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice")
	}

	switch count := len(entries) / 4; count {
	case 1:
		if bytes.Compare(key, m.getentry(0, entries).key()) >= cmp {
			return 0
		}
		return -1

	default:
		mid := int32(count / 2)
		if bytes.Compare(key, m.getentry(uint32(mid), entries).key()) >= cmp {
			return mid + m.searchkey(key, entries[mid*4:], cmp)
		}
		return m.searchkey(key, entries[:mid*4], cmp)
	}
}

func (m mnode) entryslice() []byte {
	count := binary.BigEndian.Uint32(m[:4])
	return m[4 : 4+(count*4)]
}

type mentry []byte

func (m mentry) key() []byte {
	klen := binary.BigEndian.Uint16(m[:2])
	return m[2 : 2+klen]
}

func (m mentry) vpos() int64 {
	klen := binary.BigEndian.Uint16(m[:2])
	return int64(binary.BigEndian.Uint64(m[2+klen:]))
}

func (m mentry) rpos() int64 {
	klen := binary.BigEndian.Uint16(m[:2])
	return int64(binary.BigEndian.Uint64(m[2+klen+8:]))
}
