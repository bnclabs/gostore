package bubt

import "bytes"
import "encoding/binary"

import "github.com/prataprc/storage.go/api"

type mnode []byte

func (m mnode) rangeforward(
	ss *Snapshot, lkey, hkey []byte, cmp [2]int, callb api.RangeCallb) bool {

	var from int32

	entries := m.entryslice()
	switch len(entries) {
	case 0:
		panic("impossible code path, call the programmer !")

	default:
		from = 1 + m.searchforward(lkey, entries, cmp[0])
	}

	for x := from; x < int32(len(entries)/4); x++ {
		vpos := m.getentry(uint32(x), entries).vpos()
		if ss.rangeforward(lkey, hkey, vpos, cmp, callb) == false {
			return false
		}
	}
	return true
}

func (m mnode) searchforward(lkey []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice, call the programmer!")
	} else if lkey == nil {
		return -1
	}

	switch count := len(entries) / 4; count {
	case 0:
		panic("impossible code path, call the programmer!")

	case 1:
		if bytes.Compare(m.getentry(0, entries).key(), lkey) < cmp {
			return -1
		}
		return 0

	default:
		mid := int32(count / 2)
		ekey := m.getentry(uint32(mid), entries).key()
		if bytes.Compare(ekey, lkey) < cmp {
			return mid + m.searchforward(lkey, entries[mid*4:], cmp)
		}
		return m.searchforward(lkey, entries[:mid*4], cmp)
	}
}

func (m mnode) rangebackward(
	ss *Snapshot, lkey, hkey []byte, cmp [2]int, callb api.RangeCallb) bool {

	var from int32

	entries := m.entryslice()
	switch len(entries) {
	case 0:
		panic("impossible code path, call the programmer !")

	default:
		from = m.searchbackward(hkey, entries, cmp[0])
	}

	for x := from; x >= 0; x-- {
		vpos := m.getentry(uint32(x), entries).vpos()
		if ss.rangebackward(lkey, hkey, vpos, cmp, callb) == false {
			return false
		}
	}
	return true
}

func (m mnode) searchbackward(hkey []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice, call the programmer!")
	} else if hkey == nil {
		return int32(len(entries)/4) - 1
	}

	switch count := len(entries) / 4; count {
	case 0:
		panic("impossible code path, call the programmer!")

	case 1:
		if bytes.Compare(m.getentry(0, entries).key(), hkey) > cmp {
			return -1
		}
		return 0

	default:
		mid := int32(count / 2)
		ekey := m.getentry(uint32(mid), entries).key()
		if bytes.Compare(ekey, hkey) > cmp {
			return m.searchbackward(hkey, entries[:mid*4], cmp)
		}
		return mid + m.searchbackward(hkey, entries[mid*4:], cmp)
	}
}

func (m mnode) getentry(n uint32, entries []byte) mentry {
	off := n * 4
	koff := binary.BigEndian.Uint32(entries[off : off+4])
	return mentry(entries[koff:len(entries)])
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
