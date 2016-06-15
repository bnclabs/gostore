package bubt

import "encoding/binary"
import "fmt"
import "bytes"

import "github.com/prataprc/storage.go/api"

//---- znode for reading entries.

type znode []byte

func (z znode) rangeforward(
	ss *Snapshot,
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.RangeCallb) bool {

	entries := z.entryslice()
	from := z.searchforward(lkey, entries, cmp[0])
	for x := from; x < int32(len(entries)/4); x++ {
		ekey := z.getentry(uint32(x), entries).key()
		if hkey == nil || bytes.Compare(ekey, hkey) <= cmp[1] {
			koff := x * 4
			entryoff := int64(binary.BigEndian.Uint32(entries[koff : koff+4]))
			nd := new(node)
			ss.newznode(nd, []byte(z[entryoff:]), fpos+entryoff)
			if callb(nd) == false {
				return false
			}
			continue
		}
		return false
	}
	return true
}

func (z znode) searchforward(lkey []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice, call the programmer!")
	} else if lkey == nil {
		return 0
	}

	switch count := len(entries) / 4; count {
	case 0:
		panic("impossible code path, call the programmer!")

	case 1:
		if bytes.Compare(z.getentry(0, entries).key(), lkey) < cmp {
			return 1
		}
		return 0

	default:
		mid := int32(count / 2)
		if bytes.Compare(z.getentry(uint32(mid), entries).key(), lkey) < cmp {
			return mid + z.searchforward(lkey, entries[mid*4:], cmp)
		}
		return z.searchforward(lkey, entries[:mid*4], cmp)
	}
}

func (z znode) rangebackward(
	ss *Snapshot,
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.RangeCallb) bool {

	entries := z.entryslice()
	from := z.searchbackward(hkey, entries, cmp[0])
	for x := from; x >= 0; x-- {
		ekey := z.getentry(uint32(x), entries).key()
		if lkey == nil || bytes.Compare(ekey, lkey) >= cmp[1] {
			koff := x * 4
			entryoff := int64(binary.BigEndian.Uint32(entries[koff : koff+4]))
			nd := new(node)
			ss.newznode(nd, []byte(z[entryoff:]), fpos+entryoff)
			if callb(nd) == false {
				return false
			}
			continue
		}
		return false
	}
	return true
}

func (z znode) searchbackward(hkey []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice, call the programmer!")
	} else if hkey == nil {
		return int32(len(entries)/4) - 1
	}

	switch count := len(entries) / 4; count {
	case 0:
		panic("impossible code path, call the programmer!")

	case 1:
		if bytes.Compare(z.getentry(0, entries).key(), hkey) > cmp {
			return -1
		}
		return 0

	default:
		mid := int32(count / 2)
		if bytes.Compare(z.getentry(uint32(mid), entries).key(), hkey) > cmp {
			return z.searchbackward(hkey, entries[:mid*4], cmp)
		}
		return mid + z.searchbackward(hkey, entries[mid*4:], cmp)
	}
}

func (z znode) getentry(n uint32, entries []byte) zentry {
	off := n * 4
	koff := binary.BigEndian.Uint32(entries[off : off+4])
	return zentry(z[koff:])
}

func (z znode) entryslice() []byte {
	count := binary.BigEndian.Uint32(z[:4])
	return z[4 : 4+(count*4)]
}

func (z znode) dumpkeys(ss *Snapshot, prefix string) {
	entries := z.entryslice()
	for off := 0; off < len(entries); off += 4 {
		koff := binary.BigEndian.Uint32(entries[off : off+4])
		klen := uint32(binary.BigEndian.Uint16(z[koff+26:]))
		fmt.Println(prefix, string(z[koff+28:koff+28+klen]))
	}
}

type zentry []byte

func (z zentry) key() []byte {
	klen := binary.BigEndian.Uint16(z[26 : 26+2])
	return z[26+2 : 26+2+klen]
}
