package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/storage.go/api"

//---- znode for reading entries.

type znode []byte

func (z znode) rangeforward(
	ss *Snapshot,
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.NodeCallb) bool {

	entries := z.entryslice()
	from := z.searchforward(lkey, entries, cmp[0])
	for x := from; x < int32(len(entries)/4); x++ {
		ekey := z.getentrykey(uint32(x), entries)
		if hkey == nil || api.Binarycmp(ekey, hkey, cmp[1] == 0) <= cmp[1] {
			koff := x * 4
			entryoff := int64(binary.BigEndian.Uint32(entries[koff : koff+4]))
			ze := zentry(z[entryoff : entryoff+zentryLen])
			nd := new(node)
			ss.newznode(nd, ze)
			if callb(ss, 0, nd, nd, nil) == false {
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
		ekey := z.getentrykey(0, entries)
		if api.Binarycmp(ekey, lkey, cmp == 1) >= cmp {
			return 0
		}
		return 1

	default:
		mid := int32(count / 2)
		ekey := z.getentrykey(uint32(mid), entries)
		if api.Binarycmp(ekey, lkey, cmp == 1) >= cmp {
			return z.searchforward(lkey, entries[:mid*4], cmp)
		}
		return mid + z.searchforward(lkey, entries[mid*4:], cmp)
	}
	panic("unreachable code")
}

func (z znode) rangebackward(
	ss *Snapshot,
	lkey, hkey []byte, fpos int64, cmp [2]int, callb api.NodeCallb) bool {

	entries := z.entryslice()
	from := z.searchbackward(hkey, entries, cmp[1])
	for x := from; x >= 0; x-- {
		ekey := z.getentrykey(uint32(x), entries)
		if lkey == nil || api.Binarycmp(ekey, lkey, cmp[0] == 1) >= cmp[0] {
			koff := x * 4
			entryoff := int64(binary.BigEndian.Uint32(entries[koff : koff+4]))
			ze := zentry(z[entryoff : entryoff+zentryLen])
			nd := new(node)
			ss.newznode(nd, ze)
			if callb(ss, 0, nd, nd, nil) == false {
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
		ekey := z.getentrykey(0, entries)
		if api.Binarycmp(ekey, hkey, cmp == 0) > cmp {
			return -1
		}
		return 0

	default:
		mid := int32(count / 2)
		ekey := z.getentrykey(uint32(mid), entries)
		if api.Binarycmp(ekey, hkey, cmp == 0) > cmp {
			return z.searchbackward(hkey, entries[:mid*4], cmp)
		}
		return mid + z.searchbackward(hkey, entries[mid*4:], cmp)
	}
	panic("unreachable code")
}

func (z znode) getentrykey(n uint32, entries []byte) []byte {
	off := n * 4
	eoff := binary.BigEndian.Uint32(entries[off : off+4])
	klen := uint32(zentry(z[eoff : eoff+zentryLen]).keylen())
	return z[eoff+zentryLen : eoff+zentryLen+klen]
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
