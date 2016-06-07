package bubt

import "encoding/binary"
import "bytes"

import "github.com/prataprc/storage.go/api"

//---- znode for reading entries.

type znode []byte

func (z znode) rangekey(
	ss *Snapshot,
	key []byte, fpos int64, cmp [2]int, callb api.RangeCallb) bool {

	var nd node

	entries := z.entryslice()
	from := z.searchkey(key, entries, cmp[0])
	for x := from; x < int32(len(entries)/4); x++ {
		ekey := z.getentry(uint32(x), entries).key()
		ge := bytes.Compare(key, ekey) >= cmp[0]
		le := bytes.Compare(key, ekey) >= cmp[1]
		if ge && le {
			koff := x * 4
			offset := fpos + int64(binary.BigEndian.Uint32(entries[koff:koff+4]))
			ss.newznode(&nd, []byte(z), offset)
			if callb(&nd) == false {
				return false
			}

		} else if le == false {
			return false
		}
	}
	return true
}

func (z znode) getentry(n uint32, entries []byte) zentry {
	off := n * 4
	koff := binary.BigEndian.Uint32(entries[off : off+4])
	return zentry(entries[koff:len(entries)])
}

func (z znode) searchkey(key []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice")
	}

	switch count := len(entries) / 4; count {
	case 1:
		return 0

	default:
		mid := int32(count / 2)
		if bytes.Compare(key, z.getentry(uint32(mid), entries).key()) >= cmp {
			return mid + z.searchkey(key, entries[mid*4:], cmp)
		}
		return z.searchkey(key, entries[:mid*4], cmp)
	}
}

func (z znode) entryslice() []byte {
	count := binary.BigEndian.Uint32(z[:4])
	return z[4 : 4+(count*4)]
}

type zentry []byte

func (z zentry) key() []byte {
	klen := binary.BigEndian.Uint16(z[26 : 26+2])
	return z[26+2 : 26+2+klen]
}
