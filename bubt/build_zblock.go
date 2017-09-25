package bubt

import "fmt"
import "encoding/binary"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

const znentriesSz = 4

// zblock represents the leaf node in bubt tree.
//   n_entries uint32 - 4-byte count of number entries in this zblock.
//   hindex []uint32 - 4 byte offset into zblock for each entry.
//   zentries - array of zentries.
// note that instance of this structure is not an exact map to on
// disk footprint of zblock.
type zblock struct {
	f        *Bubt
	fpos     [2]int64 // {kpos, vpos} where zblock starts in file.
	rpos     int64
	firstkey []byte

	index hindex

	keys    [][]byte
	values  [][]byte
	reduced []byte
	kbuffer []byte // leaf node block
	dbuffer []byte
}

func (f *Bubt) newz(fpos [2]int64) (z *zblock) {
	z = &zblock{
		f:        f,
		fpos:     fpos,
		firstkey: nil,
		index:    make([]uint32, 0),
		keys:     make([][]byte, 0, 16),
		values:   make([][]byte, 0, 16),
		kbuffer:  make([]byte, 0, f.zblocksize),
		dbuffer:  make([]byte, 0, f.zblocksize), // not flushed in blocksize
	}
	return z
}

func (z *zblock) insert(nd api.Node) (ok, fin bool) {
	if nd == nil {
		return false, true // build finish
	}
	key, value := nd.Key(), nd.Value()
	// check whether enough space available in the block.
	if z.isoverflow(key, value) {
		return false, false // exceeds block and iterations remaining.
	}
	// continue with this block.
	z.f.n_count++
	z.f.a_keysize.Add(int64(len(key)))
	z.f.a_valsize.Add(int64(len(value)))

	z.keys, z.values = append(z.keys, key), append(z.values, value)
	z.setfirstkey(key)

	z.index = append(z.index, uint32(len(z.kbuffer)))
	startoff, endoff := len(z.kbuffer), len(z.kbuffer)+zentryLen
	z.kbuffer = z.kbuffer[0:endoff]
	entry := zentry(z.kbuffer[startoff:endoff])
	entry = z.setmetadata(nd, z.setflags(nd, entry))

	entry.setkeylen(uint16(len(key)))
	if z.f.hasdatafile {
		var scratch [8]byte
		vpos := z.fpos[1] + int64(len(z.dbuffer))
		binary.BigEndian.PutUint64(scratch[:8], uint64(len(value)))
		z.dbuffer = append(z.dbuffer, scratch[:8]...)
		z.dbuffer = append(z.dbuffer, value...)
		entry.setvaluenum(uint64(vpos))
	} else {
		entry.setvaluenum(uint64(len(value)))
	}

	// encode key and value
	z.kbuffer = append(z.kbuffer, key...)
	if z.f.hasdatafile == false {
		z.kbuffer = append(z.kbuffer, value...)
	}
	return true, false // finished iterations.
}

func (z *zblock) finalize() {
	// check whether block size is exceeding.
	indexsz := int(znentriesSz + z.index.footprint())
	sz := int64(indexsz) + int64(len(z.kbuffer))
	if zblksize := z.f.zblocksize; int64(sz) > zblksize {
		fmsg := "zblock overflow %v > %v, call the programmer!"
		panic(fmt.Errorf(fmsg, sz, zblksize))
	}
	// check make space in kbuffer for index of entries.
	z.kbuffer = makespace(z.kbuffer[:z.f.zblocksize], indexsz, len(z.kbuffer))
	// 4-byte length of index array.
	binary.BigEndian.PutUint32(z.kbuffer, uint32(z.index.length()))
	// each index entry is 4 byte, index point into z-block for zentry.
	n := znentriesSz
	for _, koff := range z.index {
		binary.BigEndian.PutUint32(z.kbuffer[n:], uint32(indexsz)+koff)
		n += 4
	}
}

func (z *zblock) reduce() []byte {
	if z.f.mreduce == false {
		return nil
	} else if z.reduced != nil {
		return z.reduced
	}
	doreduce := func(rereduce bool, keys, values [][]byte) []byte {
		return nil
	}
	z.reduced = doreduce(false /*rereduce*/, z.keys, z.values)
	return z.reduced
}

//---- local methods

func (z *zblock) isoverflow(key, value []byte) bool {
	entrysz := int64(zentryLen + len(key))
	if z.f.hasdatafile == false {
		entrysz += int64(len(value))
	}
	index := znentriesSz + z.index.nextfootprint()
	if int64(index+int64(len(z.kbuffer))+entrysz) > z.f.zblocksize {
		return true
	}
	return false
}

func (z *zblock) setfirstkey(key []byte) {
	if len(z.firstkey) == 0 {
		z.firstkey = lib.Fixbuffer(z.firstkey, int64(len(key)))
		copy(z.firstkey, key)
	}
}

func (z *zblock) setflags(nd api.Node, entry zentry) zentry {
	flags := zentryFlags(entry.getheader())
	if nd.IsDeleted() {
		flags = flags.setdeleted()
	} else {
		flags = flags.cleardeleted()
	}
	if z.f.hasdatafile {
		flags = flags.setvalfile()
	} else {
		flags = flags.clearvalfile()
	}
	entry.setflags(flags)
	return entry
}

func (z *zblock) setmetadata(nd api.Node, entry zentry) zentry {
	entry.setvbno(nd.Vbno())
	var vbuuid, bornseqno, deadseqno uint64
	if z.f.hasvbuuid {
		vbuuid = nd.Vbuuid()
	}
	if z.f.hasbornseqno {
		bornseqno = nd.Bornseqno()
	}
	if z.f.hasdeadseqno {
		deadseqno = nd.Deadseqno()
	}
	entry.setvbuuid(vbuuid).setbornseqno(bornseqno).setdeadseqno(deadseqno)
	return entry
}

//---- blocker interface.

func (z *zblock) startkey() (int64, []byte) {
	if z.index.length() > 0 {
		koff := binary.BigEndian.Uint32(z.kbuffer[4:8])
		return z.fpos[0] + int64(koff), z.firstkey
	}
	return z.fpos[0], nil
}

func (z *zblock) offset() int64 {
	return z.fpos[0]
}

func (z *zblock) backref() int64 {
	return z.offset()
}

func (z *zblock) roffset() int64 {
	return z.rpos
}
