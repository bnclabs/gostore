package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

var _ = fmt.Sprintf("dummy")

const znentriesSz = 4

// zblock represents the leaf node in bubt tree.
//   n_entries uint32 - 4-byte count of number entries in this zblock.
//   zindex []uint32 - 4 byte offset into zblock for each entry.
//   zentries - array of zentries.
type zblock struct {
	f        *Bubt
	fpos     [2]int64 // {kpos, vpos} where zblock starts in file.
	rpos     int64
	firstkey []byte

	index zindex

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
		dbuffer:  make([]byte, 0, f.zblocksize),
	}
	return z
}

func (z *zblock) insert(nd api.Node) (ok, fin bool) {
	if nd == nil {
		return false, true
	}
	key, value := nd.Key(), nd.Value()
	if int64(len(key)) > api.MaxKeymem {
		panic(fmt.Errorf("key cannot exceed %v", api.MaxKeymem))
	} else if int64(len(value)) > api.MaxValmem {
		panic(fmt.Errorf("value cannot exceed %v", api.MaxValmem))
	}

	// check whether enough space available in the block.
	entrysz := int64(zentryLen + len(key))
	if z.f.hasdatafile == false {
		entrysz += int64(len(value))
	}
	index := znentriesSz + z.index.nextfootprint()
	if int64(index+int64(len(z.kbuffer))+entrysz) > z.f.zblocksize {
		return false, false
	}

	z.keys, z.values = append(z.keys, key), append(z.values, value)

	if len(z.firstkey) == 0 {
		z.firstkey = lib.Fixbuffer(z.firstkey, int64(len(key)))
		copy(z.firstkey, key)
	}

	startoff, endoff := len(z.kbuffer), len(z.kbuffer)+zentryLen

	z.f.n_count++
	z.index = append(z.index, uint32(len(z.kbuffer)))
	z.f.a_keysize.Add(int64(len(key)))
	z.f.a_valsize.Add(int64(len(value)))

	z.kbuffer = z.kbuffer[0:endoff]
	entry := zentry(z.kbuffer[startoff:endoff])

	// encode metadadata
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
	return true, false
}

func (z *zblock) finalize() {
	index := znentriesSz + z.index.footprint()
	sz, ln := index+int64(len(z.kbuffer)), len(z.kbuffer)
	if zblksize := z.f.zblocksize; int64(sz) > zblksize {
		fmsg := "zblock overflow %v > %v, call the programmer!"
		panic(fmt.Errorf(fmsg, sz, zblksize))
	}

	z.kbuffer = makespace(z.kbuffer[:z.f.zblocksize], int(index), ln)

	binary.BigEndian.PutUint32(z.kbuffer, uint32(z.index.length()))
	n := znentriesSz
	for _, koff := range z.index {
		binary.BigEndian.PutUint32(z.kbuffer[n:], uint32(index)+koff)
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
