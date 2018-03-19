package bubt

//import "fmt"
import "encoding/binary"

import "github.com/bnclabs/gostore/lib"

type zblock struct {
	zblocksize int64
	vblocksize int64
	firstkey   []byte
	index      blkindex
	vlog       []byte // value buffer will be valid if vblocksize is > 0
	vlogpos    int64
	buffer     []byte

	// working buffer
	zerovbuff []byte
	entries   []byte // points into buffer
	block     []byte // points into buffer
}

// zblock represents the leaf node in bubt tree, can be in a separate
// file, shape of block is:
//
// n_entries uint32   - 4-byte count of number entries in this zblock.
// blkindex  []uint32 - 4 byte offset into zblock for each entry.
// zentries           - array of zentries.
func newz(zblocksize, vblocksize, vlogpos int64, vlog []byte) (z *zblock) {
	z = &zblock{
		zblocksize: zblocksize,
		vblocksize: vblocksize,
		firstkey:   make([]byte, 0, 256),
		index:      make(blkindex, 0, 64),
		buffer:     make([]byte, zblocksize*2),
		vlog:       vlog,
		vlogpos:    vlogpos,
	}
	if z.vblocksize > 0 {
		z.zerovbuff = make([]byte, vblocksize)
	}
	z.entries = z.buffer[zblocksize:zblocksize]
	return
}

func (z *zblock) reset(vlogpos int64, vlog []byte) *zblock {
	z.firstkey = z.firstkey[:0]
	z.index = z.index[:0]
	z.vlog, z.vlogpos = vlog, vlogpos
	z.buffer = z.buffer[:z.zblocksize*2]
	z.entries = z.entries[:0]
	z.block = nil
	return z
}

func (z *zblock) insert(key, value []byte, seqno uint64, deleted bool) bool {
	if key == nil {
		return false
	} else if z.isoverflow(key, value) {
		return false
	}

	z.index = append(z.index, uint32(len(z.entries)))

	var scratch [24]byte
	ze := zentry(scratch[:])
	ze = ze.setseqno(seqno).cleardeleted()
	ze = ze.setkeylen(uint64(len(key))).setvaluelen(uint64(len(value)))
	if deleted {
		ze.setdeleted()
	}
	ok, vlogpos := z.addtovalueblock(value)
	if ok {
		ze.setvaluelen(uint64(len(value)) + 8)
		ze.setvlog()
	}

	z.entries = append(z.entries, scratch[:]...)
	z.entries = append(z.entries, key...)
	if ok == false { // value in zblock.
		z.entries = append(z.entries, value...)
	} else if vlogpos > 0 { // value in vlog
		binary.BigEndian.PutUint64(scratch[:8], uint64(vlogpos))
		z.entries = append(z.entries, scratch[:8]...)
	}

	z.setfirstkey(key)

	return true
}

func (z *zblock) finalize() bool {
	if len(z.index) == 0 {
		return false
	}
	indexlen := z.index.footprint()
	block := z.buffer[z.zblocksize-indexlen : int64(len(z.buffer))-indexlen]
	// 4-byte length of index array.
	binary.BigEndian.PutUint32(block, uint32(z.index.length()))
	// each index entry is 4 byte, index point into z-block for zentry.
	n := 4
	for _, entryoff := range z.index {
		binary.BigEndian.PutUint32(block[n:], uint32(indexlen)+entryoff)
		n += 4
	}
	// ZERO padding
	n += len(z.entries)
	for i := range block[n:] {
		block[n+i] = 0
	}
	z.block = block
	return true
}

//---- local methods

func (z *zblock) isoverflow(key, value []byte) bool {
	entrysz := int64(zentrysize + len(key))
	if z.vblocksize > 0 {
		entrysz += 8 // just file position into value log.
	} else {
		entrysz += int64(len(value))
	}
	total := int64(len(z.entries)) + entrysz + z.index.nextfootprint()
	if total > z.zblocksize {
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

func (z *zblock) addtovalueblock(value []byte) (bool, int64) {
	var scratch [8]byte

	if z.vblocksize <= 0 {
		return false, 0

	} else if len(value) == 0 {
		return true, 0

	}

	if int64(len(value)) < z.vblocksize {
		remain := z.vblocksize - (int64(len(z.vlog)) % z.vblocksize)
		if int64(len(value)) > remain {
			z.vlog = append(z.vlog, z.zerovbuff[:remain]...)
			z.vlogpos += remain
		}
	}
	vlogpos := z.vlogpos
	if len(value) > 0 {
		binary.BigEndian.PutUint64(scratch[:], uint64(len(value)))
		z.vlog = append(z.vlog, scratch[:]...)
		z.vlog = append(z.vlog, value...)
		z.vlogpos += int64(len(scratch) + len(value))
		return true, vlogpos
	}
	return true, -1
}
