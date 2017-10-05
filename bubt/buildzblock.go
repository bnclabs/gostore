package bubt

//import "fmt"
import "encoding/binary"

import "github.com/prataprc/gostore/lib"

type zblock struct {
	blocksize int64
	firstkey  []byte
	index     blkindex
	buffer    []byte
	entries   []byte // points into buffer
	block     []byte // points into buffer
}

// zblock represents the leaf node in bubt tree, can be in a separate
// file, shape of block is:
//
// n_entries uint32   - 4-byte count of number entries in this zblock.
// blkindex  []uint32 - 4 byte offset into zblock for each entry.
// zentries           - array of zentries.
func newz(blocksize int64) (z *zblock) {
	z = &zblock{
		blocksize: blocksize,
		firstkey:  make([]byte, 0, 256),
		index:     make(blkindex, 0, 64),
		buffer:    make([]byte, blocksize*2),
	}
	z.entries = z.buffer[blocksize:blocksize]
	return
}

func (z *zblock) reset() *zblock {
	z.firstkey = z.firstkey[:0]
	z.index = z.index[:0]
	z.buffer = z.buffer[:z.blocksize*2]
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
	z.entries = append(z.entries, scratch[:]...)
	z.entries = append(z.entries, key...)
	if len(value) > 0 {
		z.entries = append(z.entries, value...)
	}

	z.setfirstkey(key)

	return true
}

func (z *zblock) finalize() bool {
	if len(z.index) == 0 {
		return false
	}
	indexlen := z.index.footprint()
	block := z.buffer[z.blocksize-indexlen : int64(len(z.buffer))-indexlen]
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
	entrysz := int64(len(key) + len(value) + zentrysize)
	total := int64(len(z.entries)) + entrysz + z.index.nextfootprint()
	if total > z.blocksize {
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
