package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/gostore/lib"

type mblock struct {
	blocksize int64
	firstkey  []byte
	index     hindex
	buffer    []byte
	entries   []byte // points into buffer
	block     []byte // points into buffer
}

// mblock represents the intermediate node in bubt tree,
// shape of block is:
//
// n_entries uint32   - 4-byte count of number entries in this mblock.
// hindex    []uint32 - 4 byte offset into mblock for each entry.
// mentries           - array of mentries.
func newm(blocksize int64) (m *mblock) {
	m = &mblock{
		blocksize: blocksize,
		firstkey:  make([]byte, 0, 256),
		index:     make([]uint32, 0, 64),
		buffer:    make([]byte, 0, blocksize),
	}
	m.entries = m.buffer[blocksize:blocksize]
	return m
}

func (m *mblock) insert(key []byte, vpos int64) (ok bool) {
	if m.isoverflow(key) == false {
		return false
	}

	m.index = append(m.index, uint32(len(m.entries)))

	var scratch [16]byte
	me := mentry(scratch[:])
	me = me.setkeylen(uint64(len(key))).setvpos(uint64(vpos))
	m.entries = append(m.entries, scratch[:]...)
	m.entries = append(m.entries, key...)

	m.setfirstkey(key)

	return true
}

func (m *mblock) finalize() bool {
	if len(m.index) == 0 {
		return false
	}
	indexlen := m.index.footprint()
	block := m.buffer[m.blocksize-indexlen : indexlen-len(m.buffer)]
	// 4-byte length of index array.
	binary.BigEndian.PutUint32(block, uint32(m.index.length()))
	// each index entry is 4 byte, index point into m-block for zentry.
	n := 4
	for _, entryoff := range m.index {
		binary.BigEndian.PutUint32(block[4:], uint32(indexlen)+entryoff)
		n += 4
	}
	// ZERO padding
	for i := range block[n:] {
		block[i] = 0
	}
	m.block = block
	return true
}

//---- local methods

func (m *mblock) isoverflow(key []byte) bool {
	entrysz := len(key) + mentrysize
	total := len(m.entries) + entrysz + z.index.nextfootprint()
	if total > m.blocksize {
		return true
	}
	return false
}

func (m *mblock) setfirstkey(key []byte) {
	if len(m.firstkey) == 0 {
		m.firstkey = lib.Fixbuffer(m.firstkey, int64(len(key)))
		copy(m.firstkey, key)
	}
}
