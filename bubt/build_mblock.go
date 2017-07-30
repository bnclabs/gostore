package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/gostore/lib"

type mblock struct {
	f        *Bubt
	fpos     [2]int64
	rpos     int64
	firstkey []byte

	index zindex

	values  [][]byte
	reduced []byte
	kbuffer []byte
}

func (f *Bubt) newmblock() (m *mblock) {
	m = &mblock{
		f:        f,
		firstkey: nil,
		index:    make([]uint32, 0, 16),
		values:   make([][]byte, 0, 16),
		kbuffer:  make([]byte, 0, f.mblocksize),
	}
	return m
}

func (m *mblock) insert(block blocker) (ok bool) {
	if block == nil {
		return false
	}
	_, key := block.startkey()
	coffset, rpos := block.backref(), block.roffset()
	m.values = append(m.values, block.reduce())
	// check whether enough space available in the block.
	if m.isoverflow(key) == false {
		return false
	}
	m.setfirstkey(key)
	m.index = append(m.index, uint32(len(m.kbuffer)))
	startoff, endoff := len(m.kbuffer), len(m.kbuffer)+mentryLen
	m.kbuffer = m.kbuffer[0:endoff]
	me := mentry(m.kbuffer[startoff:endoff])
	me.setkeylen(uint16(len(key))).setvaluepos(uint64(coffset))
	me.setreducepos(uint64(rpos))
	m.kbuffer = append(m.kbuffer, key...)
	return true
}

func (m *mblock) finalize() {
	// check whether the block size is exceeding.
	indexsz := int(znentriesSz + m.index.footprint())
	sz := int64(indexsz + len(m.kbuffer))
	if mblksize := m.f.mblocksize; sz > mblksize {
		fmsg := "mblock overflow %v > %v, call the programmer!"
		panic(fmt.Errorf(fmsg, sz, mblksize))
	}
	// check make space in kbuffer for index of entries.
	m.kbuffer = makespace(m.kbuffer[:m.f.mblocksize], indexsz, len(m.kbuffer))
	// 4-byte length of index array.
	binary.BigEndian.PutUint32(m.kbuffer, uint32(m.index.length()))
	// each index entry is 4 byte, index point into m-block for entry.
	n := znentriesSz
	for _, koff := range m.index {
		binary.BigEndian.PutUint32(m.kbuffer[n:], uint32(indexsz)+koff)
		n += 4
	}
}

func (m *mblock) reduce() []byte {
	if m.f.mreduce == false {
		return nil
	} else if m.reduced != nil {
		return m.reduced
	}

	doreduce := func(rereduce bool, keys, values [][]byte) []byte {
		return nil
	}
	m.reduced = doreduce(true /*rereduce*/, nil, m.values)
	return m.reduced
}

//---- local methods

func (m *mblock) isoverflow(key []byte) bool {
	entrysz := int64(mentryLen + len(key))
	index := mnentriesSz + m.index.nextfootprint()
	if (index + int64(len(m.kbuffer)) + entrysz) > m.f.mblocksize {
		return false
	}
	return true
}

func (m *mblock) setfirstkey(key []byte) {
	if len(m.firstkey) == 0 {
		m.firstkey = lib.Fixbuffer(m.firstkey, int64(len(key)))
		copy(m.firstkey, key)
	}
}

//---- blocker interface.

func (m *mblock) startkey() (int64, []byte) {
	return -1, m.firstkey // NOTE: we don't need kpos
}

func (m *mblock) offset() int64 {
	return m.fpos[0]
}

func (m *mblock) backref() int64 {
	return m.offset() | 0x1
}

func (m *mblock) roffset() int64 {
	return m.rpos
}
