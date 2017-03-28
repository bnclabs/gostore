package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/gostore/lib"

var _ = fmt.Sprintf("dummy")

type mblock struct {
	f        *Bubt
	fpos     [2]int64
	rpos     int64
	firstkey []byte
	entries  []uint32
	values   [][]byte
	reduced  []byte
	kbuffer  []byte
}

func (f *Bubt) newmblock() (m *mblock) {
	m = &mblock{
		f:        f,
		firstkey: nil,
		entries:  make([]uint32, 0, 16),
		values:   make([][]byte, 0, 16),
		kbuffer:  make([]byte, 0, f.mblocksize),
	}
	return m
}

func (m *mblock) insert(block blocker) (ok bool) {
	var scratch [16]byte

	if block == nil {
		return false
	}

	_, key := block.startkey()
	coffset, rpos := block.backref(), block.roffset()
	m.values = append(m.values, block.reduce())

	// check whether enough space available in the block.
	entrysz := 2 + len(key) + 8 /*vpos*/ + 8 /*rpos*/
	arrayblock := 4 + ((len(m.entries) + 1) * 4)
	if (arrayblock + len(m.kbuffer) + entrysz) > int(m.f.mblocksize) {
		return false
	}

	// remember first key
	if len(m.firstkey) == 0 {
		m.firstkey = lib.Fixbuffer(m.firstkey, int64(len(key)))
		copy(m.firstkey, key)
	}

	m.entries = append(m.entries, uint32(len(m.kbuffer)))

	// encode key
	binary.BigEndian.PutUint16(scratch[:2], uint16(len(key)))
	m.kbuffer = append(m.kbuffer, scratch[:2]...)
	m.kbuffer = append(m.kbuffer, key...)
	// encode value
	binary.BigEndian.PutUint64(scratch[:8], uint64(coffset))
	m.kbuffer = append(m.kbuffer, scratch[:8]...)
	// encode reduce-value
	binary.BigEndian.PutUint64(scratch[:8], uint64(rpos))
	m.kbuffer = append(m.kbuffer, scratch[:8]...)

	return true
}

func (m *mblock) finalize() {
	arrayblock := 4 + (len(m.entries) * 4)
	sz, ln := arrayblock+len(m.kbuffer), len(m.kbuffer)
	if mblksize := m.f.mblocksize; int64(sz) > mblksize {
		fmsg := "mblock overflow %v > %v, call the programmer!"
		panic(fmt.Errorf(fmsg, sz, mblksize))
	}

	m.kbuffer = makespace(m.kbuffer[:m.f.mblocksize], arrayblock, ln)

	n := 0
	binary.BigEndian.PutUint32(m.kbuffer[n:], uint32(len(m.entries)))
	n += 4
	for _, koff := range m.entries {
		binary.BigEndian.PutUint32(m.kbuffer[n:], uint32(arrayblock)+koff)
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
