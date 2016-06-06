package bubt

// import "github.com/prataprc/storage.go/
import "fmt"
import "bytes"
import "encoding/binary"

type mblock struct {
	f        *Bubtstore
	fpos     [2]int64
	rpos     int64
	firstkey []byte
	entries  []uint32
	values   [][]byte
	reduced  []byte
	kbuffer  []byte
}

func (f *Bubtstore) newmblock() (m *mblock) {
	select {
	case m = <-f.mpool:
		m.f = f
		m.firstkey = m.firstkey[:0]
		m.entries = m.entries[:0]
		m.values = m.values[:0]
		m.kbuffer = m.kbuffer[:0]

	default:
		m = &mblock{
			f:       f,
			entries: make([]uint32, 0, 16),
			values:  make([][]byte, 0, 16),
			kbuffer: make([]byte, 0, f.mblocksize),
		}
	}
	f.mnodes++
	return
}

func (m *mblock) insert(block blocker) (ok bool) {
	var scratch [16]byte // 2 + 8

	if block == nil {
		return false
	}

	_, key := block.startkey()
	coffset, rpos := block.backref(), block.roffset()
	m.values = append(m.values, block.reduce())

	// check whether enough space available in the block.
	entrysz := 2 + len(key) + 8 /*vpos*/ + 8 /*rpos*/
	arrayblock := 4 + (len(m.entries) * 4)
	if (arrayblock + len(m.kbuffer) + entrysz) > int(m.f.mblocksize) {
		return false
	}

	// remember first key
	if len(m.firstkey) == 0 {
		m.firstkey = m.firstkey[:len(key)]
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
	if m.f.mreduce {
		binary.BigEndian.PutUint64(scratch[:8], uint64(rpos))
		m.kbuffer = append(m.kbuffer, scratch[:8]...)
	}

	return true
}

func (m *mblock) finalize() {
	arrayblock := 4 + (len(m.entries) * 4)
	sz, ln := arrayblock+len(m.kbuffer), len(m.kbuffer)
	if int64(sz) > m.f.mblocksize {
		fmsg := "mblock buffer overflow %v > %v"
		panic(fmt.Sprintf(fmsg, sz, m.f.mblocksize))
	}

	m.kbuffer = m.kbuffer[:m.f.mblocksize] // first increase slice length

	copy(m.kbuffer[arrayblock:], m.kbuffer[:ln])
	n := 0
	binary.BigEndian.PutUint32(m.kbuffer[n:], uint32(len(m.entries)))
	n += 4
	for _, koff := range m.entries {
		binary.BigEndian.PutUint32(m.kbuffer[n:], uint32(arrayblock)+koff)
		n += 4
	}
}

func (m *mblock) reduce() []byte {
	doreduce := func(rereduce bool, keys, values [][]byte) []byte {
		return nil
	}
	if m.f.mreduce && m.f.hasdatafile() == false {
		panic("enable datafile for mreduce")
	} else if m.f.mreduce == false {
		panic("mreduce not configured")
	} else if m.reduced != nil {
		return m.reduced
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

//---- mnode for reading entries.

type mnode []byte

func (m mnode) getentry(n uint32, entries []byte) mentry {
	off := n * 4
	koff := binary.BigEndian.Uint32(entries[off : off+4])
	return mentry(entries[koff:len(entries)])
}

func (m mnode) searchkey(key []byte, entries []byte, cmp int) int32 {
	if (len(entries) % 4) != 0 {
		panic("unaligned entries slice")
	}

	switch count := len(entries) / 4; count {
	case 1:
		if bytes.Compare(key, m.getentry(0, entries).key()) >= cmp {
			return 0
		}
		return -1

	default:
		mid := int32(count / 2)
		if bytes.Compare(key, m.getentry(uint32(mid), entries).key()) >= cmp {
			return mid + m.searchkey(key, entries[mid*4:], cmp)
		}
		return m.searchkey(key, entries[:mid*4], cmp)
	}
}

func (m mnode) entryslice() []byte {
	count := binary.BigEndian.Uint32(m[:4])
	return m[4 : 4+(count*4)]
}

type mentry []byte

func (m mentry) key() []byte {
	klen := binary.BigEndian.Uint16(m[:2])
	return m[2 : 2+klen]
}

func (m mentry) vpos() int64 {
	klen := binary.BigEndian.Uint16(m[:2])
	return int64(binary.BigEndian.Uint64(m[2+klen:]))
}

func (m mentry) rpos() int64 {
	klen := binary.BigEndian.Uint16(m[:2])
	return int64(binary.BigEndian.Uint64(m[2+klen+8:]))
}
