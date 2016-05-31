// +build ignore

package bubt

type bubtmblock struct {
	f        *bubtstore
	fpos     [2]int64
	rpos     int64
	firstkey []byte
	entries  []uint32
	kbuffer  []byte
}

func (f *bubtstore) newm() (m *bubtmblock) {
	select {
	case m = <-f.mpool:
		m.f = f
		m.firstkey = m.firstkey[:0]
		m.entries = m.entries[:0]
		m.kbuffer = f.getbuffer()

	default:
		m = &bubtmblock{
			f:       f,
			entries: make([]uint32, 0),
		}
		m.kbuffer = f.getbuffer()
	}
	f.mnodes++
	return
}

func (m *bubtmblock) insert(block bubtblock) (ok bool) {
	var scratch [16]byte // 2 + 8

	if block == nil {
		return false
	}

	kpos, key := block.startkey()
	childpos, rpos := block.offset(), block.roffset()

	// check whether enough space available in the block.
	entrysz := 2 + len(key) + 8 /*vpos*/ + 8 /*rpos*/
	arrayblock := 4 + (len(m.entries) * 4)
	if (arrayblock + len(m.kbuffer) + entrysz) > m.f.mblocksize {
		return false
	}

	// remember first key
	if len(m.firstkey) == 0 {
		m.firstkey = m.firstkey[:len(key)]
		copy(m.firstkey, key)
	}

	m.entries = append(m.entries, len(m.kbuffer))

	// encode key
	binary.BigEndian.PutUint16(scratch[:2], len(key))
	m.kbuffer = append(m.kbuffer, scratch[:2])
	m.kbuffer = append(m.kbuffer, key...)
	// encode value
	binary.BigEndian.PutUint64(scratch[:8], m.f.mvpos(childpos))
	m.kbuffer = append(m.kbuffer, scratch[:8]...)
	// encode reduce-value
	if m.f.mreduce {
		binary.BigEndian.PutUint64(scratch[:8], m.f.mvpos(childpos))
		m.kbuffer = append(m.kbuffer, scratch[:8]...)
	}

	return true
}

func (m *bubtmblock) startkey() (int64, []byte) {
	return -1, m.f.firstkey // NOTE: we don't need kpos
}

func (m *bubtmblock) offset() int64 {
	return m.fpos[0]
}

func (m *bubtmblock) roffset() int64 {
	return m.rpos
}

func (m *bubtmblock) finalize() {
	arrayblock := 4 + (len(m.entries) * 4)
	sz, ln := arrayblock+len(m.kbuffer), len(m.kbuffer)
	if sz > m.f.mblocksize {
		fmsg := "mblock buffer overflow %v > %v"
		panic(fmt.Sprintf(fmsg, sz, m.f.mblocksize))
	}

	m.kbuffer = m.kbuffer[:sz] // first increase slice length

	copy(m.kbuffer[arrayblock:], m.kbuffer[:ln])
	n := 0
	binary.BigEndian.PutUint32(m.kbuffer[n:], len(m.entries))
	n += 4
	for _, entry := range m.entries {
		binary.BigEndian.PutUint32(m.kbuffer[n:], arrayblock+entry)
		n += 4
	}
}

func (m *bubtmblock) reduce() []byte {
	if m.f.mreduce {
		if z.f.hasdatafile() {
			return nil
		}
		panic("enable datafile for mreduce")
	}
	return nil
}
