package bubt

import "encoding/binary"
import "fmt"

import "github.com/prataprc/storage.go/api"

type bubtzblock struct {
	f        *Bubtstore
	fpos     [2]int64 // kpos, vpos
	rpos     int64
	firstkey []byte
	entries  []uint32
	keys     [][]byte
	values   [][]byte
	reduced  []byte
	kbuffer  []byte
	dbuffer  []byte
}

func (f *Bubtstore) newz(fpos [2]int64) (z *bubtzblock) {
	select {
	case z = <-f.zpool:
		z.f, z.fpos = f, fpos
		z.firstkey = z.firstkey[:0]
		z.entries = z.entries[:0]
		z.keys = z.keys[:0]
		z.values = z.values[:0]
		z.kbuffer, z.dbuffer = z.kbuffer[:0], z.dbuffer[:0]

	default:
		z = &bubtzblock{
			f:        f,
			fpos:     fpos,
			firstkey: make([]byte, 0, api.MaxKeymem),
			entries:  make([]uint32, 0, 16),
			keys:     make([][]byte, 0, 16),
			values:   make([][]byte, 0, 16),
			kbuffer:  make([]byte, 0, f.zblocksize),
			dbuffer:  make([]byte, 0, f.zblocksize),
		}
	}
	f.znodes++
	return
}

func (z *bubtzblock) insert(nd api.Node) (ok bool) {
	var key, value []byte
	var scratch [26]byte

	if nd == nil {
		return false
	} else if key, value = nd.Key(), nd.Value(); int64(len(key)) > api.MaxKeymem {
		panic(fmt.Errorf("key cannot exceed %v", api.MaxKeymem))
	} else if int64(len(value)) > api.MaxValmem {
		panic(fmt.Errorf("value cannot exceed %v", api.MaxValmem))
	}

	// check whether enough space available in the block.
	entrysz := len(scratch) + 2 + len(key) // TODO: avoid magic numbers
	if z.f.hasdatafile() {
		entrysz += 8
	} else {
		entrysz += 2 + len(value) // TODO: avoid magic numbers
	}
	arrayblock := 4 + (len(z.entries) * 4)
	if int64(arrayblock+len(z.kbuffer)+entrysz) > z.f.zblocksize {
		return false
	}

	z.keys, z.values = append(z.keys, key), append(z.values, value)

	if len(z.firstkey) == 0 {
		z.firstkey = z.firstkey[:len(key)]
		copy(z.firstkey, key)
	}

	z.entries = append(z.entries, uint32(len(z.kbuffer)))
	z.f.a_keysize.Add(int64(len(key)))
	z.f.a_valsize.Add(int64(len(value)))

	// encode metadadata {vbno(2), vbuuid(8), bornseqno(8), deadseqno(8)}
	binary.BigEndian.PutUint16(scratch[:2], nd.Vbno())         // 2 bytes
	binary.BigEndian.PutUint64(scratch[2:10], nd.Vbuuid())     // 8 bytes
	binary.BigEndian.PutUint64(scratch[10:18], nd.Bornseqno()) // 8 bytes
	binary.BigEndian.PutUint64(scratch[18:26], nd.Deadseqno()) // 8 bytes
	z.kbuffer = append(z.kbuffer, scratch[:26]...)
	// encode key {keylen(2-byte), key(n-byte)}
	binary.BigEndian.PutUint16(scratch[:2], uint16(len(key)))
	z.kbuffer = append(z.kbuffer, scratch[:2]...)
	z.kbuffer = append(z.kbuffer, key...)
	// encode value
	if z.f.hasdatafile() {
		vpos := z.fpos[1] + int64(len(z.dbuffer))
		binary.BigEndian.PutUint16(scratch[:2], uint16(len(value)))
		z.dbuffer = append(z.dbuffer, scratch[:2]...)
		z.dbuffer = append(z.dbuffer, value...)
		binary.BigEndian.PutUint64(scratch[:8], uint64(vpos))
		z.kbuffer = append(z.kbuffer, scratch[:8]...)
	} else {
		binary.BigEndian.PutUint16(scratch[:2], uint16(len(value)))
		z.kbuffer = append(z.kbuffer, scratch[:2]...)
		z.kbuffer = append(z.kbuffer, value...)
	}
	return true
}

func (z *bubtzblock) startkey() (int64, []byte) {
	if len(z.entries) > 0 {
		koff := binary.BigEndian.Uint32(z.kbuffer[4:8])
		return z.fpos[0] + int64(koff), z.firstkey
	}
	return z.fpos[0], nil
}

func (z *bubtzblock) offset() int64 {
	return z.fpos[0]
}

func (z *bubtzblock) roffset() int64 {
	return z.rpos
}

func (z *bubtzblock) finalize() {
	arrayblock := 4 + (len(z.entries) * 4)
	sz, ln := arrayblock+len(z.kbuffer), len(z.kbuffer)
	if int64(sz) > z.f.zblocksize {
		fmsg := "zblock buffer overflow %v > %v, call the programmer!"
		panic(fmt.Sprintf(fmsg, sz, z.f.zblocksize))
	}

	z.kbuffer = z.kbuffer[:sz] // first increase slice length

	copy(z.kbuffer[arrayblock:], z.kbuffer[:ln])
	n := 0
	binary.BigEndian.PutUint32(z.kbuffer[n:], uint32(len(z.entries)))
	n += 4
	for _, koff := range z.entries {
		binary.BigEndian.PutUint32(z.kbuffer[n:], uint32(arrayblock)+koff)
		n += 4
	}
}

func (z *bubtzblock) reduce() []byte {
	doreduce := func(rereduce bool, keys, values [][]byte) []byte {
		return nil
	}
	if z.f.mreduce && z.f.hasdatafile() == false {
		panic("enable datafile for mreduce")
	} else if z.f.mreduce == false {
		panic("mreduce not configured")
	} else if z.reduced != nil {
		return z.reduced
	}
	z.reduced = doreduce(false /*rereduce*/, z.keys, z.values)
	return z.reduced
}

//----- read path

type bubtznode struct {
	f      *Bubtstore
	offset int64
	data   []byte
	value  []byte
}

func (f *Bubtstore) newbubtznode(data []byte, offset int64) *bubtznode {
	return &bubtznode{
		f:      f,
		data:   data,
		offset: offset,
		value:  make([]byte, 0),
	}
}

//---- NodeGetter implementation

// Vbno implement NodeGetter{} interface.
func (n *bubtznode) Vbno() (vbno uint16) {
	return binary.BigEndian.Uint16(n.data[:2])
}

// Access implement NodeGetter{} interface.
func (n *bubtznode) Access() (ts uint64) {
	return 0 // TODO: should we panic ??
}

// Vbuuid implement NodeGetter{} interface.
func (n *bubtznode) Vbuuid() (uuid uint64) {
	return binary.BigEndian.Uint64(n.data[2:10])
}

// Bornseqno implement NodeGetter{} interface.
func (n *bubtznode) Bornseqno() (seqno uint64) {
	return binary.BigEndian.Uint64(n.data[10:18])
}

// Deadseqno implement NodeGetter{} interface.
func (n *bubtznode) Deadseqno() (seqno uint64) {
	return binary.BigEndian.Uint64(n.data[18:26])
}

// Key implement NodeGetter{} interface.
func (n *bubtznode) Key() (key []byte) {
	klen := binary.BigEndian.Uint16(n.data[26:28])
	return n.data[28 : 28+klen]
}

// Value implement NodeGetter{} interface.
func (n *bubtznode) Value() (value []byte) {
	klen := binary.BigEndian.Uint16(n.data[26:28])
	start := 28 + klen
	if n.f.hasdatafile() {
		var vbuf [2]byte
		vpos := int64(binary.BigEndian.Uint64(n.data[start : start+8]))
		if ln, err := n.f.datafd.ReadAt(vbuf[:], vpos); err != nil {
			panic(err)
		} else if ln != len(vbuf) {
			panic("insufficient data")
		}
		vlen := int64(binary.BigEndian.Uint16(vbuf[:]))
		if int64(cap(n.value)) < vlen {
			n.value = make([]byte, 0, vlen)
		}
		n.value = n.value[:vlen]
		if ln, err := n.f.datafd.ReadAt(n.value, vpos+2); err != nil {
			panic(err)
		} else if ln != len(n.value) {
			panic("insufficient data")
		}
		return n.value
	}
	vlen := binary.BigEndian.Uint16(n.data[start : start+2])
	return n.data[start+2 : start+2+vlen]
}

// Fpos implement NodeGetter{} interface.
func (n *bubtznode) Fpos() (level byte, offset int64) {
	klen := binary.BigEndian.Uint16(n.data[26:28])
	start := 28 + klen
	if n.f.hasdatafile() {
		vpos := binary.BigEndian.Uint64(n.data[start : start+8])
		return n.f.level, int64(vpos)
	}
	return n.f.level, n.offset + int64(start)
}

//---- NodeSetter implementation

// Setvbno implement NodeSetter{} interface.
func (n *bubtznode) Setvbno(vbno uint16) api.Node {
	panic("not implemented")
}

// Setaccess implement NodeSetter{} interface.
func (n *bubtznode) Setaccess(access uint64) api.Node {
	panic("not implemented")
}

// SetVbuuid implement NodeSetter{} interface.
func (n *bubtznode) SetVbuuid(uuid uint64) api.Node {
	panic("not implemented")
}

// SetFpos implement NodeSetter{} interface.
func (n *bubtznode) SetFpos(level byte, offset uint64) api.Node {
	panic("not implemented")
}

// SetBornseqno implement NodeSetter{} interface.
func (n *bubtznode) SetBornseqno(seqno uint64) api.Node {
	panic("not implemented")
}

// SetDeadseqno implement NodeSetter{} interface.
func (n *bubtznode) SetDeadseqno(seqno uint64) api.Node {
	panic("not implemented")
}
