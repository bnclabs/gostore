package llrb

var scanlimit = 100

type scanbuf struct {
	keys   [][]byte
	values [][]byte
	seqnos []uint64
	dels   []bool
	windex int
	rindex int
}

func makescanbuf() *scanbuf {
	return &scanbuf{
		keys:   make([][]byte, scanlimit),
		values: make([][]byte, scanlimit),
		seqnos: make([]uint64, scanlimit),
		dels:   make([]bool, scanlimit),
		rindex: 0,
		windex: 0,
	}
}

func (sb *scanbuf) startwrite() {
	sb.windex = 0
}

func (sb *scanbuf) append(key, value []byte, seqno uint64, deleted bool) int {
	if sb.windex >= scanlimit {
		panic("impossible situation, scanlimit exceeded")
	}

	k := sb.keys[sb.windex]
	if k == nil || cap(k) < len(key) {
		k = make([]byte, len(key))
	}
	k = k[:len(key)]
	sb.keys[sb.windex] = k[:copy(k, key)]

	v := sb.values[sb.windex]
	if v == nil || cap(v) < len(value) {
		v = make([]byte, len(value))
	}
	v = v[:len(value)]
	sb.values[sb.windex] = v[:copy(v, value)]

	sb.seqnos[sb.windex] = seqno
	sb.dels[sb.windex] = deleted
	sb.windex++
	return sb.windex
}

func (sb *scanbuf) startread() {
	sb.rindex = 0
}

func (sb *scanbuf) pop() (key, value []byte, seqno uint64, deleted bool) {
	if sb.rindex < sb.windex {
		i := sb.rindex
		key, value = sb.keys[i], sb.values[i]
		seqno, deleted = sb.seqnos[i], sb.dels[i]
		sb.rindex++
	}
	return
}
