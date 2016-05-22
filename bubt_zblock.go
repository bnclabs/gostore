// +build ignore

package storage

type bubtzblock struct {
	f                    *bubtstore
	kbuffer, vbuffer     []byte
	firstkey, firstval   []byte
	firstkpos, firstvpos int64
}

func (f *bubtstore) newz() *bubtzblock {
	zblock := &bubtzblock{f: f, buffer: f.getzblock()}
	return zblock
}

func (f *bubtzblock) insert(nd Node) (kpos, vpos int, ok bool) {
}
