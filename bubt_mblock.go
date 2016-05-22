// +build ignore

package storage

type bubtmblock struct {
	f                    *bubtstore
	kbuffer, vbuffer     []byte
	firstkey, firstval   []byte
	firstkpos, firstvpos int64
}

func (f *bubtstore) newm() *bubtmblock {
	mblock := &bubtmblock{f: f, buffer: f.getmblock()}
	return block
}

func (f *bubtmblock) insert(block bubtblock) (kpos, vpos int, ok bool) {
}
