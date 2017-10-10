package lsm

import "bytes"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

func cp(dst, src []byte) []byte {
	dst = lib.Fixbuffer(dst, int64(len(src)))
	copy(dst, src)
	return dst
}

func pull(
	name string, x api.Iterator, fin bool,
	k, v []byte) ([]byte, []byte, uint64, bool, error) {

	key, val, seqno, del, err := x(fin)
	for err == nil && bytes.Compare(key, k) == 0 {
		//fmt.Printf("skip %s %s %s %v\n", name, key, k, err)
		key, val, seqno, del, err = x(fin)
	}
	return cp(k, key), cp(v, val), seqno, del, err
}

func YSort(name string, a, b api.Iterator) api.Iterator {
	var aseqno, bseqno uint64
	var adel, bdel bool
	var aerr, berr error

	key, val := make([]byte, 16), make([]byte, 16)

	bkey, bval := make([]byte, 16), make([]byte, 16)
	bkey, bval, bseqno, bdel, berr = pull("b", b, false /*fin*/, bkey, bval)
	akey, aval := make([]byte, 16), make([]byte, 16)
	akey, aval, aseqno, adel, aerr = pull("a", a, false /*fin*/, akey, aval)
	//fmt.Printf("%v/a - %q %q %v\n", name, akey, aval, aseqno)
	//fmt.Printf("%v/b - %q %q %v\n", name, bkey, bval, bseqno)

	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		var seqno uint64
		var del bool
		var err error

		if aerr != nil {
			key, val = cp(key, bkey), cp(val, bval)
			seqno, del, err = bseqno, bdel, berr
			bkey, bval, bseqno, bdel, berr = pull("b", b, fin, bkey, bval)

		} else if berr != nil {
			key, val = cp(key, akey), cp(val, aval)
			seqno, del, err = aseqno, adel, aerr
			akey, aval, aseqno, adel, aerr = pull("a", a, fin, akey, aval)

		} else if cmp := bytes.Compare(bkey, akey); cmp < 0 {
			key, val = cp(key, bkey), cp(val, bval)
			seqno, del, err = bseqno, bdel, berr
			bkey, bval, bseqno, bdel, berr = pull("b", b, fin, bkey, bval)

		} else if cmp > 0 {
			key, val = cp(key, akey), cp(val, aval)
			seqno, del, err = aseqno, adel, aerr
			akey, aval, aseqno, adel, aerr = pull("a", a, fin, akey, aval)

		} else {
			if bseqno > aseqno {
				key, val = cp(key, bkey), cp(val, bval)
				seqno, del, err = bseqno, bdel, berr
			} else {
				key, val = cp(key, akey), cp(val, aval)
				seqno, del, err = aseqno, adel, aerr
			}
			bkey, bval, bseqno, bdel, berr = pull("b", b, fin, bkey, bval)
			akey, aval, aseqno, adel, aerr = pull("a", a, fin, akey, aval)
		}
		return key, val, seqno, del, err
	}
}
