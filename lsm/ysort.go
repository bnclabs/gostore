package lsm

import "io"
import "bytes"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/lib"

func cp(dst, src []byte) []byte {
	dst = lib.Fixbuffer(dst, int64(len(src)))
	copy(dst, src)
	return dst
}

func pull(
	x api.Iterator, fin bool,
	k, v []byte) ([]byte, []byte, uint64, bool, error) {

	if x == nil {
		return k, v, 0, false, io.EOF
	}
	key, val, seqno, del, err := x(fin)
	for err == nil && bytes.Compare(key, k) == 0 {
		//fmt.Printf("skip %s %s %s %v\n", key, k, err)
		key, val, seqno, del, err = x(fin)
	}
	if err != nil {
		return k, v, seqno, del, err
	}
	return cp(k, key), cp(v, val), seqno, del, err
}

// YSort is a iterate combinator that takes two iterator and return
// a new iterator that handles LSM.
func YSort(a, b api.Iterator) api.Iterator {
	key, val := make([]byte, 0, 16), make([]byte, 0, 16)

	bkey, bval := make([]byte, 0, 16), make([]byte, 0, 16)
	bkey, bval, bseqno, bdel, berr := pull(b, false /*fin*/, bkey, bval)
	akey, aval := make([]byte, 0, 16), make([]byte, 0, 16)
	akey, aval, aseqno, adel, aerr := pull(a, false /*fin*/, akey, aval)
	//fmt.Printf("%v/a - %q %q %v\n", akey, aval, aseqno)
	//fmt.Printf("%v/b - %q %q %v\n", bkey, bval, bseqno)

	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		var seqno uint64
		var del bool
		var err error

		if aerr != nil && berr != nil {
			key, val, seqno, del, err = nil, nil, 0, false, io.EOF

		} else if aerr != nil {
			key, val = cp(key, bkey), cp(val, bval)
			seqno, del, err = bseqno, bdel, berr
			bkey, bval, bseqno, bdel, berr = pull(b, fin, bkey, bval)

		} else if berr != nil {
			key, val = cp(key, akey), cp(val, aval)
			seqno, del, err = aseqno, adel, aerr
			akey, aval, aseqno, adel, aerr = pull(a, fin, akey, aval)

		} else if cmp := bytes.Compare(bkey, akey); cmp < 0 {
			key, val = cp(key, bkey), cp(val, bval)
			seqno, del, err = bseqno, bdel, berr
			bkey, bval, bseqno, bdel, berr = pull(b, fin, bkey, bval)

		} else if cmp > 0 {
			key, val = cp(key, akey), cp(val, aval)
			seqno, del, err = aseqno, adel, aerr
			akey, aval, aseqno, adel, aerr = pull(a, fin, akey, aval)

		} else {
			if bseqno > aseqno {
				key, val = cp(key, bkey), cp(val, bval)
				seqno, del, err = bseqno, bdel, berr
			} else {
				key, val = cp(key, akey), cp(val, aval)
				seqno, del, err = aseqno, adel, aerr
			}
			bkey, bval, bseqno, bdel, berr = pull(b, fin, bkey, bval)
			akey, aval, aseqno, adel, aerr = pull(a, fin, akey, aval)
		}
		//fmt.Printf("ysort %q %q %v %v %v\n", key, val, seqno, del, err)
		return key, val, seqno, del, err
	}
}
