package lsm

//import "fmt"
import "bytes"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

//var ySortpool = make(chan *ySort, 100)
//
//func SetYSortpool(poolsize int) {
//	ySortpool = make(chan *ySort, poolsize)
//}
//
//type ySort struct {
//	a                          api.Iterator
//	b                          api.Iterator
//	nextop                     api.Iterator
//	akey, avalue, bkey, bvalue []byte
//	aseqno, bseqno             uint64
//	adel, bdel                 bool
//	aerr, berr                 error
//}
//
//func YSort(a, b api.Iterator) api.Iterator {
//	ys := getysort()
//	ys.a, ys.b, ys.nextop = a, b, ys.do0
//	return ys.YNext
//}
//
//func (ys *ySort) YNext(
//	fin bool) (key, val []byte, seqno uint64, del bool, err error) {
//	return ys.nextop(fin)
//}
//
//func (ys *ySort) do0(fin bool) ([]byte, []byte, uint64, bool, error) {
//	// load
//	if ys.berr == nil {
//		ys.bkey, ys.bvalue, ys.bseqno, ys.bdel, ys.berr = ys.b(fin)
//	}
//	if ys.aerr == nil {
//		ys.akey, ys.avalue, ys.aseqno, ys.adel, ys.aerr = ys.a(fin)
//	}
//	return ys.lambdasort()
//}
//
//func (ys *ySort) do1(fin bool) ([]byte, []byte, uint64, bool, error) {
//	if ys.berr == nil {
//		ys.bkey, ys.bvalue, ys.bseqno, ys.bdel, ys.berr = ys.b(fin)
//	}
//	return ys.lambdasort()
//}
//
//func (ys *ySort) do2(fin bool) ([]byte, []byte, uint64, bool, error) {
//	if ys.aerr == nil {
//		ys.akey, ys.avalue, ys.aseqno, ys.adel, ys.aerr = ys.a(fin)
//	}
//	return ys.lambdasort()
//}
//
//func (ys *ySort) lambdasort() ([]byte, []byte, uint64, bool, error) {
//	if ys.berr == nil && ys.bseqno > ys.aseqno {
//		ys.nextop = ys.do1
//		return ys.bkey, ys.bvalue, ys.bseqno, ys.bdel, ys.berr
//	} else if ys.aerr == nil {
//		ys.nextop = ys.do2
//		return ys.akey, ys.avalue, ys.aseqno, ys.adel, ys.aerr
//	}
//	return nil, nil, 0, false, io.EOF
//}
//
//func getysort() (ys *ySort) {
//	select {
//	case ys = <-ySortpool:
//	default:
//		ys = &ySort{}
//	}
//	return
//}
//
//func putysort(ys *ySort) {
//	select {
//	case ySortpool <- ys:
//	default: // Leave it to GC.
//	}
//}

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
