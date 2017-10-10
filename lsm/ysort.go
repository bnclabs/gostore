package lsm

//import "io"

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

func dispatch(
	x api.Iterator, fin bool,
	k, v []byte) ([]byte, []byte, uint64, bool, error) {

	key, value, seqno, del, err := x(fin)
	k = lib.Fixbuffer(k, int64(len(key)))
	copy(k, key)
	v = lib.Fixbuffer(v, int64(len(value)))
	copy(v, value)
	return k, v, seqno, del, err
}

func YSort(a, b api.Iterator) api.Iterator {
	var aseqno, bseqno uint64
	var adel, bdel bool
	var aerr, berr error

	op := 0

	bkey, bvalue := make([]byte, 16), make([]byte, 16)
	bkey, bvalue, bseqno, bdel, berr = dispatch(b, false /*fin*/, bkey, bvalue)

	akey, avalue := make([]byte, 16), make([]byte, 16)
	akey, avalue, aseqno, adel, aerr = dispatch(a, false /*fin*/, akey, avalue)

	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		switch op {
		case 0:
		case 1:
			if berr == nil {
				bkey, bvalue, bseqno, bdel, berr = dispatch(b, fin, bkey, bvalue)
			}
		case 2:
			if aerr == nil {
				akey, avalue, aseqno, adel, aerr = dispatch(a, fin, akey, avalue)
			}
		}
		if aerr != nil {
			return bkey, bvalue, bseqno, bdel, berr
		} else if berr != nil {
			return akey, avalue, aseqno, adel, aerr
		} else if bseqno > aseqno {
			op = 1
			return bkey, bvalue, bseqno, bdel, berr
		}
		op = 2
		return akey, avalue, aseqno, adel, aerr
	}
}
