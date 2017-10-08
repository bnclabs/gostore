package lsm

import "io"

import "github.com/prataprc/gostore/api"

var ySortpool = make(chan *ySort, 100)

func SetYSortpool(poolsize int) {
	ySortpool = make(chan *ySort, poolsize)
}

type ySort struct {
	a                          api.Iterator
	b                          api.Iterator
	nextop                     api.Iterator
	akey, avalue, bkey, bvalue []byte
	aseqno, bseqno             uint64
	adel, bdel                 bool
	aerr, berr                 error
}

func YSort(a, b api.Iterator) api.Iterator {
	ys := getysort()
	ys.a, ys.b, ys.nextop = a, b, ys.do0
	return ys.YNext
}

func (ys *ySort) YNext(
	fin bool) (key, val []byte, seqno uint64, del bool, err error) {
	return ys.nextop(fin)
}

func (ys *ySort) do0(fin bool) ([]byte, []byte, uint64, bool, error) {
	// load
	if ys.berr == nil {
		ys.bkey, ys.bvalue, ys.bseqno, ys.bdel, ys.berr = ys.b(fin)
	}
	if ys.aerr == nil {
		ys.akey, ys.avalue, ys.aseqno, ys.adel, ys.aerr = ys.a(fin)
	}
	return ys.lambdasort()
}

func (ys *ySort) do1(fin bool) ([]byte, []byte, uint64, bool, error) {
	if ys.berr == nil {
		ys.bkey, ys.bvalue, ys.bseqno, ys.bdel, ys.berr = ys.b(fin)
	}
	return ys.lambdasort()
}

func (ys *ySort) do2(fin bool) ([]byte, []byte, uint64, bool, error) {
	if ys.aerr == nil {
		ys.akey, ys.avalue, ys.aseqno, ys.adel, ys.aerr = ys.a(fin)
	}
	return ys.lambdasort()
}

func (ys *ySort) lambdasort() ([]byte, []byte, uint64, bool, error) {
	if ys.berr == nil && ys.bseqno > ys.aseqno {
		ys.nextop = ys.do1
		return ys.bkey, ys.bvalue, ys.bseqno, ys.bdel, ys.berr
	} else if ys.aerr == nil {
		ys.nextop = ys.do2
		return ys.akey, ys.avalue, ys.aseqno, ys.adel, ys.aerr
	}
	return nil, nil, 0, false, io.EOF
}

func getysort() (ys *ySort) {
	select {
	case ys = <-ySortpool:
	default:
		ys = &ySort{}
	}
	return
}

func putysort(ys *ySort) {
	select {
	case ySortpool <- ys:
	default: // Leave it to GC.
	}
}
