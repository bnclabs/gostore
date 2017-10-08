package lsm

import "github.com/prataprc/gostore/api"

var yGetpool = make(chan *yGet, 100)

func SetYGetpool(poolsize int) {
	yGetpool = make(chan *yGet, poolsize)
}

type yGet struct {
	a api.Getter
	b api.Getter
}

func YGet(a, b api.Getter) api.Getter {
	yget := getyget()
	yget.a, yget.b = a, b
	return yget.Get
}

func (yget *yGet) Get(key, value []byte) (val []byte, cas uint64, d, ok bool) {
	if val, cas, d, ok = yget.b(key, value); ok {
		return
	}
	val, cas, d, ok = yget.a(key, value)
	putyget(yget)
	return
}

func getyget() (yget *yGet) {
	select {
	case yget = <-yGetpool:
	default:
		yget = &yGet{}
	}
	return
}

func putyget(yget *yGet) {
	select {
	case yGetpool <- yget:
	default: // Leave it to GC.
	}
}
