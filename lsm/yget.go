package lsm

import "github.com/prataprc/gostore/api"

//var yGetpool = make(chan *yGet, 1000)
//
//type yGet struct {
//	a api.Getter
//	b api.Getter
//}
//
//func SetYGetpool(poolsize int) {
//	yGetpool = make(chan *yGet, poolsize)
//}
//
//func YGet(a, b api.Getter) api.Getter {
//	yget := getyget()
//	yget.a, yget.b = a, b
//	return yget.get
//}
//
//func (yget *yGet) get(key, value []byte) (val []byte, cas uint64, d, ok bool) {
//	if val, cas, d, ok = yget.b(key, value); ok {
//		putyget(yget)
//		return
//	}
//	val, cas, d, ok = yget.a(key, value)
//	putyget(yget)
//	return
//}
//
//func getyget() (yget *yGet) {
//	select {
//	case yget = <-yGetpool:
//	default:
//		yget = &yGet{}
//	}
//	return
//}
//
//func putyget(yget *yGet) {
//	select {
//	case yGetpool <- yget:
//	default: // Leave it to GC.
//	}
//}

// YGet is a get combinator that takes two get API and return a new Get api
// that handles LSM. Note that if b argument is supplied, then it is assumed
// as the latest version.
func YGet(a, b api.Getter) api.Getter {
	return func(key, value []byte) (val []byte, cas uint64, d, ok bool) {
		if val, cas, d, ok = b(key, value); ok {
			return
		}
		val, cas, d, ok = a(key, value)
		return
	}
}
