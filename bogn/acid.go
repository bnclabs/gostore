package bogn

import "time"
import "runtime"

type txnmeta struct {
	cursors   chan *Cursor
	txncache  chan *Txn
	viewcache chan *View
}

func (meta *txnmeta) inittxns() {
	maxtxns := runtime.GOMAXPROCS(-1) * 10
	meta.txncache = make(chan *Txn, maxtxns)
	meta.viewcache = make(chan *View, maxtxns)
	meta.cursors = make(chan *Cursor, maxtxns*4)
}

func (meta *txnmeta) gettxn(id uint64, bogn *Bogn, snap *snapshot) (txn *Txn) {
	select {
	case txn = <-meta.txncache:
	default:
		txn = newtxn(id, bogn, snap, meta.cursors)
	}
	txn.id, txn.bogn, txn.snap = id, bogn, snap
	if txn.id == 0 {
		txn.id = uint64(time.Now().UnixNano())
	}
	txn.inittxn()
	return
}

func (meta *txnmeta) puttxn(txn *Txn) {
	for _, cur := range txn.cursors {
		txn.putcursor(cur)
	}
	txn.mwtxn, txn.mrview, txn.mcview = nil, nil, nil
	txn.dviews = txn.dviews[:0]
	txn.cursors, txn.gets = txn.cursors[:0], txn.gets[:0]
	select {
	case meta.txncache <- txn:
	default: // Left for GC
	}
}

func (meta *txnmeta) getview(
	id uint64, bogn *Bogn, snap *snapshot) (view *View) {

	select {
	case view = <-meta.viewcache:
	default:
		view = newview(id, bogn, snap, meta.cursors)
	}
	view.id, view.bogn, view.snap = id, bogn, snap
	if view.id == 0 {
		view.id = uint64(time.Now().UnixNano())
	}
	view.initview()
	return
}

func (meta *txnmeta) putview(view *View) {
	for _, cur := range view.cursors {
		view.putcursor(cur)
	}
	view.mwview, view.mrview, view.mcview = nil, nil, nil
	view.dviews = view.dviews[:0]
	view.cursors, view.gets = view.cursors[:0], view.gets[:0]
	select {
	case meta.viewcache <- view:
	default: // Left for GC
	}
}
