package bogn

import "time"

type txnmeta struct {
	cursors   chan *Cursor
	txncache  chan *Txn
	viewcache chan *View
}

func (meta *txnmeta) inittxns() {
	maxtxns := 1000 // TODO: no magic number
	meta.txncache = make(chan *Txn, maxtxns)
	meta.viewcache = make(chan *View, maxtxns)
	meta.cursors = make(chan *Cursor, maxtxns*2)
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
	return
}

func (meta *txnmeta) puttxn(txn *Txn) {
	for _, cur := range txn.cursors {
		txn.putcursor(cur)
	}
	txn.cursors = txn.cursors[:0]
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
	view.id, view.snap = id, snap
	if view.id == 0 {
		view.id = uint64(time.Now().UnixNano())
	}
	return
}

func (meta *txnmeta) putview(view *View) {
	for _, cur := range view.cursors {
		view.putcursor(cur)
	}
	view.cursors = view.cursors[:0]
	select {
	case meta.viewcache <- view:
	default: // Left for GC
	}
}
