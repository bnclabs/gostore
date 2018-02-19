package bogn

import "sync/atomic"

import "github.com/bnclabs/gostore/api"
import "github.com/bnclabs/gostore/lib"

// View transaction definition. Read only version of Txn.
type View struct {
	id     uint64
	bogn   *Bogn
	snap   *snapshot
	mwview api.Transactor
	mrview api.Transactor
	mcview api.Transactor
	dviews []api.Transactor
	yget   api.Getter

	// working memory.
	cursors []*Cursor
	curchan chan *Cursor
	gets    []api.Getter
}

func newview(id uint64, bogn *Bogn, snap *snapshot, cch chan *Cursor) *View {
	view := &View{
		id: id, bogn: bogn, snap: snap,
		dviews:  make([]api.Transactor, 0, 32),
		cursors: make([]*Cursor, 0, 8),
		curchan: cch,
		gets:    make([]api.Getter, 0, 32),
	}
	return view
}

func (view *View) initview() *View {
	var disks [256]api.Index

	id, snap := view.id, view.snap
	view.mwview = snap.mw.View(id)
	if snap.mr != nil {
		view.mrview = snap.mr.View(id)
	}
	if snap.mc != nil {
		view.mcview = snap.mc.View(id)
	}
	if atomic.LoadInt64(&view.bogn.dgmstate) == 1 {
		for _, disk := range snap.disklevels(disks[:0]) {
			view.dviews = append(view.dviews, disk.View(id))
		}
	}
	view.yget = snap.txnyget(view.mwview, view.gets)
	return view
}

//---- Exported Control methods

// ID return transaction id.
func (view *View) ID() uint64 {
	return view.id
}

// OpenCursor open an active cursor inside the index.
func (view *View) OpenCursor(key []byte) (api.Cursor, error) {
	cur, err := view.getcursor().opencursor(nil, view, key)
	if err != nil {
		view.putcursor(cur)
		return nil, err
	}
	return cur, nil
}

// Commit not allowed.
func (view *View) Commit() error {
	panic("Commit not allowed on view")
}

// Abort view, must be called once done with the view.
func (view *View) Abort() {
	if view.mrview != nil {
		view.mrview.Abort()
	}
	if view.mcview != nil {
		view.mcview.Abort()
	}
	for _, dview := range view.dviews {
		dview.Abort()
	}

	view.mwview.Abort()
	view.bogn.abortview(view)
}

//---- Exported Read methods

// Get value for key from snapshot.
func (view *View) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	return view.yget(key, value)
}

//---- Exported Write methods, for interface compatibility !

// Set is not allowed
func (view *View) Set(key, value, oldvalue []byte) []byte {
	panic("Set not allowed on view")
}

// Delete is not allowed.
func (view *View) Delete(key, oldvalue []byte, lsm bool) []byte {
	panic("Delete not allowed on view")
}

//---- local methods

func (view *View) getcursor() (cur *Cursor) {
	select {
	case cur = <-view.curchan:
	default:
		cur = &Cursor{iters: make([]api.Iterator, 0, 32)}
	}
	view.cursors = append(view.cursors, cur)
	return
}

func (view *View) putcursor(cur *Cursor) {
	cur.txn, cur.view = nil, nil
	cur.key = lib.Fixbuffer(cur.key, 0)
	cur.value = lib.Fixbuffer(cur.value, 0)
	cur.cas, cur.deleted = 0, false
	cur.iter, cur.iters = nil, cur.iters[:0]

	select {
	case view.curchan <- cur:
	default: // leave it for GC
	}
}
