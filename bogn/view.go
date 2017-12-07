package bogn

import "github.com/prataprc/gostore/api"

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

	cursors []*Cursor
	curchan chan *Cursor
	gets    []api.Getter
}

func newview(id uint64, bogn *Bogn, snap *snapshot, cch chan *Cursor) *View {
	var disks [256]api.Index

	view := &View{
		id: id, snap: snap,
		dviews:  make([]api.Transactor, 0, 32),
		cursors: make([]*Cursor, 0, 8),
		curchan: cch,
		gets:    make([]api.Getter, 32),
	}
	view.mwview = snap.mw.View(id)
	if snap.mr != nil {
		view.mrview = snap.mr.View(id)
	}
	if snap.mc != nil {
		view.mcview = snap.mc.View(id)
	}
	for _, disk := range snap.disklevels(disks[:]) {
		view.dviews = append(view.dviews, disk.View(id))
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
	}
	return cur, err
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

// Set is not allowed
func (view *View) Set(key, value, oldvalue []byte) []byte {
	panic("Set not allowed on view")
}

// Delete is not allowed.
func (view *View) Delete(key, oldvalue []byte, lsm bool) []byte {
	panic("Delete not allowed on view")
}

//---- Exported Read methods

// Get value for key from snapshot.
func (view *View) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	return view.yget(key, value)
}

//---- local methods

func (view *View) getcursor() (cur *Cursor) {
	select {
	case cur = <-view.curchan:
	default:
		cur = &Cursor{}
	}
	view.cursors = append(view.cursors, cur)
	return
}

func (view *View) putcursor(cur *Cursor) {
	select {
	case view.curchan <- cur:
	default: // leave it for GC
	}
}
