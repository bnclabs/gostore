package llrb

import "github.com/prataprc/gostore/api"

// View transaction definition. Read only version of Txn.
type View struct {
	id       uint64
	snapshot interface{}
	cursors  []*Cursor
	curchan  chan *Cursor
}

func newview(id uint64, snapshot interface{}, cch chan *Cursor) *View {
	view := &View{id: id, snapshot: snapshot, curchan: cch}
	return view
}

//---- Exported Control methods

// ID return transaction id.
func (view *View) ID() uint64 {
	return view.id
}

// OpenCursor open an active cursor inside the index.
func (view *View) OpenCursor(key []byte) (api.Cursor, error) {
	cur := view.getcursor().opencursor(nil, view.snapshot, key)
	return cur, nil
}

// Abort view, must be called once done with the view.
func (view *View) Abort() {
	switch snap := view.snapshot.(type) {
	case *LLRB:
		snap.abortview(view)
	case *mvccsnapshot:
		snap.abortview(view)
	}
}

// Set is not allowed
func (view *View) Set(key, value, oldvalue []byte) []byte {
	panic("Set not allowed on view")
}

// Delete is not allowed.
func (view *View) Delete(key, oldvalue []byte, lsm bool) []byte {
	panic("Delete not allowed on view")
}

// Commit not allowed.
func (view *View) Commit() error {
	panic("Commit not allowed on view")
}

//---- Exported Read methods

// Get value for key from snapshot.
func (view *View) Get(key, value []byte) (v []byte, deleted, ok bool) {
	v, _, deleted, ok = view.getonsnap(key, value)
	return
}

//---- local methods

func (view *View) getonsnap(key, value []byte) ([]byte, uint64, bool, bool) {
	switch snap := view.snapshot.(type) {
	case *LLRB:
		return snap.get(key, value)
	case *mvccsnapshot:
		return snap.get(key, value)
	}
	panic("unreachable code")
}

func (view *View) getcursor() (cur *Cursor) {
	select {
	case cur = <-view.curchan:
	default:
		cur = &Cursor{stack: make([]uintptr, 32)}
	}
	cur.stack = cur.stack[:0]
	view.cursors = append(view.cursors, cur)
	return
}

func (view *View) putcursor(cur *Cursor) {
	select {
	case view.curchan <- cur:
	default: // leave it for GC
	}
}
