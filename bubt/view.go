package bubt

import "github.com/prataprc/gostore/api"

// View read only transaction instance.
type View struct {
	id      uint64
	snap    *Snapshot
	cursors []*Cursor
}

//---- Exported Control methods

// ID return view-transaction id.
func (view *View) ID() uint64 {
	return view.id
}

// OpenCursor open an active cursor, point at key, inside the index.
func (view *View) OpenCursor(key []byte) (api.Cursor, error) {
	buf := view.snap.getreadbuffer()
	cur, err := view.getcursor().opencursor(view.snap, key, buf)
	return cur, err
}

// Set not allowed.
func (view *View) Set(key, value, oldvalue []byte) []byte {
	panic("Set not allowed on view")
}

// Delete not allowed.
func (view *View) Delete(key, oldvalue []byte, lsm bool) []byte {
	panic("Delete not allowed on view")
}

// Commit not allowed.
func (view *View) Commit() error {
	panic("Commit not allowed on view")
}

// Abort view, must be called once done with the view.
func (view *View) Abort() {
	view.snap.abortview(view)
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to copy the
// entry's value. Also return whether entry is marked as deleted by LSM.
// If ok is false, then key is not found.
func (view *View) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	v, cas, deleted, ok = view.snap.Get(key, value)
	return v, cas, deleted, ok
}

//---- local methods

func (view *View) getcursor() (cur *Cursor) {
	select {
	case cur = <-view.snap.curcache:
	default:
		cur = &Cursor{
			snap:  view.snap,
			fposs: make([]int64, len(view.snap.readzs)),
		}
	}
	cur.ynext = false
	cur.index, cur.finished = 0, false
	for i := range cur.fposs {
		cur.fposs[i] = -1
	}
	view.cursors = append(view.cursors, cur)
	return
}

func (view *View) putcursor(cur *Cursor) {
	view.snap.putreadbuffer(cur.buf)
	select {
	case view.snap.curcache <- cur:
	default: // leave it for GC
	}
}
