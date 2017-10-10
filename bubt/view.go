package bubt

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
func (view *View) OpenCursor(key []byte) (*Cursor, error) {
	cur, err := view.getcursor().opencursor(view.snap, key)
	if err != nil {
		return nil, err
	}
	return cur, nil
}

// Abort view, must be called once done with the view.
func (view *View) Abort() {
	for _, cur := range view.cursors {
		view.putcursor(cur)
	}
	view.snap.abortview(view)
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to copy the
// entry's value. Also return whether entry is marked as deleted by LSM.
// If ok is false, then key is not found.
func (view *View) Get(key, value []byte) (v []byte, deleted, ok bool) {
	v, _, deleted, ok = view.snap.Get(key, value)
	return v, deleted, ok
}

//---- local methods

func (view *View) getcursor() (cur *Cursor) {
	select {
	case cur = <-view.snap.curcache:
	default:
		cur = &Cursor{
			snap:   view.snap,
			fposs:  make([]int64, len(view.snap.readzs)),
			zblock: make([]byte, view.snap.zblocksize),
		}
	}
	cur.ynext, cur.finished, cur.index = false, false, 0
	for i := range cur.fposs {
		cur.fposs[i] = -1
	}
	view.cursors = append(view.cursors, cur)
	return
}

func (view *View) putcursor(cur *Cursor) {
	select {
	case view.snap.curcache <- cur:
	default: // leave it for GC
	}
}
