package bubt

// View transaction definition. Read only version of Txn.
type View struct {
	id      uint64
	snap    *Snapshot
	cursors []*Cursor
}

//---- Exported Control methods

// ID return transaction id.
func (view *View) ID() uint64 {
	return view.id
}

// OpenCursor open an active cursor inside the index.
func (view *View) OpenCursor(key []byte) *Cursor {
	cur := view.getcursor().opencursor(nil, key)
	return cur
}

// Abort view, must be called once done with the view.
func (view *View) Abort() {
	view.snap.abortview(view)
}

//---- Exported Read methods

// Get value for key from snap.
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
