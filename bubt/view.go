package llrb

// View transaction definition. Read only version of Txn.
type View struct {
	id      uint64
	snap    *Snapshot
	cursors []*Cursor
	curchan chan *Cursor
}

func newview(id uint64, snap *Snapshot, cch chan *Cursor) *View {
	view := &View{id: id, snap: snap, curchan: cch}
	return view
}

//---- Exported Control methods

// ID return transaction id.
func (view *View) ID() uint64 {
	return view.id
}

// OpenCursor open an active cursor inside the index.
func (view *View) OpenCursor(key []byte) *Cursor {
	cur := view.getcursor().opencursor(nil, view.snap, key)
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
	case cur = <-view.curchan:
	default:
		cur = &Cursor{
			snap:    view.snap,
			ynext:   false,
			fpos:    make([]int64, len(view.snap.readzs)),
			index:   make([]int64, 256),
			zblocks: make([][]byte, len(view.snap.readzs)),
		}
		for i := range cur.zblocks {
			cur.zblocks[i] = make([]byte, view.snap.zblocksize)
		}
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
