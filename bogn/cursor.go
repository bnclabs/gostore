package bogn

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/lsm"

// Cursor object maintains an active pointer into index. Use OpenCursor
// on Txn object to create a new cursor.
type Cursor struct {
	txn  *Txn
	view *View

	key     []byte
	value   []byte
	cas     uint64
	deleted bool

	iter  api.Iterator
	iters []api.Iterator
}

func (cur *Cursor) opencursor(
	txn *Txn, view *View, key []byte) (*Cursor, error) {

	cur.txn = txn // will be nil if opened on a view.

	if cur.iters == nil {
		cur.iters = make([]api.Iterator, 0, 8)
	}
	cur.iters = cur.iters[:0]

	var snap *snapshot
	var id uint64
	if txn != nil {
		mwcur, err := txn.mwtxn.OpenCursor(key)
		if err != nil {
			return cur, err
		}
		cur.iters = append(cur.iters, mwcur.YNext)
		snap, id = txn.snap, txn.id
	} else if view != nil {
		mwcur, err := view.mwview.OpenCursor(key)
		if err != nil {
			return cur, err
		}
		cur.iters = append(cur.iters, mwcur.YNext)
		snap, id = view.snap, view.id
	}

	if snap.mr != nil {
		mview := snap.mr.View(id)
		mcur, err := mview.OpenCursor(key)
		if err != nil {
			return cur, err
		}
		cur.iters = append(cur.iters, mcur.YNext)
	}
	if snap.mc != nil {
		mview := snap.mc.View(id)
		mcur, err := mview.OpenCursor(key)
		if err != nil {
			return cur, err
		}
		cur.iters = append(cur.iters, mcur.YNext)
	}
	for _, disk := range snap.disklevels([]api.Index{}) {
		dview := disk.View(id)
		dcur, err := dview.OpenCursor(key)
		if err != nil {
			return cur, err
		}
		cur.iters = append(cur.iters, dcur.YNext)
	}
	if len(cur.iters) > 0 {
		cur.iter = cur.iters[len(cur.iters)-1]
		for i := len(cur.iters) - 2; i >= 0; i-- {
			cur.iter = lsm.YSort(cur.iter, cur.iters[i])
		}
	}

	_, _, _, _, err := cur.YNext(false /*fin*/)
	return cur, err
}

// Key return current key under the cursor. Returned byte slice will
// be a reference to index-key, hence must not be used after
// transaction is commited or aborted.
func (cur *Cursor) Key() (key []byte, deleted bool) {
	return cur.key, cur.deleted
}

// Value return current value under the cursor. Returned byte slice will
// be a reference to value in index, hence must not be used after
// transaction is commited or aborted.
func (cur *Cursor) Value() []byte {
	return cur.value
}

// GetNext move cursor to next entry in snapshot and return its key and
// value. Returned byte slices will be a reference to index entry, hence
// must not be used after transaction is committed or aborted.
func (cur *Cursor) GetNext() (key, value []byte, deleted bool, err error) {
	_, _, _, _, err = cur.YNext(false /*fin*/)
	return cur.key, cur.value, cur.deleted, err
}

// Set is an alias to txn.Set call. The current position of the cursor
// does not affect the set operation.
func (cur *Cursor) Set(key, value, oldvalue []byte) []byte {
	if cur.txn == nil {
		panic("Set not allowed on view-cursor")
	}
	return cur.txn.Set(key, value, oldvalue)
}

// Delete is an alias to txn.Delete call. The current position of the
// cursor does not affect the delete operation.
func (cur *Cursor) Delete(key, oldvalue []byte, lsm bool) []byte {
	if cur.txn == nil {
		panic("Delete not allowed on view-cursor")
	}
	return cur.txn.Delete(key, oldvalue, lsm)
}

// Delcursor deletes the entry at the cursor.
func (cur *Cursor) Delcursor(lsm bool) {
	if cur.txn == nil {
		panic("Delcursor not allowed on view-cursor")
	}
	key, _ := cur.Key()
	cur.txn.Delete(key, nil, lsm)
}

// YNext can be used for lsm-sort.
func (cur *Cursor) YNext(
	fin bool) (key, value []byte, cas uint64, deleted bool, err error) {

	key, value, cur.cas, cur.deleted, err = cur.iter(false /*fin*/)
	cur.key = lib.Fixbuffer(cur.key, int64(len(key)))
	copy(cur.key, key)
	cur.value = lib.Fixbuffer(cur.value, int64(len(value)))
	copy(cur.value, value)
	return cur.key, cur.value, cur.cas, cur.deleted, err
}
