package llrb

import "fmt"
import "unsafe"

var _ = fmt.Sprintf("")

// Cursor object maintains an active pointer into index. Use OpenCursor
// on Txn object to create a new cursor.
type Cursor struct {
	snap    *Snapshot
	ynext   bool
	fpos    []int64
	index   []int
	zblocks [][]byte
}

func (cur *Cursor) opencursor(snapshot *Snapshot, key []byte) *Cursor {
	m := msnap(snap.readat(nil, snap.readm, snap.root))
	index := m.getindex([]uint32{})
	level, fpos := m.getkey(0, blkindex(index), key)
	for level == 0 {
		m = msnap(snap.readat([]byte(m), snap.readm, fpos))
		index = m.getindex(index[:0])
		level, fpos = m.getkey(0, blkindex(index), key)
	}

	z := zsnap(snap.readat(nil, snap.readzs[level-1], fpos))
	index = z.getindex(index[:0])
	idx, _, _, _, _ = z.getkey(0, blkindex(index), key)
	return cur
}

// Key return current key under the cursor. Returned byte slice will
// be a reference to index-key, hence must not be used after
// transaction is commited or aborted.
func (cur *Cursor) Key() (key []byte, deleted bool) {
	if len(cur.stack) == 0 {
		return nil, false
	}
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	return nd.getkey(), nd.isdeleted()
}

// Value return current value under the cursor. Returned byte slice will
// be a reference to value in index, hence must not be used after
// transaction is commited or aborted.
func (cur *Cursor) Value() []byte {
	if len(cur.stack) == 0 {
		return nil
	}
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	return nd.Value()
}

// GetNext move cursor to next entry in snapshot and return its key and
// value. Returned byte slices will be a reference to index entry, hence
// must not be used after transaction is committed or aborted.
func (cur *Cursor) GetNext() (key, value []byte, deleted bool) {
	//fmt.Println(cur.stack)
	if len(cur.stack) == 0 {
		return nil, nil, false
	}
	cur.stack = cur.next(cur.stack)
	if len(cur.stack) == 0 {
		return nil, nil, false
	}
	key, deleted = cur.Key()
	value = cur.Value()
	return
}

// YNext can be used for lambda-sort or lambda-get.
func (cur *Cursor) YNext() (key, value []byte, seqno uint64, deleted bool) {
	if len(cur.stack) == 0 {
		return nil, nil, 0, false
	}
	if cur.ynext == false {
		cur.ynext = true
		ptr := cur.stack[len(cur.stack)-1]
		nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
		key, seqno, deleted = nd.getkey(), nd.getseqno(), nd.isdeleted()
		value = nd.Value()
		return
	}
	cur.stack = cur.next(cur.stack)
	if len(cur.stack) == 0 {
		return nil, nil, 0, false
	}
	ptr := cur.stack[len(cur.stack)-1]
	nd := (*Llrbnode)(unsafe.Pointer(ptr & (^uintptr(0x3))))
	key, seqno, deleted = nd.getkey(), nd.getseqno(), nd.isdeleted()
	value = nd.Value()
	return
}
