package llrb

import "bytes"
import "unsafe"
import "hash/crc32"

import "github.com/prataprc/gostore/lib"

type record struct {
	cmd   byte
	key   []byte
	value []byte
	seqno uint64
	lsm   bool
	next  *record
}

// Txn transaction definition. Transaction gives a gaurantee of isolation and
// atomicity on the latest snapshot.
type Txn struct {
	id       uint64
	rw       bool
	db       interface{}
	snapshot interface{}
	tblcrc32 *crc32.Table
	writes   map[uint32]*record
	cursors  []*Cursor
	recchan  chan *record
	curchan  chan *Cursor
}

const (
	cmdSet byte = iota + 1
	cmdDelete
)

func newtxn(
	id uint64, db, snapshot interface{},
	rch chan *record, cch chan *Cursor) *Txn {

	txn := &Txn{
		id: id, rw: rw, db: db, snapshot: snapshot,
		recchan: rch, curchan: cch,
	}
	if txn.tblcrc32 == nil {
		txn.tblcrc32 = crc32.MakeTable(crc32.IEEE)
	}
	if txn.recchan != nil && txn.writes == nil {
		txn.writes = make(map[uint32]*record)
	}
	if txn.id == 0 {
		switch snap := txn.snapshot.(type) {
		case *LLRB:
			txn.id = (uint64)((uintptr)(unsafe.Pointer(snap.root)))
		case *Snapshot:
			txn.id = (uint64)((uintptr)(unsafe.Pointer(snap.root)))
		}
	}
	return txn
}

//---- Exported Control methods

// ID return transaction id.
func (txn *Txn) ID() uint64 {
	return txn.id
}

// Commit transaction, commit will block until all write operations
// under the transaction are successfully applied. Return
// ErrorRollback if ACID properties are not met while applying the
// write operations.
func (txn *Txn) Commit() error {
	switch db := txn.db.(type) {
	case *LLRB:
		return db.commit(txn)
	case *MVCC:
		return db.commit(txn)
	}
}

// Abort transaction, underlying index won't be touched.
func (txn *Txn) Abort() {
	switch db := txn.db.(type) {
	case *LLRB:
		db.abort(txn)
	case *MVCC:
		db.abort(txn)
	}
}

// OpenCursor open an active cursor inside the index.
func (txn *Txn) OpenCursor(key []byte) *Cursor {
	cur := txn.getcursor().opencursor(key)
	return cur
}

//---- Exported Read methods

// Get value for key from snapshot.
func (txn *Txn) Get(key, value []byte) (v []byte, deleted, ok bool) {
	index := crc32.Checksum(key, txn.tblcrc32)
	head, _ := txn.writes[index]
	_, next := head.get(key)
	if next == nil {
		v, _, deleted, ok = txn.getsnap(key, value)
		return
	} else if next.cmd == cmdDelete {
		return lib.Fixbuffer(v, 0), true, true
	}
	v = lib.Fixbuffer(value, int64(len(next.value)))
	copy(v, next.value)
	return v, false, true
}

//---- Exported Write methods

// Set an entry of key, value pair. The set operation will be remembered
// as a log entry and applied on the underlying structure during Commit.
func (txn *Txn) Set(key, value, oldvalue []byte) []byte {
	var seqno uint64
	var deleted bool

	if txn.recchan == nil {
		panic("Set not allowed on read-only transaction")
	}

	node := txn.getrecord()
	node.cmd, node.key, node.value = cmdSet, key, value
	node.seqno, node.next = 0, nil

	index := crc32.Checksum(key, txn.tblcrc32)
	head, _ := txn.writes[index]
	old, newhead := head.prepend(key, node)
	txn.writes[index] = newhead

	if old != nil {
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(old.value)))
		copy(oldvalue, old.value)
		node.seqno = old.seqno
	} else {
		oldvalue, seqno, deleted, _ = txn.getsnap(key, oldvalue)
		if deleted {
			oldvalue = lib.Fixbuffer(oldvalue, 0)
		}
		node.seqno = seqno
	}
	return oldvalue
}

// Delete key from index. The Delete operation will be remembered as a log
// entry and applied on the underlying structure during commit.
func (txn *Txn) Delete(key, oldvalue []byte, lsm bool) []byte {
	var deleted bool
	var seqno uint64

	if txn.recchan == nil {
		panic("Delete not allowed on read-only transaction")
	}

	node := txn.getrecord()
	node.cmd, node.key = cmdDelete, key
	node.seqno, node.lsm, node.next = 0, lsm, nil
	node.value = lib.Fixbuffer(node.value, 0)

	index := crc32.Checksum(key, txn.tblcrc32)
	head, ok := txn.writes[index]
	old, newhead := head.prepend(key, node)
	txn.writes[index] = newhead
	if old != nil {
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(old.value)))
		copy(oldvalue, old.value)
		node.seqno = old.seqno
	} else {
		oldvalue, seqno, deleted, ok = txn.getsnap(key, oldvalue)
		if deleted || ok == false {
			oldvalue = lib.Fixbuffer(oldvalue, 0)
		}
		node.seqno = seqno
	}
	return oldvalue
}

//---- local methods

func (txn *Txn) getsnap(key, value []byte) ([]byte, uint64, bool, bool) {
	switch snap := txn.snapshot.(type) {
	case *LLRB:
		return snap.Get(key, value)
	case *Snapshot:
		return snap.Get(key, value)
	}
	panic("unreachable code")
}

func (txn *Txn) getrecord() (rec *record) {
	select {
	case rec = <-txn.recchan:
	default:
		rec = &record{}
	}
	return
}

func (txn *Txn) putrecord(rec *record) {
	select {
	case txn.recchan <- rec:
	default: // leave it for GC
	}
}

func (txn *Txn) getcursor() (cur *Cursor) {
	select {
	case cur = <-txn.curchan:
	default:
		cur = &Cursor{txn: txn, stack: make([]uintptr, 32)}
	}
	cur.stack = cur.stack[:0]
	return
}

func (txn *Txn) putcursor(cur *Cursor) {
	select {
	case txn.curchan <- cur:
	default: // leave it for GC
	}
}

func (head *record) get(key []byte) (*record, *record) {
	var parent, next *record
	if head == nil {
		return nil, nil
	}
	next = head
	for bytes.Compare(next.key, key) != 0 {
		if parent, next = next, next.next; next == nil {
			return parent, nil
		}
	}
	return parent, next
}

func (head *record) prepend(key []byte, node *record) (old, newhead *record) {
	if head == nil {
		return nil, node
	}

	var parent *record
	parent, old = head.get(key)
	parent.next, node.next = node, old
	return old, head
}

//---- embed

type txnsmeta struct {
	records   chan *record
	cursors   chan *Cursor
	txncache  chan *Txn
	viewcache chan *View
}

func (meta *txnsmeta) inittxns() {
	maxtxns := 1000 // TODO: no magic number
	meta.records = make(chan *record, maxtxns*5)
	meta.cursors = make(chan *Cursor, maxtxns*2)
	meta.txncache = make(chan *Txn, maxtxns)
	meta.viewcache = make(chan *View, maxtxns)
}

func (meta *txnsmeta) gettxn(id uint64, db, snap interface{}) (txn *Txn) {
	select {
	case txn = <-meta.txncache:
	default:
		txn = newtxn(id, db, snap, meta.records, meta.cursors)
	}
	txn.id, txn.db, txn.snapshot = id, db, snap
	return
}

func (meta *txnsmeta) puttxn(txn *Txn) {
	// if rw tx.writes won't be empty so release the records.
	for index, head := range txn.writes { // free all records in this txn.
		for head != nil {
			next := head.next
			txn.putrecord(head)
			head = next
		}
		delete(txn.writes, index)
	}
	for _, cur := range txn.cursors { // release cursors.
		txn.putcursor(cur)
	}
	txn.cursors = txn.cursors[:0]
	select {
	case meta.txncache <- txn:
	default: // Left for GC
	}
}

func (meta *txnsmeta) getview(id uint64, db, snap interface{}) (view *View) {
	select {
	case view = <-meta.viewcache:
	default:
		view = newview(id, snap, meta.cursors)
	}
	view.id, view.snapshot = id, snap
	return
}

func (meta *txnsmeta) putview(view *View) {
	for _, cur := range view.cursors { // release cursors.
		view.putcursor(cur)
	}
	view.cursors = view.cursors[:0]
	select {
	case meta.viewcache <- view:
	default: // Left for GC
	}
}
