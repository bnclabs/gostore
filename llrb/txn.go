package llrb

import "bytes"
import "unsafe"
import "hash/crc32"

import "github.com/prataprc/gostore/lib"

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

type record struct {
	cmd   byte
	key   []byte
	value []byte
	seqno uint64
	lsm   bool
	next  *record
}

func newtxn(
	id uint64, db, snapshot interface{},
	rch chan *record, cch chan *Cursor, rw bool) *Txn {
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
		case *LLRB1:
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

func (txn *Txn) Validate() {
	switch snap := txn.snapshot.(type) {
	case *LLRB1:
		root := (*Llrbnode1)(snap.root)
		validatellrb(root, snap.Stats(), snap.logprefix)
	}
	panic("unreachable code")
}

func (txn *Txn) Commit() {
	switch db := txn.db.(type) {
	case *LLRB1:
		db.commit(txn)
	}
}

func (txn *Txn) Abort() {
	switch db := txn.db.(type) {
	case *LLRB1:
		db.abort(txn)
	}
}

func (txn *Txn) OpenCursor(key []byte) *Cursor {
	cur := txn.getcursor().opencursor(key)
	return cur
}

//---- Exported Read methods

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
	switch db := txn.snapshot.(type) {
	case *LLRB1:
		return db.Get(key, value)
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
