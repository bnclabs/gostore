package llrb

import "bytes"
import "hash/crc32"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"

// Txn transaction definition. Transaction gives a gaurantee of isolation and
// atomicity on the latest snapshot.
type Txn struct {
	id       uint64
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
		id: id, db: db, snapshot: snapshot,
		recchan: rch, curchan: cch,
	}
	if txn.tblcrc32 == nil {
		txn.tblcrc32 = crc32.MakeTable(crc32.IEEE)
	}
	if txn.recchan != nil && txn.writes == nil {
		txn.writes = make(map[uint32]*record)
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
// write operations. Transactions are never partially committed.
func (txn *Txn) Commit() error {
	switch db := txn.db.(type) {
	case *LLRB:
		return db.commit(txn)
	case *MVCC:
		return db.commit(txn)
	}
	panic("unreachable code")
}

// Abort transaction, underlying index won't be touched.
func (txn *Txn) Abort() {
	switch db := txn.db.(type) {
	case *LLRB:
		db.aborttxn(txn)
	case *MVCC:
		db.aborttxn(txn)
	}
}

// OpenCursor open an active cursor inside the index.
func (txn *Txn) OpenCursor(key []byte) (api.Cursor, error) {
	cur := txn.getcursor().opencursor(txn, txn.snapshot, key)
	return cur, nil
}

//---- Exported Read methods

// Get value for key from snapshot.
func (txn *Txn) Get(key, value []byte) (v []byte, deleted, ok bool) {
	index := crc32.Checksum(key, txn.tblcrc32)
	head, _ := txn.writes[index]
	_, next := head.get(key)
	if next == nil {
		v, _, deleted, ok = txn.getonsnap(key, value)
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

	node := txn.getrecord()
	node.cmd, node.key, node.value = cmdSet, key, value
	node.seqno, node.next = 0, nil

	index := crc32.Checksum(key, txn.tblcrc32)
	head, _ := txn.writes[index]
	old, newhead := head.prepend(key, node)
	txn.writes[index] = newhead

	if old != nil {
		if oldvalue != nil {
			oldvalue = lib.Fixbuffer(oldvalue, int64(len(old.value)))
			copy(oldvalue, old.value)
		}
		node.seqno = old.seqno
	} else {
		oldvalue, seqno, _, _ = txn.getonsnap(key, oldvalue)
		node.seqno = seqno
	}
	return oldvalue
}

// Delete key from index. The Delete operation will be remembered as a log
// entry and applied on the underlying structure during commit.
func (txn *Txn) Delete(key, oldvalue []byte, lsm bool) []byte {
	var seqno uint64

	node := txn.getrecord()
	node.cmd, node.key = cmdDelete, key
	node.seqno, node.lsm, node.next = 0, lsm, nil
	node.value = lib.Fixbuffer(node.value, 0)

	index := crc32.Checksum(key, txn.tblcrc32)
	head, _ := txn.writes[index]
	old, newhead := head.prepend(key, node)
	txn.writes[index] = newhead
	if old != nil {
		if oldvalue != nil {
			oldvalue = lib.Fixbuffer(oldvalue, int64(len(old.value)))
			copy(oldvalue, old.value)
		}
		node.seqno = old.seqno
	} else {
		oldvalue, seqno, _, _ = txn.getonsnap(key, oldvalue)
		node.seqno = seqno
	}
	return oldvalue
}

//---- local methods

func (txn *Txn) getonsnap(key, value []byte) ([]byte, uint64, bool, bool) {
	switch snap := txn.snapshot.(type) {
	case *LLRB:
		deleted, seqno := false, uint64(0)
		nd, ok := snap.getkey(snap.getroot(), key)
		if ok {
			if value != nil {
				val := nd.Value()
				value = lib.Fixbuffer(value, int64(len(val)))
				copy(value, val)
			}
			seqno, deleted = nd.getseqno(), nd.isdeleted()
		} else if value != nil {
			value = lib.Fixbuffer(value, 0)
		}
		return value, seqno, deleted, ok

	case *mvccsnapshot:
		return snap.get(key, value)
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
		cur = &Cursor{stack: make([]uintptr, 32)}
	}
	cur.stack = cur.stack[:0]
	txn.cursors = append(txn.cursors, cur)
	return
}

func (txn *Txn) putcursor(cur *Cursor) {
	select {
	case txn.curchan <- cur:
	default: // leave it for GC
	}
}

type record struct {
	cmd   byte
	key   []byte
	value []byte
	seqno uint64
	lsm   bool
	next  *record
}

func (head *record) get(key []byte) (*record, *record) {
	var parent, next *record
	if head == nil {
		return nil, nil
	}
	next = head
	for next != nil && bytes.Compare(next.key, key) != 0 {
		parent, next = next, next.next
	}
	return parent, next
}

func (head *record) prepend(key []byte, node *record) (old, newhead *record) {
	if head == nil {
		return nil, node
	}

	parent, old := head.get(key)
	if parent == nil {
		node.next = old
		return old, node
	}
	parent.next, node.next = node, old
	return old, head
}
