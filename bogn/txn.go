package bogn

import "github.com/prataprc/gostore/api"

// Txn transaction definition. Transaction gives a gaurantee of isolation and
// atomicity on the latest snapshot.
type Txn struct {
	id    uint64
	bogn  *Bogn
	snap  *snapshot
	mwtxn api.Transactor
	yget  api.Getter

	cursors []*Cursor
	curchan chan *Cursor
	gets    []api.Getter
}

const (
	cmdSet byte = iota + 1
	cmdDelete
)

func newtxn(id uint64, bogn *Bogn, snap *snapshot, cch chan *Cursor) *Txn {
	txn := &Txn{
		id: id, bogn: bogn, snap: snap,
		cursors: make([]*Cursor, 0, 8), curchan: cch,
		gets: make([]api.Getter, 32),
	}
	txn.mwtxn = snap.mw.BeginTxn(id)
	txn.yget = snap.txnyget(txn.mwtxn, txn.gets)
	return txn
}

//---- Exported Control methods

// ID return transaction id.
func (txn *Txn) ID() uint64 {
	return txn.id
}

// OpenCursor open an active cursor inside the index.
func (txn *Txn) OpenCursor(key []byte) (api.Cursor, error) {
	cur, err := txn.getcursor().opencursor(txn, nil, key)
	if err != nil {
		txn.putcursor(cur)
	}
	return cur, err
}

// Commit transaction, commit will block until all write operations
// under the transaction are successfully applied. Return
// ErrorRollback if ACID properties are not met while applying the
// write operations. Transactions are never partially committed.
func (txn *Txn) Commit() error {
	return txn.bogn.commit(txn)
}

// Abort transaction, underlying index won't be touched.
func (txn *Txn) Abort() {
	txn.bogn.aborttxn(txn)
}

//---- Exported Read methods

// Get value for key from snapshot.
func (txn *Txn) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	return txn.yget(key, value)
}

//---- Exported Write methods

// Set an entry of key, value pair. The set operation will be remembered
// as a log entry and applied on the underlying structure during Commit.
func (txn *Txn) Set(key, value, oldvalue []byte) []byte {
	return txn.mwtxn.Set(key, value, oldvalue)
}

// Delete key from index. The Delete operation will be remembered as a log
// entry and applied on the underlying structure during commit.
func (txn *Txn) Delete(key, oldvalue []byte, lsm bool) []byte {
	return txn.mwtxn.Delete(key, oldvalue, lsm)
}

//---- local methods

func (txn *Txn) getcursor() (cur *Cursor) {
	select {
	case cur = <-txn.curchan:
	default:
		cur = &Cursor{}
	}
	txn.cursors = append(txn.cursors, cur)
	return
}

func (txn *Txn) putcursor(cur *Cursor) {
	select {
	case txn.curchan <- cur:
	default: // leave it for GC
	}
}
