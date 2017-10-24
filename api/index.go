package api

// Getter function, given a key, returns indexed entry.
type Getter func(key, value []byte) (val []byte, cas uint64, del, ok bool)

// Iterator function to iterate on each indexed entry in sort order.
type Iterator func(fin bool) (key, val []byte, seqno uint64, del bool, e error)

// Index defines basic set of index operations that are mandated.
type Index interface {
	// ID is same as the name supplied while creating the LLRB instance.
	ID() string

	// Set a key, value pair in the index, if key is already present, its value
	// will be over-written. Make sure key is not nil. Return old value if
	// oldvalue points to valid buffer.
	Set(key, value, oldvalue []byte) (ov []byte, cas uint64)

	// SetCAS a key, value pair in the index, if CAS is ZERO then key should
	// not be present in the index, otherwise existing CAS should match the
	// supplied CAS. Value will be over-written. Make sure key is not nil.
	// Return old value if oldvalue points to valid buffer.
	SetCAS(key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error)

	// Delete key from index. Key should not be nil, if key found return its
	// value. If lsm is true, then don't delete the node instead mark the node
	// as deleted. Again, if lsm is true but key is not found in index, a new
	// entry will inserted.
	Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64)

	// Get value for key, if value argument points to valid buffer it will, be
	// used to copy the entry's value. Also returns entry's cas, whether entry
	// is marked as deleted by LSM. If ok is false, then key is not found.
	Get(key, value []byte) (v []byte, cas uint64, deleted, ok bool)

	// Scan return a full table iterator.
	Scan() Iterator

	// BeginTxn starts a read-write transaction. All transactions should either
	// be commited or aborted. Transactions must satisfy ACID properties.
	// Returned transactor must be Aborted or Committed.
	BeginTxn(id uint64) Transactor

	// View start a read only transaction, all read operations will be stable
	// on this snapshot until it is Aborted.
	View(id uint64) Transactor

	// Destroy releases all resources held by the index. No other method call
	// are allowed after Destroy.
	Destroy()
}

// Txn transaction definition. Transaction gives a gaurantee of isolation
// and atomicity on the latest snapshot.
type Transactor interface {
	// ID return transaction id.
	ID() uint64

	// Set an entry of key, value pair. The set operation will be remembered as
	// a log entry and applied on the underlying structure during Commit. Not
	// allowed in read-only transactions.
	Set(key, value, oldvalue []byte) []byte

	// Delete key from index. The Delete operation will be remembered as a log
	// entry and applied on the underlying structure during commit. Not
	// allowed in read-only transactions.
	Delete(key, oldvalue []byte, lsm bool) []byte

	// Get value for key from snapshot.
	Get(key, value []byte) (v []byte, deleted, ok bool)

	// OpenCursor open an active cursor inside the index.
	OpenCursor(key []byte) *Cursor

	// Abort transaction, underlying index won't be touched.
	Abort()

	// Commit transaction, commit will block until all write operations under
	// the transaction are successfully applied. Return ErrorRollback if ACID
	// properties are not met while applying the write operations. Transactions
	// are never partially committed. Not allowed in read-only transactions.
	Commit() error
}

// Cursor object maintains an active pointer into index. Use OpenCursor on
// Transactor object to create a new cursor.
type Cursor interface {
	// Set is an alias to txn.Set call. The current position of the cursor does
	// not affect the set operation.
	Set(key, value, oldvalue []byte) []byte

	// Delete is an alias to Transactor.Delete call. The current position of
	// the cursor does not affect the delete operation.
	Delete(key, oldvalue []byte, lsm bool) []byte

	// Delcursor delete the entry at the cursor.
	Delcursor(lsm bool)

	// Key return current key under the cursor. Returned byte slice will be a
	// reference to index-key, hence must not be used after transaction is
	// commited or aborted.
	Key() (key []byte, deleted bool)

	// Value return current value under the cursor. Returned byte slice will be
	// a reference to value in index, hence must not be used after transaction
	// is commited or aborted.
	Value() []byte

	// GetNext move cursor to next entry in snapshot and return its key and
	// value. Returned byte slices will be a reference to index entry, hence
	// must not be used after transaction is committed or aborted.
	GetNext() (key, value []byte, deleted bool, err error)

	// YNext implements Iterator api.
	YNext(fin bool) (key, value []byte, seqno uint64, deleted bool, err error)
}
