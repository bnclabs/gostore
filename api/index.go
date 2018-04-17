package api

// Getter function, given a key, returns indexed entry.
type Getter func(key, value []byte) (val []byte, cas uint64, del, ok bool)

// Iterator function to iterate on each indexed entry in sort order.
type Iterator func(fin bool) (key, val []byte, seqno uint64, del bool, e error)

// EntryIterator function to iterate on each indexed entry in sort order.
// Returned IndexEntry is valid only till next call to the EntryIterator.
type EntryIterator func(fin bool) IndexEntry

// Index defines basic set of index operations that are mandated.
type Index interface {
	// ID is same as the name supplied while creating the index instance.
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

	// Get value for key, if value argument points to valid buffer it will be
	// used to copy the entry's value. Also return entry's cas and whether entry
	// is marked deleted. If ok is false, then key is not found.
	Get(key, value []byte) (v []byte, cas uint64, deleted, ok bool)

	// Scan return a full table iterator.
	Scan() Iterator

	// ScanEntries return a full table iterator.
	ScanEntries() EntryIterator

	// BeginTxn starts a read-write transaction. Transactions must
	// satisfy ACID properties. Finally all transactor objects must
	// be Aborted or Committed.
	BeginTxn(id uint64) Transactor

	// View start a read only transaction, all read operations will be on
	// a stable snapshot until it is Aborted. Finally all view objects must
	// be Aborted.
	View(id uint64) Transactor

	// Close will release all temporary resources held by the index. No other
	// method call, except Destroy, are allowed after Close.
	Close()

	// Destroy releases all temporary and permanent resources held by the
	// index. No other method call are allowed after Destroy.
	Destroy()
}

// Transaction definition. Transactions give a gaurantee of isolation
// and atomicity on the latest snapshot. For iteration, use OpenCursor().
// If Transactor is created using View() API, it is treated as read-only
// transaction hence write methods like Set, Delete, Delcursor, Commit
// are not allowed.
type Transactor interface {
	// ID return transaction id.
	ID() uint64

	// Set an entry of key, value pair. The set operation will be remembered as
	// a log entry and applied on the underlying structure during Commit.
	Set(key, value, oldvalue []byte) []byte

	// Delete key from index. The Delete operation will be remembered as a log
	// entry and applied on the underlying structure during commit.
	Delete(key, oldvalue []byte, lsm bool) []byte

	// Get value for key from snapshot.
	Get(key, value []byte) (v []byte, cas uint64, deleted, ok bool)

	// OpenCursor open an active cursor inside the index.
	OpenCursor(key []byte) (Cursor, error)

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
	// Set is an alias to txn.Set call. The current position of the cursor
	// does not affect the set operation.
	Set(key, value, oldvalue []byte) []byte

	// Delete is an alias to Transactor.Delete call. The current position of
	// the cursor does not affect the delete operation.
	Delete(key, oldvalue []byte, lsm bool) []byte

	// Delcursor delete the entry at the cursor.
	Delcursor(lsm bool)

	// Key return current key under the cursor. Returned byte slice will be a
	// reference to index-key, hence must not be used after transaction is
	// commited or aborted.
	// TODO: should we add `cas uint64` as part of return values ?
	Key() (key []byte, deleted bool)

	// Value return current value under the cursor. Returned byte slice will
	// be a reference to value in index, hence must not be used after
	// transaction is commited or aborted.
	Value() []byte

	// GetNext move cursor to next entry in snapshot and return its key and
	// value. Returned byte slices will be a reference to index entry, hence
	// must not be used after transaction is committed or aborted.
	GetNext() (key, value []byte, deleted bool, err error)

	// YNext implements Iterator api, to iterate over the index. Typically
	// used for lsm-sort.
	YNext(fin bool) (key, val []byte, seqno uint64, deleted bool, err error)
}

// IndexEntry interface can be used to access individual fields in an entry.
type IndexEntry interface {
	// ID return the index id.
	ID() string

	// Key return entry's key, mutation seqno and whether entry is marked
	// deleted. If entry is last entry, error will be io.EOF.
	Key() (key []byte, seqno uint64, deleted bool, err error)

	// Value return entry's value. If entry is empty, returns nil.
	Value() []byte

	// Valueref returns reference to value, if value is stored in separate
	// file, else vpos will be -1.
	Valueref() (valuelen uint64, vpos int64)
}

// Disksnapshot provides read-only API to fetch snapshot information.
type Disksnapshot interface {
	// Appdata return application data associated with this snapshot.
	Appdata() []byte
}
