package storage

// Node interface methods to access node attributes.
type Node interface {
	NodeSetter
	NodeGetter
}

// NodeGetter interface methods to get node attributes.
type NodeGetter interface {
	// Vbno return entry's vbucket number.
	Vbno() (vbno uint16)

	// Vbuuid return entry's unique vbucket id.
	Vbuuid() (uuid uint64)

	// Bornseqno return vbucket-seqno at which this entry was upserted.
	Bornseqno() (seqno uint64)

	// Deadseqno return vbucket-seqno at which this entry was deleted.
	Deadseqno() (seqno uint64)

	// Key return entry key as byte slice.
	Key() (key []byte)

	// Value return entry value, if present, as byte slice.
	Value() (value []byte)
}

// NodeSetter interface methods to set node attributes.
type NodeSetter interface {
	// Setvbno to set the vbucket-number for this entry.
	Setvbno(vbno uint16) Node

	// SetVbuuid to set unique vbucket id for this entry
	SetVbuuid(uuid uint64) Node

	// SetBornseqno to set vbucket-seqno at which this entry was upserted.
	SetBornseqno(seqno uint64) Node

	// SetDeadseqno to set vbucket-seqno at which this entry was deleted.
	SetDeadseqno(seqno uint64)
}

// Writer interface methods for updating index data structure.
type Writer interface {
	// Upsert a key/value pair, if key is already present update its value
	// and return the old value, else return nil.
	Upsert(key, value []byte) []byte

	// Delete a key entry, if present return the old value.
	Delete(key []byte) []byte

	// Delete the first entry in the sorted index, and if index is non-empty
	// return the corresponding key/value pair.
	DeleteMax() (key, value []byte)

	// Delete the last entry in the sorted index, and if index is non-empty
	// return the corresponding key/value pair.
	DeleteMin() (key, value []byte)
}

// Reader interface for fetching one or more entries from index data
// structure.
type Reader interface {
	// Count return the number entries in the sorted index.
	Count() int64
	Get(key []byte) []byte
	Has(key []byte) bool
	Max() (key, value []byte)
	Min() (key, value []byte)
	Range(lowkey, highkey []byte, incl string, iter KVIterator)
}

type Index interface {
	Count() int64
	RSnapshot() Index
}
