package storage

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
