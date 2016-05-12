package storage

// RangeCallb callback from Range API.
type RangeCallb func(nd Node) bool

// UpsertCallback callback from Upsert API. Don't keep any reference
// to newnd and oldnd:
// * oldnd can only be read.
// * newnd can be read or updated.
type UpsertCallback func(index Index, offset int64, newnd, oldnd Node)

// DeleteCallback callback from Delete API. Don't keep any reference
// deleted node, it is read-only and valid until callback returns.
type DeleteCallback func(index Index, deleted Node)

// Node interface methods to access node attributes.
type Node interface {
	NodeSetter
	NodeGetter
}

// NodeGetter interface methods to get node attributes.
type NodeGetter interface {
	// Vbno return entry's vbucket number.
	Vbno() (vbno uint16)

	// Access return entry's access timestamp.
	Access() (ts uint64)

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

	// Setaccess to set access timestamp for this entry.
	Setaccess(access uint64) Node

	// SetVbuuid to set unique vbucket id for this entry
	SetVbuuid(uuid uint64) Node

	// SetBornseqno to set vbucket-seqno at which this entry was upserted.
	SetBornseqno(seqno uint64) Node

	// SetDeadseqno to set vbucket-seqno at which this entry was deleted.
	SetDeadseqno(seqno uint64) Node
}

// IndexIterator interface to pull entries from index over a range of
// low key and high key.
type IndexIterator interface {
	// Next node if present, else nil.
	Next() Node

	// Prev node if present, else nil.
	Prev() Node

	// Close iterator. Itertors can be concurrently read, but cannot be
	// used with a concurrent write.
	Close()
}

// Index interface for managing key,value pairs.
// TODO: add interface for vector-clock.
type Index interface {
	// index id
	Id() string

	// Count return the number of entries indexed
	Count() int64

	// Isactive return whether the index is active or not.
	Isactive() bool

	// RSnapshot return snapshot that shan't be disturbed by
	// subsequent writes. Caller should make sure to call snapshot.Release()
	// once it is done with the snapshot.
	RSnapshot(snapch chan IndexSnapshot) error

	// Stats return a set of index statistics.
	Stats() (map[string]interface{}, error)

	// Fullstats return an involved set of index statistics.
	Fullstats() (map[string]interface{}, error)

	// Log current statistics, if humanize is true log some or all of the
	// stats in human readable format.
	Log(involved int, humanize bool)

	// Validate check whether index is in sane state.
	Validate()

	// Destroy to delete an index and clean up its resources.
	Destroy() error

	IndexReader
	IndexWriter
}

// IndexSnapshot for read-only operation into the index.
// TBD: add interface for vector-clock.
type IndexSnapshot interface {
	// unique id for snapshot
	Id() string

	// Count return the number of entries indexed
	Count() int64

	// Isactive return whether the index is active or not.
	Isactive() bool

	// Refer() snapshot before reading, don't hold on to it for long time.
	Refer()

	// Release snapshot after reading.
	Release()

	// Validate check whether index is in sane state.
	Validate()

	IndexReader
}

// IndexReader interface for fetching one or more entries from index.
type IndexReader interface {
	// Has checks wether key is present in the index.
	Has(key []byte) bool

	// Get entry for key.
	Get(key []byte) Node

	// Min get entry that sort befor every other entries in the index.
	Min() Node

	// Max get entry that sort after every other entries in the index.
	Max() Node

	// Range iterate over entries between lowkey and highkey
	// incl,
	//	"none" - ignore lowkey and highkey while iterating
	//	"low"  - include lowkey but ignore highkey
	//	"high" - ignore lowkey but include highkey
	//	"both" - include both lowkey and highkey
	Range(lowkey, highkey []byte, incl string, iter RangeCallb)

	// Iterate over entries between lowkey and highkey
	// incl,
	//  "none" - ignore lowkey and highkey while iterating
	//  "low"  - include lowkey but ignore highkey
	//  "high" - ignore lowkey but include highkey
	//  "both" - include both lowkey and highkey
	Iterate(lowkey, highkey []byte, incl string, reverse bool) IndexIterator
}

// IndexWriter interface methods for updating index.
type IndexWriter interface {
	// Upsert a key/value pair.
	Upsert(key, value []byte, callb UpsertCallback) error

	// UpsertMany upsert one or more key/value pairs.
	UpsertMany(keys, values [][]byte, callb UpsertCallback) error

	// DeleteMin delete the last entry in the index.
	DeleteMin(callb DeleteCallback) error

	// Delete the first entry in the index.
	DeleteMax(callb DeleteCallback) error

	// Delete entry specified by key.
	Delete(key []byte, callb DeleteCallback) error
}
