package api

type Iterator func() (key, value []byte, seqno uint64, deleted bool)

const (
	// UpsertCmd to insert or update a key,value pair in index.
	// To be used with MutationCmd.
	UpsertCmd byte = iota + 1

	// CasCmd to insert or update a key,value pair in index, if
	// CAS matches. To enforce a fresh insert of key,value pair CAS
	// should be ZERO. Don't use CASCmd on index instance that holds
	// a subset of full dataset. To be used with MutationCmd.
	CasCmd

	// DeleteCmd to delete a key,value pair in index.
	// To be used with MutationCmd.
	DeleteCmd
)

// MutationCmd for write operation. Each instance of MutationCmd is for
// a single write operation in index. Multiple MutationCmd can be
// batched for efficiency.
type MutationCmd struct {
	Cmd        byte
	Cas        uint64
	Key, Value []byte
}

// NodeCallb callback from IndexReader and IndexWriter.
//  * Don't keep any reference to newnd and oldnd:
//  * oldnd can only be read.
//  * newnd can be read or, for Upsert calls, updated.
//  * Other than Mutations and Range API, `i` will always be ZERO.
//  * for IndexReader API, newnd and oldnd will be SAME.
//  * for Delete APIs, newnd and oldnd will be SAME and point to
//    DELETED node.
type NodeCallb func(index Index, i int64, newnd, oldnd Node, err error) bool

// Node accessor methods.
type Node interface {
	NodeSetter
	NodeGetter
}

// NodeGetter to read node attributes.
type NodeGetter interface {
	// Vbno return entry's vbucket number.
	Vbno() (vbno uint16)

	// Access return entry's access timestamp.
	Access() (ts uint64)

	// Bornseqno return vbucket-seqno at which this entry was
	// upserted.
	Bornseqno() (seqno uint64)

	// Deadseqno return vbucket-seqno at which this entry was
	// deleted.
	Deadseqno() (seqno uint64)

	// IsDeleted return true if node is marked as deleted.
	IsDeleted() bool

	// Vbuuid return entry's unique vbucket id.
	Vbuuid() (uuid uint64)

	// Fpos return disk backed position for value. Returned
	// offset points to value on disk encoded as
	// {8byte-len, value-byte-array}
	Fpos() (level byte, offset uint64)

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

	// SetBornseqno to set vbucket-seqno at which this entry was
	// upserted.
	SetBornseqno(seqno uint64) Node

	// SetDeadseqno to set vbucket-seqno at which this entry was
	// deleted.
	SetDeadseqno(seqno uint64) Node

	// SetVbuuid to set unique vbucket id for this entry
	SetVbuuid(uuid uint64) Node

	// SetFpos to set unique vbucket id for this entry
	SetFpos(level byte, offset uint64) Node
}

// IndexMeta defines metadata operations on storage instance. Unless
// specified they are cheap calls and can be used at any point in time.
type IndexMeta interface {
	// Id return index id. Typically, it is human readable and unique.
	ID() string

	// Count return the number of entries indexed.
	Count() int64

	// Isactive return whether index is active or not.
	Isactive() bool

	// Metadata return index/snapshot properties as json encoded
	// object. Purpose of metadata is to save and restore the context
	// of underlying index.
	Metadata() []byte

	// Stats return a set of index statistics.
	Stats() (map[string]interface{}, error)

	// Fullstats return an involved set of index statistics, calling
	// this function may lead to a full table scan.
	Fullstats() (map[string]interface{}, error)

	// Validate check whether index/snapshot is in sane state. Until
	// validate completes on a snapshot, it can be freed. Refer to
	// implementing storage algorithm to know the effects of calling
	// Validate.
	Validate()

	// Log current statistics, if humanize is true log memory
	// information in human readable format. If what is "full", detailed
	// information about instance will be logged.
	Log(what string, humanize bool)
}

// Index interface for managing key,value pairs.
type Index interface {
	IndexMeta

	// RSnapshot return snapshot that shan't be disturbed by
	// subsequent writes. Caller should make sure to call
	// snapshot.Release() once it is done with the snapshot, which
	// also implies that all iterators on the snapshot are closed.
	// If `next` is true block till next snapshot is available on
	// the index.
	RSnapshot(snapch chan IndexSnapshot, next bool) error

	// Clone will do a deep copy of the underlying data structure and
	// return the new copy.
	Clone(name string) (Index, error)

	// Destroy to delete an index and clean up its resources. Calling
	// this method while keeping references to its resources will return
	// error.
	Destroy() error

	IndexReader
	IndexWriter
}

// IndexSnapshot for read-only operation into the index.
type IndexSnapshot interface {
	IndexMeta

	// Refer snapshot before reading, don't hold on to it for long time.
	// Note that RSnapshot() implicitly call a Refer() and there after
	// Refer() should be called once of every new reference.
	Refer()

	// Release snapshot after reading. Should be called after
	// RSnapshot() and for every Refer() calls made on that snapshot.
	Release()

	IndexReader
}

// IndexReader interface for fetching one or more entries from index.
type IndexReader interface {
	// Has checks wether key is present in the index.
	Has(key []byte) bool

	// Get entry for key.
	Get(key []byte, callb NodeCallb) bool

	// Range iterate over entries between lowkey and highkey
	// incl,
	//	"none" - ignore lowkey and highkey while iterating
	//	"low"  - include lowkey but ignore highkey
	//	"high" - ignore lowkey but include highkey
	//	"both" - include both lowkey and highkey
	// If LSM is enabled, Range should include deleted entries.
	Range(lowkey, highkey []byte, incl string, reverse bool, iter NodeCallb)

	// Iterate over entries between lowkey and highkey incl,
	//  "none" - ignore lowkey and highkey while iterating
	//  "low"  - include lowkey but ignore highkey
	//  "high" - ignore lowkey but include highkey
	//  "both" - include both lowkey and highkey
	// If LSM is enabled, Iterate should include deleted entries.
	Iterate(lowkey, highkey []byte, incl string, reverse bool) IndexIterator
}

// IndexIterator interface to pull entries from index over a range
// of low key and high key.
type IndexIterator interface {
	// Next node if present, else nil.
	Next() Node

	// Close iterator, to release resources.
	Close()
}

// IndexWriter interface methods for updating index.
type IndexWriter interface {
	// Upsert a key/value pair.
	Upsert(key, value []byte, callb NodeCallb) error

	// Upsert a key/value pair.
	UpsertCas(key, value []byte, cas uint64, callb NodeCallb) error

	// Delete key from index.
	Delete(key []byte, callb NodeCallb) error

	// Mutations upsert one or more key/value pairs.
	Mutations(cmds []*MutationCmd, callb NodeCallb) error
}
