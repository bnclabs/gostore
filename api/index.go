// Package api define types and interfaces common to all storage
// algorithms implemented by this package.
package api

const (
	UpsertCmd byte = iota + 1
	CasCmd
	DelminCmd
	DelmaxCmd
	DeleteCmd
)

type MutationCmd struct {
	Cmd        byte
	Cas        uint64
	Key, Value []byte
}

// NodeCallb callback from IndexReader and IndexWriter.
// * Don't keep any reference to newnd and oldnd:
// * oldnd can only be read.
// * newnd can be read or, for Upsert calls, updated.
// * Other than Mutations and Range API, `i` will always be ZERO.
// * for IndexReader API, newnd and oldnd will be SAME.
// * for Delete APIs, newnd and oldnd will be SAME and point to DELETED node.
type NodeCallb func(index Index, i int64, newnd, oldnd Node, err error) bool

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

	// Bornseqno return vbucket-seqno at which this entry was upserted.
	Bornseqno() (seqno uint64)

	// Deadseqno return vbucket-seqno at which this entry was deleted.
	Deadseqno() (seqno uint64)

	// IsDeleted return true if node is marked as deleted.
	IsDeleted() bool

	// Vbuuid return entry's unique vbucket id.
	Vbuuid() (uuid uint64)

	// Fpos return disk backed position for value. Returned offset points
	// to value on disk encoded as {2byte-len, value-byte-array}
	Fpos() (level byte, offset int64)

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

	// SetFpos to set unique vbucket id for this entry
	SetFpos(level byte, offset uint64) Node

	// SetBornseqno to set vbucket-seqno at which this entry was upserted.
	SetBornseqno(seqno uint64) Node

	// SetDeadseqno to set vbucket-seqno at which this entry was deleted.
	SetDeadseqno(seqno uint64) Node
}

type Clock interface {
	// Update clock upto latest msg.
	Update(msg interface{}) Clock

	// Clone creates a copy of the clock.
	Clone() Clock

	// Less compare wether this clock is less than other clock
	Less(other Clock) bool

	// LessEqual compare wether this clock is less than or equal to the
	// other clock
	LessEqual(other Clock) bool
}

// Index interface for managing key,value pairs.
type Index interface {
	// Id return index id. Typically, it is human readable and unique.
	ID() string

	// Count return the number of entries indexed.
	Count() int64

	// Isactive return whether index is active or not.
	Isactive() bool

	// RSnapshot return snapshot that shan't be disturbed by subsequent writes.
	// Caller should make sure to call snapshot.Release() once it is done with
	// the snapshot.
	RSnapshot(snapch chan IndexSnapshot) error

	// Getclock return current clock attached to the index.
	Getclock() Clock

	// Setclock attaches a new clock to the index.
	Setclock(clock Clock)

	// Stats return a set of index statistics.
	Stats() (map[string]interface{}, error)

	// Fullstats return an involved set of index statistics, calling this
	// function may lead to a full table scan.
	Fullstats() (map[string]interface{}, error)

	// Log current statistics, if humanize is true log some or all of the stats
	// in human readable format.
	Log(involved int, humanize bool)

	// Metadata return index properties as json encoded object.
	Metadata() []byte

	// Validate check whether index is in sane state.
	Validate()

	// Destroy to delete an index and clean up its resources. Calling this
	// method while keeping references to its resources will return error.
	Destroy() error

	IndexReader
	IndexWriter
}

// IndexSnapshot for read-only operation into the index.
type IndexSnapshot interface {
	// Id return index id. Typically, it is human readable and unique.
	ID() string

	// Count return the number of entries indexed.
	Count() int64

	// Isactive return whether the index is active or not.
	Isactive() bool

	// Getclock return the clock at which this snapshot was created.
	Getclock() Clock

	// Refer snapshot before reading, don't hold on to it for long time.
	// Note that RSnapshot() implicitly call a Refer() and there after Refer()
	// should be called once of every new reference.
	Refer()

	// Release snapshot after reading. Should be called after RSnapshot() and
	// for every Refer() calls made on that snapshot.
	Release()

	// Metadata return snapshot properties as json encoded object.
	Metadata() []byte

	// Validate check whether index is in sane state.
	Validate()

	IndexReader
}

// IndexReader interface for fetching one or more entries from index.
type IndexReader interface {
	// Has checks wether key is present in the index.
	Has(key []byte) bool

	// Get entry for key.
	Get(key []byte, callb NodeCallb) bool

	// Min get entry that sort before every other entries in the index.
	Min(callb NodeCallb) bool

	// Max get entry that sort after every other entries in the index.
	Max(callb NodeCallb) bool

	// Range iterate over entries between lowkey and highkey
	// incl,
	//	"none" - ignore lowkey and highkey while iterating
	//	"low"  - include lowkey but ignore highkey
	//	"high" - ignore lowkey but include highkey
	//	"both" - include both lowkey and highkey
	Range(lowkey, highkey []byte, incl string, reverse bool, iter NodeCallb)

	// Iterate over entries between lowkey and highkey
	// incl,
	//  "none" - ignore lowkey and highkey while iterating
	//  "low"  - include lowkey but ignore highkey
	//  "high" - ignore lowkey but include highkey
	//  "both" - include both lowkey and highkey
	Iterate(lowkey, highkey []byte, incl string, reverse bool) IndexIterator
}

// IndexIterator interface to pull entries from index over a range of low key
// and high key.
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

	// DeleteMin delete the last entry in the index.
	DeleteMin(callb NodeCallb) error

	// Delete the first entry in the index.
	DeleteMax(callb NodeCallb) error

	// Delete entry specified by key.
	Delete(key []byte, callb NodeCallb) error

	// Mutations upsert one or more key/value pairs.
	Mutations(cmds []MutationCmd, callb NodeCallb) error
}
