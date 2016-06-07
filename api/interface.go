// Package api define types and interfaces common to all storage
// algorithms implemented by this package.
package api

import "unsafe"

// DeleteCallback callback from Delete API. Don't keep any reference to
// deleted node, it is read-only and valid until callback returns.
type DeleteCallback func(index Index, deleted Node)

// UpsertCallback callback from Upsert API. Don't keep any reference
// to newnd and oldnd:
//   * oldnd can only be read.
//   * newnd can be read or updated.
type UpsertCallback func(index Index, offset int64, newnd, oldnd Node)

// NodeCallb callback from Get,Min,Max API.
type NodeCallb func(nd Node) bool

// RangeCallb callback from Range API.
type RangeCallb func(nd Node) bool

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

	// Stats return a set of index statistics.
	Stats() (map[string]interface{}, error)

	// Fullstats return an involved set of index statistics, calling this
	// function may lead to a full table scan.
	Fullstats() (map[string]interface{}, error)

	// Log current statistics, if humanize is true log some or all of the stats
	// in human readable format.
	Log(involved int, humanize bool)

	// Validate check whether index is in sane state.
	Validate()

	// Destroy to delete an index and clean up its resources. Calling this
	// method while keeping references to its resources is not advised.
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

	// Refer snapshot before reading, don't hold on to it for long time.
	// Note that RSnapshot() implicitly call a Refer() and there after Refer()
	// should be called once of every new reference.
	Refer()

	// Release snapshot after reading. Should be called after RSnapshot() and
	// for every Refer() calls made on that snapshot.
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
	Range(lowkey, highkey []byte, incl string, reverse bool, iter RangeCallb)

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

// Mallocer interface for custom memory management.
type Mallocer interface {
	// Allocate a chunk of `n` bytes from `pool`.
	Alloc(n int64) (ptr unsafe.Pointer, pool Mallocer)

	// Free pointer back to the pool.
	Free(ptr unsafe.Pointer)

	// Memory return memory allocated from OS and overhead of managing it.
	Memory() (overhead, useful int64)

	// Allocated return memory allocated from `useful` memory.
	Allocated() (allocated int64)

	// Available return allocatable memory from arena.
	Available() (available int64)

	// Chunksizes allocatable chunk-sizes.
	Chunksizes() (sizes []int64)

	// Utilization map of chunk-size and its pool utilization
	Utilization() ([]int, []float64)

	// Release arena, all its pools and resources.
	Release()

	// Chunksize alias for alloc:Mpooler{} interface.
	Chunksize() int64

	// Less alias for alloc:Mpooler interface.
	Less(pool interface{}) bool

	// Allocate alias for alloc:Mpooler interface.
	Allocchunk() (ptr unsafe.Pointer, ok bool)
}
