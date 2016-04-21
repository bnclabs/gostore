// Package storage implement a collection of indexing and persistance
// algorithm.
//
// LLRB:
//
//   * index key, value (value is optional).
//   * each key shall be unique within the index sample-set.
//   * custom memory management
//   * copy on GC to control memory fragmentation.
//   * configurable metadata - like vbno, bornseqno, deadseqno, vbuuid etc..
//   * in single-threaded configuration, reads and writes are serialized.
//   * supports multi-version-concurrency-control, where writes are
//     serialized, even if there are concurrent writers, and there can be
//     zero or more concurrent readers.
//
// metadata fields are part of index entry, and describes them with:
//
//   a. 16 bit vbucket-number virtual bucket for the key.
//   b. 20 bit access time bits time.Now()[50:30].
//
//   and upto 12 optional fields that are configured.
//
//   1. 64 bit unique vbucket id for the vbucket number a.
//   2. 64 bit born-seqno vbucket seqno in which this entry was upserted.
//   3. 64 bit dead-seqno vbucket seqno in which this entry was deleted.
//   4. 64 bit mvalue either pointer to memory or fpos to disk to pick value.
//
//   few more to be added...
//
// mvalue:
//
//   * value can be kept in memory or backed by disk.
//   * if value is kept in memory mvalue[:3] describes the memory offset to
//     fetch the entry value.
//   * if value is backed by disk then mvalue[2:], whose value might be 1-7
//     will pick a file that contains the value and mvalue[:3] describes the
//     file-position to fetch the entry from file.
//
// llrb hard limits:
//
//   * maximum vbuckets   - 65535
//   * maximum access     - 2^20 counted in steps of 1.07S for 12 days
//   * maximum key size   - 4096 bytes
//   * maximum born seqno - (2^64 - 1)
//   * maximum dead seqno - (2^64 - 1)
package storage
