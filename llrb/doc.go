// Package llrb implement a self-balancing verions of binary-tree, called,
// LLRB (Left Leaning Red Black).
//
//   * index key, value (value is optional).
//   * each key shall be unique within the index sample-set.
//   * custom memory management
//   * configurable metadata - vbno, access-time, bornseqno, deadseqno, vbuuid.
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
//   and upto 12 optional fields that are configurable.
//
//   1. 64 bit unique vbucket id for the vbucket number.
//   2. 64 bit born-seqno vbucket seqno in which this entry was upserted.
//   3. 64 bit dead-seqno vbucket seqno in which this entry was deleted.
//   4. 64 bit mvalue, pointer to memory refering to value.
//   5. 64 bit fpos, offset into a disk file, also encode the file-id.
//
//   few more to be added...
//
// fpos:
//
//   * value can be kept in memory or backed by disk.
//   * if value is kept in memory, mvalue refers to the memory offset for
//     fetch the entry value.
//   * if value is backed by disk then,
//     fpos[64:59], specifies file-id, ranging from 0-31.
//     fpos[59:], specifies the offset into the identified file.
//   * file-id identifies the level, with 0 being the most recent level.
//
// Hard Limits:
//
//   * maximum vbuckets   - 65535
//   * maximum access     - 2^20 counted in steps of 1.07S for 12 days
//   * maximum key size   - 4096 bytes
//   * maximum value size - 1 Terabyte.
//   * maximum born seqno - (2^64 - 1)
//   * maximum dead seqno - (2^64 - 1)
package llrb
