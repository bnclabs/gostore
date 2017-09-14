// TODO: update documentation.
// Package llrb implement a self-balancing verions of binary-tree, called,
// LLRB (Left Leaning Red Black).
//
//   * Index key, value (value is optional).
//   * Each key shall be unique within the index sample-set.
//   * Configurable memory backend.
//   * Metadata - vbno, access-time, bornseqno, deadseqno, vbuuid.
//   * In single-threaded settings, reads and writes are serialized.
//   * Supports multi-version-concurrency-control, where writes are
//     serialized even if there are concurrent writers, and there can be
//     zero or more concurrent readers.
//
// Metadata fields are part of index entry, and describes them with:
//
//   a. 16 bit vbucket-number virtual bucket for the key.
//   b. 20 bit access time bits time.Now()[50:30].
//
//   and upto 12 optional fields that can be configured via settings.
//
//   1. 64 bit unique vbucket id.
//   2. 64 bit born-seqno vbucket seqno in which this entry was upserted.
//   3. 64 bit dead-seqno vbucket seqno in which this entry was deleted.
//   4. 64 bit mvalue, pointer to memory refering to value.
//   5. 64 bit fpos, offset into a disk file, also encode the file-id.
//
//   few more to be added...
//
// Note on `fpos`:
//
//   * Value can be kept in memory or backed by disk.
//   * If value is kept in memory, mvalue refers to the memory offset for
//     fetching the entry value.
//   * If value is backed by disk then,
//     fpos[64:59], specifies file-id, ranging from 0-31.
//     fpos[59:], specifies the offset into the identified file.
//   * File-id identifies the level, with 0 being the most recent level.
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
