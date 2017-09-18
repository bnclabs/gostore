// Package llrb implement a self-balancing verions of binary-tree, called,
// LLRB (Left Leaning Red Black).
//
//   * Index key, value (value is optional).
//   * Each key shall be unique within the index sample-set.
//   * Configurable memory backend.
//   * In single-threaded settings, reads and writes are serialized.
//
// MVCC instances support serialized writes and concurrent reads.
//
package llrb
