// Package malloc supplies custom memory management for in-memory data
// structures. Note that Types and Functions exported by this package are not
// thread safe.
//
// Arena is a single chunk of memory that will be utilized by the
// algorithm/data-structure during its existence. To improve memory
// utilization arenas are divided into pools of fixed sized slabs, where each
// pool will supply memory blocks of same size. Arenas can be created with
// following parameters:
//
//   capacity  : size of arena in bytes.
//   minblock  : blocks less than minblock sizes cannot be allocated.
//   maxblock  : blocks greater than maxblock sizes cannot be allocated.
//   pcapacity : pool's capacity, in this arena, cannot exceed this limit.
//   maxpools  : maximum number of pool-sizes allowed.
//   maxchunks : maximum number of block-chunks allowed in a pool.
//   allocator : allocator algorithm to use supports `flist` or `fbit`.
package malloc
