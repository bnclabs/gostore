// Package malloc supplies custom memory management for storage
// algorithms, with a limited scope:
//
//  * Types and Functions exported by this package are not thread safe.
//  * Work best when memory behaviour is known apriori.
//  * Memory is allocated in pools, of several Megabytes, where each
//    pool manages several memory-chunks of same size.
//  * Once a pool block is allocated from OS, it is not automatically
//    given back to OS. Pools are freed only when the entire arena
//    is Released.
//  * There is no pointer re-write, if copying garbage collector is
//    necessary it can be implemented on top of this implementation.
//  * Memory-chunks allocated by this package will always be 64-bit
//    aligned.
//
// Arena is a bucket space of memory, with a maximum capacity, that
// is empty to begin with and starts filling up as and when new
// allocations are requested by application. For performance reasons
// arena allocate memory from OS in large blocks, called pool,
// where each pool contains several memory-chunks of same size.
//
// Applications are allowed to allocate memory chunks whose size fall
// between a pre-configured minimum chunk size and maximum chunk size,
// supplied while instantiating a new arena.
package malloc

// TODO: Right now memory is given back to OS only when the arena is released.
// Implement arena API to detect and free memory at pool level granularity.
