/*
Package malloc supplies threadsafe concurrent memory allocator useful
for MVCC friendly storage data structures.

 * Work best when memory behaviour is known apriori.
 * Memory is allocated in pools, of several Megabytes, where each
   pool manages several memory-chunks of same size.
 * Once a pool block is allocated from OS, it is not automatically
   given back to OS. Pools are freed only when the entire arena
   is released.
 * There is no pointer re-write, if copying garbage collector is
   necessary it can be implemented on top of this implementation.
 * Memory-chunks allocated by this package will always be 8-byte
   aligned.

Arena is a bucket space of memory, with a maximum capacity, that
is empty to begin with and starts filling up as and when new
allocations are requested by application. For performance reasons
arena allocate memory from OS in large blocks, called pool,
where each pool contains several memory-chunks of same size.
*/
package malloc
