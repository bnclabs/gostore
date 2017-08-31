Allocator backend
=================

Malloc supplies different allocator backend based on application
behaviour:

* `flist` uses custom allocator built for gostore. Default allocator.
* `malloc` uses OS standard library, Ref #33.
* `jemalloc` from FreeBSD [jemalloc.net](http://jemalloc.net).
   Ref #34.

FLIST allocator
===============

Malloc supplies custom memory management for storage algorithms,
with a limited scope:

* Types and Functions exported by this allocator are not thread safe.
* Work best when memory behaviour is known apriori.
* Memory is allocated in pools, of several Megabytes, where each
  pool manages several memory-chunks of same size.
* Once a pool block is allocated from OS, it is not automatically
  given back to OS. Pools are freed only when the entire arena
  is Released.
* There is no pointer re-write, if copying garbage collector is
  necessary it can be implemented on top of this implementation.
* Larger overhead when arena-capacity is less but chunks are large.

Applications are allowed to allocate memory chunks whose size fall
between a pre-configured minimum chunk size and maximum chunk size,
supplied while instantiating a new arena.

Arena
-----

Arena is a bucket space of memory, with a maximum capacity, that
is empty to begin with and starts filling up as and when new
allocations are requested by application.

Slabs
-----

Slabs are created from 0 to 1TB of memory. Between`0` bytes and
`1Terabyte`, sizes are rounded off to discrete slabs. For example,
when memory-chunk of size 67 is requested, it will be allocated
from a slab of size 72. Organizing chunk sizes in slabs can help
reduce the overhead cost and improve performance.

Pools
-----

Memory for each slab-size will be managed in pools. A pool of memory
can contain several memory-chunks of same size. Memory is obtained
from OS in pool-size, which is calculated using a fair-model on
the assumption that there will be equal number of allocations required
from each slab. This issue is tracked under #30. Number chunks in
a pool cannot exceed `Maxpools` and a slab cannot contain more than
`Maxpools`.

Memory-chunk
------------

Memory-chunk is the basic unit of allocation in a pool and it is
always greater than or equal to the requested memory by application.

Panic and Recovery
------------------

Panics are to expected when APIs are misused. Programmers might choose
to ignore the errors, but not panics. For example:

* When arena's requested capacity exceeds Maxarenasize, which is typically
  1TB.
* When Free() is called on arena, call Free on the pool.
* When Arena runs Out-Of-Memory.
* When trying to free a nil pointer.
