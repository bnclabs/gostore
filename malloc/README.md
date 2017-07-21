Custom memory allocator
=======================

Malloc supplies custom memory management for storage algorithms,
with a limited scope:

* Types and Functions exported by this package are not thread safe.
* Work best when memory behaviour is known apriori.
* Memory is allocated in pools, of several Megabytes, where each
  pool manages several memory-chunks of same size.
* Once a pool block is allocated from OS, it is not automatically
  given back to OS. Pools are freed only when the entire arena
  is Released.
* There is no pointer re-write, if copying garbage collector is
  necessary it can be implemented on top of this implementation.


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

This implentation expects cues from application with regards to
its memory requirement. Two such parameters are:

* `minblock` is the smallest memory chunk that will be requested
  by the application.
* `maxblock` is the largest memory chunk that will be requested
  by the application.

Between `minblock` and `maxblock` sizes are rounded off to discrete
slabs. For example, when memory-chunk of size 67 is requested, it
will be allocated from a slab of size 70. Organizing chunk sizes
in slabs can help reduce the overhead cost and improve performance.

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

TODO:
