LLRB settings:
--------------

`maxvb`
    as integer, maximum number of vbuckets that will used in this llrb tree.

`mvcc.enable`
    as boolean, consume LLRB as Multi-Version-Concurrency-Control-led tree.

`mvcc.snapshot.tick`
    as integer, interval in milli-second for generating read-snapshots.

`mvcc.writer.chanbuffer`
    as integer, buffer size for mvcc writer's i/p channel.

`nodearena.minblock`
    as integer, minimum node-block size that shall be requested from the arena.

`nodearena.maxblock`
    as integer, maximum node-block size that shall be requested from the arena.

`nodearena.capacity`
    as integer, capacity in bytes that the arena shall manage for node-blocks
    using one or more pools.

`nodearena.pool.capacity`
    as integer, limit the size of a pool, irrespective of pool's block size.

`nodearena.maxpools`
    as integer, maximum number of pools allowed in an arena.

`nodearena.maxchunks`
    as integer, maximum number of chunks allowed in a pool.

`nodearena.allocator`
    as string, type of allocater to use "flist".

`valarena.minblock`
    as integer, minimum value-block size that shall be requested from the
    arena.

`valarena.maxblock`
    as integer, maximum value-block size that shall be requested from the
    arena.

`valarena.capacity`
    as integer, capacity in bytes that the arena shall manage for
    node-blocks using one or more pools.

`valarena.pool.capacity`
    as integer, limit the size of a pool, irrespective of pool's block size.

`valarena.maxpools`
    as integer, maximum number of pools allowed in an arena.

`valarena.maxchunks`
    as integer, maximum number of chunks allowed in a pool.

`valarena.allocator`
    as string, type of allocater to use "flist".

`metadata.bornseqno`
    as boolean, if true, use metadata field to book-keep node's born
    sequence number.

`metadata.deadseqno`
    as boolean, if true, use metadata field to book-keep node's dead
    sequence number.

`metadata.mvalue`
    as boolean, if true, and expect to index value for each key entry.

`metadata.vbuuid`
    as boolean, if true, use metadata field to book-keep node's vbuuid.

`metadata.fpos`
    as boolean, if true, use file position in disk files where value is
    stored.

`iterpool.size`
    number concurrent iterations that are supported.
