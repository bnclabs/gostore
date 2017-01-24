LLRB settings:
--------------

`maxvb`
    as integer, maximum number of vbuckets that will used in this llrb tree.

`minkeysize`
    as integer, minimum size allowed for key.

`maxkeysize`
    as integer, maximum size allowed for key.

`minvalsize`
    as integer, minimum size allowed for value, if non-zero, `mvalue` should
    be enabled.

`maxvalsize`
    as integer, maximum size allowed for value, if non-zero, `mvalue` should
    be enabled.

`mvcc.enable`
    as boolean, consume LLRB as Multi-Version-Concurrency-Control-led tree.

`mvcc.snapshot.tick`
    as integer, interval in milli-second for generating read-snapshots.

`mvcc.writer.chansize`
    as integer, buffer size for mvcc writer's i/p channel.

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
