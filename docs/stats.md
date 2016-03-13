LLRB data structure stats:

`node.useful`
`value.useful`
    useful memory obtained from OS.

`node.overhead`
`value.overhead`
    book-keeping overhead to manage `useful` memory.

`node.allocated`
`value.allocated`
    account of memory allocated from the `useful` pool.

`node.available`
`value.available`
    memory available for allocation.

`node.blocks`
`value.blocks`
    list of allocatable block-sizes.

`keymemory`
    exact memory occupied by entry-keys.

`valmemory`
    exact memory occupied by entry-values.

`n_count`
    number of {key,value} entries in the tree.

`n_blacks`
    number of blacks links from root node to any leaf node.

`n_lookups`
    number of lookup queries handled by the tree.

`n_ranges`
    number of range queries handled by the tree.

`n_inserts`
    number of entries inserted into the tree.

`n_updates`
    number of entries updated in the tree.

`n_deletes`
    number of entries deleted from the tree.

`n_allocs`
    number of nodes allocated.

`n_frees`
    number of nodes freed.

`n_clones`
    number of nodes cloned.

`h_upsertdepth`
    histogram of node depth while upserting into the tree.

`h_height`
    walk entire tree to compute histogram over height of the tree.
    won't be same as `h_upsertdepth` because the nodes may get
    rotated along the way up after the upsert..

`mvcc.n_snapshots`
    number of snapshots created.

`mvcc.n_purgedss`
    number of snapshots that are created and already purged.

`mvcc.h_bulkfree`
    histogram of nodes freed while purging a snapshot.

`mvcc.h_reclaims`
    histogram of nodes generated for reclaimation during upserts and deletes.
