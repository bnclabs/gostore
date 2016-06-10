LLRB data structure stats:
--------------------------

all statistics belong to either LLRB{} or LLRBSnapshot{} instances.
LLRBSnapshot{} is applicable only in MVCC variant of LLRB. 

IndexSnapshot statistics:
-------------------------

`n_lookups`
    number of lookup operations performed on this snapshot, note that
    this value is counted only when there are no concurrent mutation happening
    to the writer version of the tree.

`n_ranges`
    number of range and iterate operations performed on this snapshot,
    note that this value is counted only when there are no concurrent
    mutation happening to the writer version of the tree.

`n_cclookups`
    number of lookup operations performed on this snapshot, note that
    this value is counted only when concurrent mutation is happening
    to the writer version of the tree.

`n_ccranges`
    number of range and iterate operations performed on this snapshot,
    note that this value is counted only when concurrent mutation is
    happening to the writer version of the tree.

when the snapshot is purged these counts are accumlated to the parent LLRB{}
instance.

LLRB statistics:
----------------

`n_lookups`
    number of lookup operations performed on this tree, in case of MVCC
    variant it is the cummulative count of all lookup operations performed
    on all the snapshots (purged) so far.

`n_ranges`
    number of range and iterate operations performed on this tree, in case
    of MVCC variant it is the cummulative count of all lookup operations
    performed on all the snapshots (purged) so far.

`n_count`
    number of {key,value} entries in the tree.

`n_inserts`
    number of entries inserted into the tree.

`n_updates`
    number of entries updated in the tree.

`n_deletes`
    number of entries deleted from the tree.

`n_nodes`
    number of nodes allocated.

`n_frees`
    number of nodes freed.

`n_clones`
    number of nodes cloned.

`h_upsertdepth`
    histogram of node depth while upserting into the tree.

MVCC statistics:
----------------

`mvcc.n_cclookups`
    the cummulative count of all lookup operations performed on all
    the snapshots (purged) so far, when concurrent mutation is happening
    to the writer version of the tree.

`mvcc.n_ccranges`
    the cummulative count of all range and iterate operations performed
    on all the snapshots (purged) so far, when concurrent mutation is
    happening to the writer version of the tree.

`mvcc.n_snapshots`
    number of snapshots created.

`mvcc.n_purgedss`
    number of snapshots that are created and already purged.

`mvcc.n_activess`
    number of snapshots that are created and still active.

`mvcc.h_bulkfree`
    histogram of nodes freed while purging a snapshot.

`mvcc.h_reclaims`
    histogram of nodes generated for reclaimation during upserts and deletes.

`mvcc.h_versions`
    histogram of active snapshots when concurrent mutation is happening
    to the writer version of the tree.

following statistics are computed on the fly when Stat() API is called with
`involved` level 9, note that level > 0 might invoke a full table
scan the tree.

`n_blacks`
    number of blacks links from root node to any leaf node.

`h_height`
    walk entire tree to compute histogram over height of the tree.
    won't be same as `h_upsertdepth` because the nodes may get
    rotated along the way up after the upsert..

Memory statistics:
------------------

`node.useful` & `value.useful`
    useful memory obtained from OS.

`node.overhead` & `value.overhead`
    book-keeping overhead to manage `useful` memory.

`node.allocated` & `value.allocated`
    account of memory allocated from the `useful` pool.

`node.available` & `value.available`
    memory available for allocation.

`node.blocks` & `value.blocks`
    list of allocatable block-sizes.

`keymemory`
    exact memory occupied by entry-keys.

`valmemory`
    exact memory occupied by entry-values.

