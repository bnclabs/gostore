LLRB data structure stats:

`llrb.node.useful`
`llrb.value.useful`
    useful memory obtained from OS.

`llrb.node.overhead`
`llrb.value.overhead`
    book-keeping overhead to manage `useful` memory.

`llrb.node.allocated`
`llrb.value.allocated`
    account of memory allocated from the `useful` pool.

`llrb.node.available`
`llrb.value.available`
    memory available for allocation.

`llrb.node.blocks`
`llrb.value.blocks`
    list of allocatable block-sizes.

`llrb.keymemory`
`llrb.valmemory`
    actual memory consumed by keys and values indexed in the tree.

`llrb.n_blacks`
    number of blacks links from root node to any leaf node.

`llrb.upsertdepth.samples`
`llrb.upsertdepth.min`
`llrb.upsertdepth.max`
`llrb.upsertdepth.mean`
`llrb.upsertdepth.variance`
`llrb.upsertdepth.stddeviance`
`llrb.upsertdepth.histogram:[]int64`
    every upsert into the into the tree will count the depth of the node
    inserted or updated, histogram starts from 1 to 128 incremented in
    steps of 1.

`llrb.height.samples`
`llrb.height.min`
`llrb.height.max`
`llrb.height.mean`
`llrb.height.variance`
`llrb.height.stddeviance`
`llrb.height.histogram:[]int64`
    walk entire tree to compute histogram over height of the tree.
