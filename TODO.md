LLRB:
* imporve panic messages.
* upsert samples should match upsert+insert counts.
* improve logging while purging snapshots
    PURGE <snapshot>, remaining chain of snapshot.
* should get snapshot by a sync call to writer or post call ?
* check the maximum value for height stats.
* improve panic messages.
* should llrb.clone() copy value also ? can it be optimized for mvcc ?
* if no snapshots are refering to a node, don't clone.
* check the difference between upsertdepth and tree height statistics
  and try to reason with it.
* keymemory == 'sum of all Node.Key()'
* valmemory == 'sum of all Node.Value()'
* count stats in LLRB{} and LLRBSnapshot{}
* test mvcc.writer.DeleteMin() on a tree with single node.
* update access field.
* dotdump hightlight red/black edges.
* add checker for memory leak to memarena.
* mvcc.writer export it via method Writer()
* change config["mvcc.enabled"] to config["mvcc.enable"].
* snapshot.Count(), don't use llrb.count
* monster validation should randomly include value for upsert.
* amend Get API to return the depth of the key inside the tree,
  can be used for debugging.
* add llrb tree validation rules, based on sedgewick paper.
* add test cases for DeleteMin() and DeleteMax().
* should we export vectorclock, also add vector clock method to Index{}
  interface ?
* YCSB test cases.

clock:
* exhaust all possible combination of clock comparision TestVectorclockOrder.
  * subset on LHS, fullset on RHS
  * subset on RHS, fullset on LHS
* fullset having a lesser seqno for vbucket not present in subset.
* fullset having a lesser seqno for vbucket present in subset.
* subset having a lesser seqno.
* fullset, subset having all equal seqnos.
* fullset, subset having all equal seqnos.
* fullset having a greater seqno for vbucket not present in subset.
* fullset having a greater seqno for vbucket present in subset.
* subset having a greater seqno.

* CBOR in ; JSON out.
* CBOR based json-pointer lookup is essential for all KV operation.
* delta operation on KV is essential for reducing the log file size.

Insert, Upsert, Delete:

* Insert will cause rebalance to the tree.
* Delete will be marked for GC during next de-fragmentation/compaction cycle.
* Upsert will atomically over-write the old-value reference with new-value.
  * when to free the old value block ?
* Every mutation to the tree will be counted as tseqno. (tree-seqno).

De-fragmentation cycle:

* defragmentation will be triggered if,
  * Allocated/Memory ratio is beyond a certain threshold defragmentation
    is required.
* Rebuild the entire LLRB tree,
  * note the llrb-tseqno. for first iteration, as filter-tseqno.
  * start first iteration by issuing piecewise range calls on the main tree.
    * purge all node marked as `deleted`.
    * with 1B entries, first iteration can take upto 25 minutes.
  * start the next iteration filtering out all entries <= filter-seqno.
  * repeat this until difference between filter-tseqno. and current-tseqno
    is < threshold.
  * after which all write will be simultaneously applied to both trees.
  * switch the tree and release the old tree.

Disk Greater than Memory:

* LLRB will be backed by one or more disk btree, each in a separate file.
* on-disk btree is always built bottoms up and once persisted and sealed
  remains immutable.
* One `alpha-file` that contains the full set of entries upto a tseqno.

Persistance-cycle:

* Persistance-cycle will be triggered if,
  * Memory consumed by the tree exceed a threshold limit.
  * N seconds have passed since the last Persistance or Compaction cycle.
* Build delta-files,
  * note the tseqno. in the alpha-file as alpha-tseqno.
  * note the llrb-tseqno. as filter-tseqno.
  * start first iteration by issuing piecewise range calls on the
    main tree, all on-disk delta-files if any, filtering out
    all entries <= aplpha-tseqno.
    * purge all node marked as `deleted`.
    * merge sort the entries from main tree and delta-files and build a new
      delta-file.
  * end the first iteration by persisting the delta file and sealing them.
  * start the second iteration filtering out all entries <= filter-seqno.
  * repeat this until difference between filter-tseqno. and current-tseqno
    is < threshold.

Compaction-cycle:

* Compaction-cycle will be triggered if,
  * Cummulative Size of delta-file / Size of alpha-file exceeds a threshold.
* Merge sort alpha-file with all the delta-file and build a new alpha file.
  * Construct a new llrb-tree from the alpha-file.
