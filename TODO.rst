- README: brief description of all sub-packages, links to godocs and articles.
  use the template from goparsec.
- Test lsm on large dataset using llrb and bubt iterators. Try with 1,2,4,8
  iterators. Include validate, benchmark and profiling.
- proof read README.rst
- lsm.go, test with IsDelete() returning true or false based on Deadseqno().
- update store/ test cases to create and update clocks for dict and llrb
  structure.
- implement Clone() method for Index{} interface.
- check whether calling multiple Destroy() is safe.
- replace `chan bool` sync channels with `chan struct{}`
- cleanup panic messages, log messages, and dummy-imports
- replace panicerr() with panic()
- optimize MergeIterator() to automatically discard keys (nodes) with older
  seqno.
- optimize LSM merger logic, avoid repeated sort() calls.
- move all TODO to github-issues.
- link default-settings for bubt,llrb,malloc in README page.
- lsm test case. Delete an entry and insert a new one. Check whether it
  is present in the new set.
- create a pkg for jemalloc and use that as alternative for malloc.
  compare and contrast them both for llrb.

llrb:

* block diagram of go-routines.
* rename api.MinKeymem to api.MinKeysize, and rest of the 3 constants.
* document statistics information and organise them between memory, llrb,
  mvcc, snapshots.
* `memutilization` param should be configurable ? What is it used for ?
* test marked deletes in mvcc mode.
* check for whether stats values need to be atomically protected.
* writer.go, respch is []interface{}, should we change that ?
* if there are no outstanding snapshots, may be we can do non-mvcc
  upserts and deletes ?
* YCSB test cases.

bubt:

* block diagram of go-routines.
* add settings parameter {min,vax} key size and {min,max} val size.
  validate input keys and values for lower and upper limit.
* build_mblock and snapshot_mblock should not use magic numbers.
* optimize allocation that are using make() and new().
* do one round of review and refactoring.
* catch all panic and gracefully recover.
* test bubt for empty list of key,value pairs.
* instead of repeating the key-content in the intermediate nodes,
  just refer them to znodes and cache all the references for future
  use.
* buffercache for intermediate nodes.
* useful types and interfaces from api/
  Index (panic on RSnapshot)
  IndexSnapshot (panic as not-implemented)
  IndexReader
  IndexIterator
  IndexWriter (panic as not-implemented)

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
* document the idea of vector clock (timestamp) and how it
  can be used to implement RYOW.

Insert, Upsert, Delete:

* Insert will cause rebalance to the tree.
* Delete will be marked for GC during next de-fragmentation/compaction cycle.
* Upsert will atomically over-write the old-value reference with new-value.
  * when to free the old value block ?
* Every mutation to the tree will be counted as tseqno. (tree-seqno).

BNC-Cabinet:

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
