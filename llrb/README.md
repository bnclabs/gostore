Left Leaning Red Black tree (LLRB)
==================================

LLRB can manage an in-memory instance of sorted index using
left-leaning-red-black tree. LLRB is self balancing tree that supports
all basic write operations, like create, update, delete. Additionally
this implementation supports multi-version-concurrency control on
the LLRB tree, for applications that are read intensive.

* **Entry** also called as **llrb-node** has a key, value pair.
* **Key** are binary string that can handle comparision operation.
* **Value** can be a blob of binary, text or JSON. LLRB don't interpret
  the shape of Value.
* **Metadata** other than key and value each entry can have zero or
  more optional metadata fields. Like:
  * **Vbno** for pointing the vbucket shard in which this key/value
    originaly belong.
  * **Vbuuid** unique uuid for handing failures and rollbacks.
  * **Access-time** for managing cache eviction and memory recylcing.
  * **Bornseqno** for implementing log-structured-merge.
  * **Deadseqno** for implementing log-structured-merge.

Background routines
-------------------

LLRB without MVCC is a passive library that serializes all
index-read and index-write operations using lock. Based on the
operations an LLRB instance will incur memory cost. Apart from
that it is straight forward and should not throw any surprises.

With MVCC enabled, all index-write operations are serialsed but
there can be any number of concurrent readers. Note that, with
MVCC, read can be issued only on index-snapshots, any reads
on the index itself are simply not allowed.

With MVCC enabled, LLRB will spawn two background routines:
* Writer routine to serialize all write operations.
* Snapshot ticker, that will keep generating snapshots. It ensures
  that there is alteast only snapshot to read at any given time.

Snapshots
---------

Snapshots matter only when MVCC is enabled. Enabling or disabling
MVCC is based on the ratio between write operations and read operations.
For write intensive applications, it is recommended to disable MVCC.
While Read intensive applications might want to enable MVCC and use
concurrent readers.

When MVCC is enabled, eead operations are allowed only on the snapshots.
It important to Release the snapshot withing few milliseconds, especially
when there are thousands of concurrent writes happening.

Memory fragmentation
--------------------

Memory fragmentation is when most of the memory is allocated in a large
number of non-contiguous blocks, or chunks - leaving a good percentage
of total memory unallocated, but unusable by rest of the system. This
can manifest itself as out of memory exceptions. Sadly this can happen
to any process running on the machine, even though it is not at fault.

LLRB tree is built to handle large number of upsert/delete operations
which can quickly lead to memory fragmentation. Especially when entries
are deleted in a particular pattern, where each pool allocated from OS
contains just few allocations.

A proper solution would be to have an allocator that can directly
manage the the CPU/Memory virtual pages. Even then fragmentation issue
won't be solved completely, and, having an allocator that is tightly
bolted to bare metal will open up a new set of issues.

One idea that can be employed to avoid memory-fragmentation is to use
**[copying-garbage-collection](http://www.cs.cornell.edu/courses/cs312/2003fa/lectures/sec24.htm)**.
LLRB does support Copying GC out of the box, but here is a set of
limitations and recommentations that applications can use:

* Pointer re-write is not possible and not recommended.
* Allocate a new LLRB and start populating it with current set of
  entries.
* This means, holding an iterator for a long time.
* With MVCC disabled, Iterations will lock the entire tree, until it
  completes.
* With MVCC enabled, Iterations won't lock the writer and won't
  interfer with other concurrent readers. But if there are hundreds
  and thousands of mutations happening in the background, while the
  iterator is holding the snapshot, it can lead to huge memory
  pressure.
* If applications maintain a seqno for all mutations and LLRB is
  enabled with `bornseqno` and `deadseqno`, then it is possible
  to build a piece-wise Iterator() that can be released for every
  few milliseconds. Refer #35.

Log-Structured-Merge (LSM)
--------------------------

Log-Structured-Merge (LSM) is available off-the-shelf with LLRB store.
Just enable `lsm` via settings while creating the LLRB tree. Enabling
LSM will have the following effects:

* Delete will simply be marked as deleted, its deadseqno will be
  updated to currseqno.
* For Delete operation, if entry is missing in the index. An entry
  will be inserted and then marked as deleted with its deadseqno
  updated to currseqno.
* When a key marked as deleted is Upserted again, its deadseqno will
  be set to ZERO, and deleted flag is cleared.
* In case of UpsertCAS, CAS should match before entry is cleared from
  delete log.
* All of the above bahaviour are equally applicable with MVCC enabled.

Compare-And-Set (CAS)
---------------------

CAS operations help in atomic updates to index entries. It ensures that
index entry does not change between a previous read/write operation and
next write operation. CAS operation has the following effects:

* If CAS is > ZERO, Get key from the index, and check whether its
  `bornseqno` matches with the supplied CAS value.
* If CAS is ZERO, then it is equivalent to CREATE operation, and expects
  that the key is not already present in the index.

If LLRB tree is holding only a subset, called the working-set, of an index,
it is application's responsibility to do CAS match with full set of
index and convert the CAS operation into plain Upsert operation.

Panic and Recovery
------------------

Panics are to expected when APIs are misused. Programmers might choose
to ignore the errors, but not panics. For example:

- When trying to upsert a nil key.
- When Log() is called on snapshots.
- When a snapshot is released more than it is refered.
- When a snapshot is released when there is un-closed iteration on the
  snapshot.
- Validate() will panic if there is a fatal error.
- When trying to iterate on index/snapshot after it is closed.
- When RSnapshot() is requested with mvcc disabled.
