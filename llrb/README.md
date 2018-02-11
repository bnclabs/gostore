# Left Leaning Red Black tree (LLRB)

[![GoDoc](https://godoc.org/github.com/bnclabs/gostore/llrb?status.png)](https://godoc.org/github.com/bnclabs/gostore/llrb)

LLRB can manage an in-memory instance of sorted index using
left-leaning-red-black tree. LLRB is self balancing tree that supports
all basic write operations, like create, update, delete. Additionally
there is an MVCC (Multi-Version-Concurrency-Control) implementation
of lef-leaning-read-black-tree.

* **Entry** also called as **llrb-node** has a key, value pair.
* **Key** are binary string that can handle comparision operation.
* **Value** can be a blob of binary, text or JSON. LLRB don't interpret
  the shape of Value.

## Background routines

LLRB without MVCC is a passive library that serializes all
index-read and index-write operations using rw-lock. That is, all
write and read operations are serialized, while there can be concurrent
readers. Based on the operations, an LLRB instance will incur memory cost.
Apart from that it is straight forward and should not throw any surprises.

When using MVCC, all index-write operations are serialsed but
there can be any number of concurrent readers on a MVCC snapshots. Unlike
LLRB, write operations don't block read operations. For each MVCC instance
there will be single go-routine spawned to generate periodic snapshots.

## Snapshots

Snapshots matter only with MVCC. For write intensive applications, it
is recommended to use LLRB. While Read intensive applications might want
to use MVCC and use concurrent readers to scale with number of cores.

## Memory fragmentation

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
**[copying-garbage-collection][copygc-link]**. LLRB does support Copying
GC out of the box, but here is a set of limitations and recommentations
that applications can use:

* Pointer re-write is not possible and not recommended.
* Allocate a new LLRB and start populating it with current set of entries.
* This means, holding an iterator for a long time.
* With MVCC, Iterations won't lock the writer and won't interfer with other
  concurrent readers. But if there are hundreds and thousands of mutations
  happening in the background, while the iterator is holding the snapshot,
  it can lead to huge memory pressure.
* If applications maintain a seqno for all mutations, then it is possible
  to build a piece-wise Iterator() that can be released for every
  few milliseconds. Refer #35.

## Log-Structured-Merge (LSM)

Log-Structured-Merge (LSM) is supported at api level. Specifically with
Delete API.

* Delete will simply be marked as deleted and seqno is updated to current
  seqno.
* For Delete operation, if entry is missing in the index. An entry
  will be inserted and then marked as deleted with its seqno updated to
  current-seqno.
* When a key is marked as deleted and Upserted again, the delete operation
  will get de-duped.

Package lsm/ provides a set of API that can do merge-get and merge-sort on
LSM enabled data-structures.

## Compare-And-Set (CAS)

CAS operations help in atomic updates to index entries. It ensures that
index entry does not change between a previous read/write operation and
next write operation. CAS operation has the following effects:

* If CAS is > ZERO, Get key from the index, and check whether its
  `seqno` matches with the supplied CAS value.
* If CAS is ZERO, then it is equivalent to CREATE operation, and expects
  that the key is not present in the index.

If LLRB tree is holding only a subset, called the working-set, of an index,
it is application's responsibility to do CAS match with full set of
index and convert the CAS operation into plain Upsert operation.

## Panic and Recovery

Panics are to be expected when APIs are misused. Programmers might choose
to ignore the errors, but not panics. For example:

- Validate() will panic if there is a fatal error.

[copygc-link]: http://www.cs.cornell.edu/courses/cs312/2003fa/lectures/sec24.htm
