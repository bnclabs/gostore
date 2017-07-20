Log-Structured-Merge (LSM)
--------------------------

Log-Structured-Merge (LSM) is available off-the-shelf with LLRB store.
Just enable `lsm` via settings while creating the LLRB tree. Enabling
LSM will have the following effects:

* DeleteMin, DeleteMax and Delete will simply be marked as deleted
  and its deadseqno will be updated to currseqno.
* For Delete operation, if entry is missing in the index. An entry
  will be inserted and then marked as deleted with its deadseqno
  updated to currseqno.
* When a key marked as deleted is Upserted again, its deadseqno will
  be set to ZERO, and deleted flag is cleared.
* In case of UpsertCAS, CAS should match before entry is cleared from
  delete log.
* All of the above bahaviour are equally applicable with MVCC enabled.

NOTE: DeleteMin and DeleteMax is not useful when LLRB index is only
holding a subset, called working-set, of the full index. It is upto
the application to convert DeleteMin and DeleteMax operation into
Delete operation.

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
