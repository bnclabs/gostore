Log-Structured-Merge
--------------------

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
holding a subset, called working-set, of the full index.
