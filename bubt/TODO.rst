* lsm testcase.
* build_mblock and snapshot_mblock should not use magic numbers.
* optimize allocation that are using make() and new().
* rename `markdelete` settings to `lsm`.

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
