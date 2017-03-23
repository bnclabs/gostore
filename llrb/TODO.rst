* check for whether stats values need to be atomically protected.
* writer.go, respch is []interface{}, should we change that ?
* IndexReader{} and IndexWriter{} APIs now include error arguments. Update the
  test case for handling error arguments.
* test marked deletes in mvcc mode.
* write test cases for Mutations() API.
* if there are no outstanding snapshots, may be we can do non-mvcc
  upserts and deletes ?
* YCSB test cases.
* add llrb tree validation rules, based on sedgewick paper.
* add interface for vector-clock under api:Index{} and api:IndexSnapshot{}
