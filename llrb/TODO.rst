* rename `markdelete` as `lsm`.
* handle KeyMissing for Delete(), DeleteMin(), DeleteMax()
* run `make test` and `snaptools.go:/run.sh mvcc` simultaneously there seem to
  be some data race.
* IndexReader{} and IndexWriter{} APIs now include error arguments. Update the
  test case for handling error arguments.
* test marked deletes in mvcc mode.
* write test cases for Mutations() API.
* if there are no outstanding snapshots, may be we can do non-mvcc
  upserts and deletes ?
* YCSB test cases.
* add llrb tree validation rules, based on sedgewick paper.
* add interface for vector-clock under api:Index{} and api:IndexSnapshot{}
