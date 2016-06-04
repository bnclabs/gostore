* test bubt for empty list of key,value pairs.
* instead of repeating the key-content in the intermediate nodes,
  just refer them to znodes and cache all the references for future
  use.
* useful types and interfaces from api/
  RangeCallb
  NodeGetter
  NodeSetter (panic as not-implemented)
  IndexIterator
  Index (panic on RSnapshot)
  IndexSnapshot (panic as not-implemented)
  IndexReader
  IndexWriter (panic as not-implemented)
