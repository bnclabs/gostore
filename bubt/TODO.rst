* catch all panic and gracefully recover.
* is 2 byte enough for vlen ?
* if value is nil, should we store value-length as ZERO ? Is there a way to
  optimize that ?
* test bubt for empty list of key,value pairs.
* instead of repeating the key-content in the intermediate nodes,
  just refer them to znodes and cache all the references for future
  use.
* useful types and interfaces from api/
  RangeCallb
  Index (panic on RSnapshot)
  IndexSnapshot (panic as not-implemented)
  IndexReader
  IndexIterator
  IndexWriter (panic as not-implemented)
