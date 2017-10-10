package api

// Getter function, given a key, returns indexed entry.
type Getter func(key, value []byte) (val []byte, cas uint64, del, ok bool)

// Iterator function to iterate on each indexed entry in sort order.
type Iterator func(fin bool) (key, val []byte, seqno uint64, del bool, e error)
