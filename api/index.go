package api

type Getter func(key, value []byte) (val []byte, cas uint64, del, ok bool)

type Iterator func(fin bool) (key, val []byte, seqno uint64, del bool, e error)
