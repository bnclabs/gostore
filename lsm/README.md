# Log-Structured-Merge

[![GoDoc](https://godoc.org/github.com/bnclabs/gostore/lsm?status.png)](https://godoc.org/github.com/bnclabs/gostore/lsm)

LSM is a popular technique to manage multi-level trees for write optimized
system. Refer [Wikipedia article][wiki-link]. `LLRB`, `MVCC`, and `Bubt`
instances support LSM. This package provides APIs for both Get() operations
and Range/Full-Table-Scan operations.

[wiki-link]: https://en.wikipedia.org/wiki/Log-structured_merge-tree
