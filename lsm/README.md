Log-Structured-Merge
--------------------

LSM is a popular technique to manage multi-level trees for write optimized
systesm.
Refer [Wikipedia article](https://en.wikipedia.org/wiki/Log-structured_merge-tree)

This package provide two APIs LSMMerge and LSMRange with minor differences
between them. They both take two or more IndexIterators and return a new
IndexIterator that merges and de-duplicates entries from input iterators.

Panic and Recovery
------------------

**This package does not panic**.
