Storage algorithms in golang
============================

Package storage implement a collection of storage algorithm and necessary
tools and libraries. Applications wishing to use this package please checkout
interfaces defined under [api/](api/README.md).

As of now, two data structures are available for indexing key,value entries:
* [**llrb**](llrb/README.md) left-leaning red-black tree
* [**bubt**](bubt/README.md) bottoms up btree.

There are some sub-packages that are common to all storage algorithms:
* [**lib**](lib/README.md) collections of helper functions.
* [**dict**](dict/README.md) implements a mock index that can be used for test and
  verification.
* [**lsm**](lsm/README.md) implements log-structured-merge.
* [**malloc**](malloc/README.md) custom memory alloctor, can be used instead of golang's
  memory allocator or OS allocator.

How to contribute
-----------------

* Pick an issue, or create an new issue. Provide adequate documentation for
  the issue.
* Assign the issue or get it assigned.
* Work on the code, once finished, raise a pull request.
* Gostore is written in [golang](https://golang.org/), hence expected to follow the
  global guidelines for writing go programs.
* If the changeset is more than few lines, please generate a
  [report card](https://goreportcard.com/report/github.com/prataprc/gostore).
* As of now, branch ``master`` is the development branch.
