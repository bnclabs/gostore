Storage algorithms in golang
============================

[![talk on matrix](https://github.com/prataprc/dotfiles/blob/master/assets/talkonmatrix.svg)](https://riot.im/app/#/user/@prataprc:matrix.org?action=chat)
[![GoDoc](https://godoc.org/github.com/bnclabs/gostore?status.png)](https://godoc.org/github.com/bnclabs/gostore)
[![Go Report Card](https://goreportcard.com/badge/github.com/bnclabs/gostore)](https://goreportcard.com/report/github.com/bnclabs/gostore)

Package storage implement a collection of storage algorithm and necessary
tools and libraries. Applications wishing to use this package please checkout
interfaces defined under [api/](api/README.md).

As of now, two data structures are available for indexing key,value entries:

* [**llrb**](llrb/README.md) in memory left-leaning red-black tree
* [**bubt**](bubt/README.md) immutable, durable bottoms up btree.
* [**bogn**](bogn/README.md) multi-leveled, lsm based, ACID compliant storage.

There are some sub-packages that are common to all storage algorithms:

* [**flock**](flock/README.md) read-write mutex locks across process.
* [**lib**](lib/README.md) collections of helper functions.
* [**lsm**](lsm/README.md) implements log-structured-merge.
* [**malloc**](malloc/README.md) custom memory alloctor, can be used instead
  of golang's memory allocator or OS allocator.

How to contribute
-----------------

[![Issue Stats](http://issuestats.com/github/bnclabs/gostore/badge/issue)](http://issuestats.com/github/bnclabs/gostore)
[![Issue Stats](http://issuestats.com/github/bnclabs/gostore/badge/pr)](http://issuestats.com/github/bnclabs/gostore)

* Pick an issue, or create an new issue. Provide adequate documentation for
  the issue.
* Assign the issue or get it assigned.
* Work on the code, once finished, raise a pull request.
* Gostore is written in [golang](https://golang.org/), hence expected to follow the
  global guidelines for writing go programs.
* If the changeset is more than few lines, please generate a
  [report card][report-link].
* As of now, branch ``master`` is the development branch.

[report-link]: https://goreportcard.com/report/github.com/bnclabs/gostore.
