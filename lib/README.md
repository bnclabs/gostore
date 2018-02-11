# Collection of library of functions

[![GoDoc](https://godoc.org/github.com/bnclabs/gostore/lib?status.png)](https://godoc.org/github.com/bnclabs/gostore/lib)

* Contain useful functions and features that are not particularly tied up to
  any storage algorithm.
* Implementations under this package must be self contained, and should not
  depend on anything other than standard library.
* Shall not import gostore package or any of its sub-packages.

## Panic and recover

* Prettystats will panic if json.Marshal returns an error.
