Library of functions
--------------------

* Contains useful functions and features that are not particularly tied up to
  any storage algorithm.
* Implementations under this package must be self contained, and should not
  depend on anything other than standard library.
* Shall not import gostore package or any of its sub-packages.

Panic and Recover
-----------------

* Prettystats will panic if json.Marshal returns an error.
