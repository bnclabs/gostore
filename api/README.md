Gostore API
-----------

Package api/ define the common interface points with gostore library.
Except for creating a new storage instance, applications should use methods
defined under this package.

**Index APIs**

* Constants that are relevant for all storage algorithms.
* Well defined error codes.
* Define Index operations, like, Upsert, CAS, Delete, and Get, Range.
* Index iteration.
* Snapshotted index.
* Clock operations, for index that include time order for Index
  operations.
* Types and constants to batch several write-operations on an index instance.
* Index Node abstractions, each entry in an index is encapsulated as node.
* Node read/write/metadata operations.
* Metadata information include:
  * Vbucket-number and its unique-id called Vbuuid.
  * Access time.
  * Bornseqno for upsert operations, Deadseqno for delete operations.
    Sequence number themself are maintained outside the index instance.
  * Deleted flag, for Log-Structured-Merge implementation.
* Snapshot on index for concurrent read.

**Memory APIs**

* Allocate memory chunks.
* Free memory once it become unused.
* Statistics on memory arena.

**Although minor updates are to be expected on APIs, they are stable enough
for building storage application**.

**NOTE:** Check out `Scalarclock` implementation to get an idea on
implementing timestamps for index write-operations.

Panic and Recovery
------------------

**Package api/ never panics.**
