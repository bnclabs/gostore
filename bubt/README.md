# Bottoms up Btree

[![GoDoc](https://godoc.org/github.com/bnclabs/gostore/bubt?status.png)](https://godoc.org/github.com/bnclabs/gostore/bubt)

The short version `bubt` means Bottoms-up-Btree. It simply says how the
btree was built, which is bottoms up. The idea behind this implementation
is that sorted index of key,value entries are built once, marked as
immutable and available for concurrent readers.

## Bottoms up construction

* Building a bottoms-up tree starts with an iterator object that supply
  a stream of key,value entries, and associated fields for each entry.
* Incoming key,value entries are populated in a leaf node, called `z-node`.
* z-nodes can be stored in a separate file.
* Once an z-node is fully utilized, a new entry, called `m-entry`, is
  composed with:
  * Key element which is the `z-node` first entry's key element.
  * Value element which is the file position of the `z-node`.
* m-entries are inserted in the intermediate node called `m-node`.
* For every z-node, fully utilized, a m-entry is created and added into
  the m-node.
* As and when z-nodes are fully utilized they are flushed to disk in
  append only mode.
* When a m-node is fully utilized another level of intermediate node is
  created which points down to the fully utilized m-node.
* As and when m-nodes are fully utilized they are flushed to disk in
  append only mode.
* Size of z-node is same across the tree and configurable with each
  build.
* Size of m-node is same across the tree and configurable with each
  build.
* Finally the root node is flushed.
* After the root node, a single info-block of MarkerBlocksize is flushed.
  Infoblock contains arguments used to build the snapshot and also some
  statistics about the snapshot.
* After info-block, one or more blocks of index metadata (blocksize same
  as m-node) is flushed.
* After metadata, a single block, (blocksize is MarkerBlocksize) of
  marker-block is flushed.

** TODO: block diagram of disk format**

## Value log

To optimize on the `write-amplification`, Bubt instances can be constructed
with values (from each key,value entry) can be stored in separate file.
Note that this might have some negative impact on `disk-amplication` and in
come cases can decrease the throughput of random Get operations.

## Metadata, info-block

Applications can attach an opaque blob of **metadata** with every bubt
index. This can be supplied as argument to the Build() API. It is upto
the application to interpret this metadata.

Similarly info-block saves all the arguments / parameters supplied to
the Build() API, along with useful statistics about the snapshit as JSON
property. The size of info-block cannot exceed MarkerBlocksize.

** TODO: shape of info-block property**

## Background routines

While building the btree, separate go-routines are spawned to flush data
into file. There will be one go-routine for each index file.

A fully formed bubt instance can be opened using `OpenSnapshot` for read
only access and can be shared between go-routines. Snapshots can be opened
across multiple process without the danger of any race conditions.
None of the snapshots spawn go-routines. Everytime a snapshot is shared
with another go-routine, its reference count should be bumped. Only when
all snapshot references are released, snapshot can be closed. Likewise,
only when all snapshots are closed, the last reference can destory the
snapshot.

## Panic and Recovery

Panics are to expected when APIs are misused. Programmers might choose
to ignore the errors, but not panics. For example:

- For disk errors while building the tree or reading from snapshots.
- If input iterator returns error other than io.EOF.
- If bytes required to encode a key,value entry is more than the
  zblock's size.
- Using mutation APIs, like Set, Delete, Commit, on View object.
- Using mutation APIs like BeginTxn, Set, SetCAS, Delete, on Snapshot.
- Using mutation APIs, like Set, Delete, Delcursor, on Cursor object.
- Validate() API will panic, if:
  - keys in the bubt instance are not in sort order.
  - number of entries in the bubt instance does not match with header.
  - disk footprint is unreasonably larger.

None of the panics will automatically recover. It is upto the caller
to recover or fail-quick as the case may be.
