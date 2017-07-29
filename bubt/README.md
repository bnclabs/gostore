Bottoms up Btree
================

The short version `bubt` means Bottoms-up-Btree. It simply says how the
btree was built, which is bottoms up. The idea behind this implementation
is that sorted index of key,value entries are built once, marked as
immutable and available for concurrent readers.

Bottoms up construction
=======================

* Building a bottoms-up tree starts with an iterator object that supply
  a stream of key,value entries.
* Incoming key,value entries are populated in a leaf node, called `z-node`.
* Values can be appended to a separate file, based on configuration.
* Once the z-node is fully utilized a new entry, called `m-entry` is
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
* After the root node, build statistics is flushed.
* After build statistics, tree settings is flushed.
* After tree settings, index metadata is flushed.
* After metadata, marker-block is flushed.
* And finally bubt-header is flushed.

Header
------

* 8-byte length of build statistics field.
* 8-byte length of settings field.
* 8-byte length of metadata filed.
* 8-byte file-position for rooblock in the index file.
* 8-byte file-position for root-reduce either in index file or in
  value file.

Marker block
------------

A marker-block of size `MarkerBlocksize` is populate with `MarkerByte`
and flushed before the header fields. Only when marker-block is
identified with its full size followed by 40 bytes of header, the index
file is considered well-formed.

Metadata, Settings and Statistics
---------------------------------

* Applications can attach an opaque blob of **metadata** with every bubt
  index. This can be supplied as argument to the Build() API. It is upto
  the application to interpret this metadata.
* Similarly settings argument supplied as argument to the Build() API
  is persisted as **settings** field after the root block. To know the shape
  and field of settings refer to Defaultsettings(). The same settings
  will be used to initialize a snapshot from bubt index file.
* Bubt builder does few statistics accounting while building the tree,
  which is persisted as statistics after the root blockk.

Background routines
-------------------
