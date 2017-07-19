// Package storage implement a collection of storage algorithm and
// necessary tools and libraries.
//
// api:
//
// Interface specification to access gostore datastructures.
//
// dict:
//
// Implement a simple in-memory hash map based on golang map. Primary used
// as reference for testing.
//
// llrb:
//
// Implement a version of Left Leaning Red Black tree for sorting and
// retrieving {key,value}.
//
// bubt:
//
// Implement a version of Bayer-Tree for sorting and retrieving {key,value}.
//
// lsm:
//
// Implement Log-structured-merge between two indexes. Accounts for deleted
// entries, bornseqno of a new entry, deadseqno of deleted entries.
// Deduplicate older mutations for the same entry.
//
// malloc:
//
// Custom memory management for storage algorithms.
package storage
