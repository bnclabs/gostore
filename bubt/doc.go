// Package bubt builds Btree bottoms up and keeps it immutable.
// By having it as immutable, it is possible to attain near 100%
// node utilization, and allow concurrent reads on fully built
// tree.
package bubt
