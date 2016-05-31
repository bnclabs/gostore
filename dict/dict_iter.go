package storage

import "bytes"
import "sync/atomic"

import "github.com/prataprc/storage.go/api"

type dictIterator struct {
	dict       map[uint64]*dictnode
	hashks     []uint64
	index      int
	endkey     []byte // can be highkey or lowkey
	cmp        int
	reverse    bool
	closed     bool
	activeiter *int64
}

// Next implement IndexIterator{} interface.
func (iter *dictIterator) Next() api.Node {
	if iter.closed {
		panic("cannot iterate over a closed iterator")
	}

	if iter.reverse {
		if iter.index >= 0 {
			nd := iter.dict[iter.hashks[iter.index]]
			if iter.endkey != nil {
				if bytes.Compare(nd.key, iter.endkey) >= iter.cmp {
					iter.index--
					return nd
				}
				return nil
			}
			iter.index--
			return nd
		}
	} else {
		if iter.index < len(iter.hashks) {
			nd := iter.dict[iter.hashks[iter.index]]
			if iter.endkey != nil {
				if bytes.Compare(nd.key, iter.endkey) < iter.cmp {
					iter.index++
					return nd
				}
				return nil
			}
			iter.index++
			return nd
		}
	}
	return nil
}

// Close implement IndexIterator{} interface.
func (iter *dictIterator) Close() {
	iter.closed = true
	atomic.AddInt64(iter.activeiter, -1)
}
