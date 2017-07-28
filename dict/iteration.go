package dict

import "sync/atomic"
import "fmt"

import "github.com/prataprc/gostore/api"

var _ = fmt.Sprintf("dummy")

type iterator struct {
	tree       api.IndexReader
	dict       *Dict
	index      int
	lkey       []byte
	hkey       []byte
	incl       string
	reverse    bool
	closed     bool
	activeiter *int64
}

// Next implement api.IndexIterator interface.
func (iter *iterator) Next() api.Node {
	if iter.closed {
		panic("cannot iterate over a closed iterator")
	} else if iter.index >= len(iter.dict.hashks) {
		return nil
	} else if iter.index < 0 {
		return nil
	}

	nd := iter.dict.dict[iter.dict.hashks[iter.index]]
	if iter.reverse == false {
		iter.index++
		if iter.hkey == nil {
			return nd
		}
		switch iter.incl {
		case "high", "both":
			if api.Binarycmp(nd.key, iter.hkey, true /*partial*/) <= 0 {
				return nd
			}
		default:
			if api.Binarycmp(nd.key, iter.hkey, true /*partial*/) < 0 {
				return nd
			}
		}

	} else {
		iter.index--
		if iter.lkey == nil {
			return nd
		}
		switch iter.incl {
		case "low", "both":
			if api.Binarycmp(nd.key, iter.lkey, true /*partial*/) >= 0 {
				return nd
			}
		default:
			if api.Binarycmp(nd.key, iter.lkey, true /*partial*/) > 0 {
				return nd
			}
		}
	}
	return nil
}

// Close implement api.IndexIterator method
func (iter *iterator) Close() {
	iter.closed = true
	// give it back to the pool if not overflowing.
	atomic.AddInt64(iter.activeiter, -1)
}
