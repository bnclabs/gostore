package llrb

import "sync/atomic"
import "fmt"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

var _ = fmt.Sprintf("dummy")

const startlimit = 5

type iterator struct {
	// 64-bit aligned
	n_activeiter *int64

	tree       api.IndexReader
	llrb       *LLRB
	continuate bool
	nodes      []api.Node
	index      int
	limit      int
	startkey   []byte
	endkey     []byte
	incl       string
	reverse    bool
	closed     bool
}

// Next implement IndexIterator{} interface.
func (iter *iterator) Next() api.Node {
	if iter.closed {
		panic("cannot iterate over a closed iterator")
	} else if iter.index >= len(iter.nodes) && iter.continuate == false {
		return nil
	}

	if iter.index < len(iter.nodes) {
		nd := iter.nodes[iter.index]
		iter.index++
		return nd

	} else if iter.rangefill(); iter.index < len(iter.nodes) {
		nd := iter.nodes[iter.index]
		iter.index++
		return nd
	}
	return nil
}

func (iter *iterator) Close() {
	iter.closed, iter.nodes = true, iter.nodes[:cap(iter.nodes)]
	for i := range iter.nodes {
		iter.nodes[i] = nil
	}
	iter.nodes = iter.nodes[:0]
	atomic.AddInt64(iter.n_activeiter, -1)

	// give it back to the pool if not overflowing.
	if iter.llrb.mvcc.enabled == false { // NOTE: remember to reader unlock
		iter.llrb.putiterator(iter)
		// This is nasty :(
		iter.llrb.rw.RUnlock()
	} else {
		iter.llrb.putiterator(iter)
	}
}

func (iter *iterator) rangefill() {
	var breakkey, prev []byte
	iter.nodes, iter.index, iter.continuate = iter.nodes[:0], 0, false
	count := 0
	iter.tree.Range(
		iter.startkey, iter.endkey, iter.incl, iter.reverse,
		func(_ api.Index, _ int64, _, nd api.Node, err error) bool {
			if err != nil {
				return false
			}
			breakkey = nd.Key()
			if count < iter.limit || api.Binarycmp(prev, breakkey, true) == 0 {
				prev = breakkey
				iter.nodes = append(iter.nodes, nd)
				count++
				return true
			}
			iter.limit = count
			if int64(iter.limit) < iter.llrb.maxlimit {
				iter.limit *= 2
			}
			iter.continuate = true
			return false
		})

	if iter.reverse {
		iter.endkey = lib.Fixbuffer(iter.endkey, int64(len(breakkey)))
		copy(iter.endkey, breakkey)
	} else {
		iter.startkey = lib.Fixbuffer(iter.startkey, int64(len(breakkey)))
		copy(iter.startkey, breakkey)
	}
}
