package llrb

import "sync/atomic"

import "github.com/prataprc/storage.go/api"

type iterator struct {
	tree       api.IndexReader
	llrb       *LLRB
	nodes      []api.Node
	continuate bool
	index      int
	limit      int
	startkey   []byte
	endkey     []byte
	incl       string
	reverse    bool
	closed     bool
	activeiter *int64
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

	// give it back to the pool if not overflowing.
	llrb := iter.llrb
	if int64(len(llrb.iterpool)) < llrb.config.Int64("iterpool.size") {
		llrb.iterpool <- iter
	}
	atomic.AddInt64(iter.activeiter, -1)

	if llrb.mvcc.enabled == false {
		// NOTE: remember to see this reader unlock
		llrb.rw.RUnlock()
	}
}

func (iter *iterator) rangefill() {
	var breakkey []byte
	iter.nodes, iter.index, iter.continuate = iter.nodes[:0], 0, false
	count := 0
	iter.tree.Range(iter.startkey, iter.endkey, iter.incl, iter.reverse,
		func(nd api.Node) bool {
			breakkey = nd.Key()
			if count < iter.limit {
				iter.nodes = append(iter.nodes, nd)
				count++
				return true
			}
			if iter.limit < 100 {
				iter.limit *= 2
			}
			iter.continuate = true
			return false
		})
	if iter.reverse {
		iter.endkey = breakkey
	} else {
		iter.startkey = breakkey
	}
}
