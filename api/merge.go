package api

import "bytes"
import "sort"

type mergeiterator struct {
	iterators []IndexIterator
	nexts     []Node
}

func MergeIterators(iterators ...IndexIterator) IndexIterator {
	miter := &mergeiterator{
		iterators: make([]IndexIterator, len(iterators)),
		nexts:     make([]Node, len(iterators)),
	}
	for i, iterator := range iterators {
		miter.iterators[i], miter.nexts[i] = iterator, iterator.Next()
	}
	sort.Sort(miter)
	return miter
}

func (miter *mergeiterator) Next() Node {
	node, iterator := miter.nexts[0], miter.iterators[0]
	if node == nil {
		return nil

	} else if nextnode := iterator.Next(); nextnode == nil {
		copy(miter.iterators, miter.iterators[1:])
		copy(miter.nexts, miter.nexts[1:])
		till := len(miter.iterators) - 1
		miter.iterators[till], miter.nexts[till] = iterator, nil

	} else {
		var till int
		var iternode Node
		nextkey := nextnode.Key()
		for till, iternode = range miter.nexts[1:] {
			if iternode != nil && bytes.Compare(nextkey, iternode.Key()) > 0 {
				continue
			}
			break
		}
		if iternode != nil && bytes.Compare(nextkey, iternode.Key()) > 0 {
			till++
		}
		copy(miter.iterators, miter.iterators[1:till+1])
		copy(miter.nexts, miter.nexts[1:till+1])
		miter.iterators[till], miter.nexts[till] = iterator, nextnode
	}
	return node
}

func (miter *mergeiterator) Close() {
	for i, it := range miter.iterators {
		it.Close()
		miter.iterators[i], miter.nexts[i] = nil, nil
	}
}

func (miter *mergeiterator) Len() int {
	return len(miter.iterators)
}

func (miter *mergeiterator) Less(i, j int) bool {
	nodei, nodej := miter.nexts[i], miter.nexts[j]
	if nodei == nil {
		return false
	} else if nodej == nil {
		return true
	}
	keyi, keyj := nodei.Key(), nodej.Key()
	return bytes.Compare(keyi, keyj) < 0
}

func (miter *mergeiterator) Swap(i, j int) {
	miter.iterators[i], miter.iterators[j] =
		miter.iterators[j], miter.iterators[i]
	miter.nexts[i], miter.nexts[j] = miter.nexts[j], miter.nexts[i]
}
