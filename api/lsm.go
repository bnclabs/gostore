package api

import "bytes"
import "sort"

type lsmiterator struct {
	merge     bool
	iterators []IndexIterator
	nexts     []Node
	reverse   bool
	node      Node
}

func LSMIterators(reverse, merge bool, iterators ...IndexIterator) IndexIterator {
	if len(iterators) < 2 {
		panic("pass 2 or more iterators for LSM")
	}

	miter := &lsmiterator{
		merge:     merge,
		iterators: make([]IndexIterator, len(iterators)),
		nexts:     make([]Node, len(iterators)),
		reverse:   reverse,
	}
	for i, iterator := range iterators {
		miter.iterators[i], miter.nexts[i] = iterator, iterator.Next()
	}
	if reverse {
		sort.Sort(sort.Reverse(miter))
	} else {
		sort.Sort(miter)
	}
	if len(iterators) > 0 {
		miter.node = miter.nextnode()
	}
	return miter
}

func (miter *lsmiterator) Next() (n Node) {
	if miter.node == nil {
		return nil
	}
	n, miter.node = miter.node, miter.nextnode()
	return n
}

func (miter *lsmiterator) Close() {
	for i, it := range miter.iterators {
		it.Close()
		miter.iterators[i], miter.nexts[i] = nil, nil
	}
}

func (miter *lsmiterator) nextnode() (node Node) {
	if miter.merge {
		// if lsm is used for merging two orders, then return deleted nodes.
		// as well.
		node = miter.next()
	} else {
		// else if lsm is used for quering active entries, then fileter
		// deleted nodes.
		for node = miter.next(); node != nil && node.IsDeleted(); {
			node = miter.next()
		}
	}
	return
}

func (miter *lsmiterator) next() Node {
	node, iterator := miter.nexts[0], miter.iterators[0]

	// figure out the nextnode.

	if node == nil {
		return nil // no more left

	} else if nextnode := iterator.Next(); nextnode == nil {
		// this iterator is fully exhausted, move it to tail.
		copy(miter.iterators, miter.iterators[1:])
		copy(miter.nexts, miter.nexts[1:])
		till := len(miter.iterators) - 1
		miter.iterators[till], miter.nexts[till] = iterator, nil

	} else {
		// compare first-iterators nextnode's sort position with rest of the
		// iterator's nextnode.
		for till, iternode := range miter.nexts[1:] {
			if iternode != nil {
				cmpafter, cmpts := miter.compare(nextnode, iternode)
				if cmpafter > 0 {
					// a. first iterator's node sort after, swap and continue
					nextnode, iterator = miter.swapiter(0, till)
					continue
				} else if cmpafter < 0 {
					// b. first iterator's node is the smallest.
					break
				}
				// same key
				if cmpts < 0 { // nextnode is older than iternode
					nextnode, iterator = miter.swapiter(0, till)
				}
				miter.nexts[till] = miter.iterators[till].Next()
				continue
			}
			// c. remaining iterators are exhaused.
			break
		}
	}
	return node
}

func (miter *lsmiterator) swapiter(i, j int) (Node, IndexIterator) {
	x, y := miter.nexts[j], miter.iterators[j]
	miter.nexts[j], miter.iterators[j] = miter.nexts[i], miter.iterators[i]
	miter.nexts[i], miter.iterators[i] = x, y
	return miter.nexts[i], miter.iterators[i]
}

func (miter *lsmiterator) compare(nodei, nodej Node) (cmpafter, cmpts int) {
	keyi, keyj := nodei.Key(), nodej.Key()
	if miter.reverse {
		if cmpafter = bytes.Compare(keyj, keyi); cmpafter != 0 {
			return cmpafter, cmpafter
		}
	} else {
		if cmpafter = bytes.Compare(keyi, keyj); cmpafter != 0 {
			return cmpafter, cmpafter
		}
	}
	seqnoi, deadi := nodei.Bornseqno(), nodei.Deadseqno()
	seqnoj, deadj := nodej.Bornseqno(), nodej.Deadseqno()
	if seqnoi < deadi {
		seqnoi = deadi // nodei is already marked deleted
	}
	if seqnoj < deadj {
		seqnoj = deadj // nodej is already marked deleted
	}
	if seqnoi < seqnoj {
		return 0, -1
	} else if seqnoi > seqnoj {
		return 0, 1
	}
	panic("impossible situation, call the programmer!")
}

func (miter *lsmiterator) Len() int {
	return len(miter.iterators)
}

func (miter *lsmiterator) Less(i, j int) bool {
	nodei, nodej := miter.nexts[i], miter.nexts[j]
	if nodei == nil {
		return false
	} else if nodej == nil {
		return true
	}
	keyi, keyj := nodei.Key(), nodej.Key()
	return bytes.Compare(keyj, keyi) < 0
}

func (miter *lsmiterator) Swap(i, j int) {
	miter.iterators[i], miter.iterators[j] =
		miter.iterators[j], miter.iterators[i]
	miter.nexts[i], miter.nexts[j] = miter.nexts[j], miter.nexts[i]
}
