package api

import "bytes"
import "sort"

type lsmiterator struct {
	iterators []IndexIterator
	nexts     []Node
	reverse   bool
	node      Node
}

func LSMIterators(reverse bool, iterators ...IndexIterator) IndexIterator {
	miter := &lsmiterator{
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
		miter.node = miter.nextnotdeleted()
	}
	return miter
}

func (miter *lsmiterator) Next() (n Node) {
	if miter.node == nil {
		return nil
	}
	n, miter.node = miter.node, miter.nextnotdeleted()
	for {
		if miter.node == nil {
			return n
		}
		cmpkey, cmpts := miter.compare(n, miter.node)
		switch {
		case cmpkey > 0:
			panic("impossible situation, call the programmer")
		case cmpkey == 0:
			if cmpts < 0 {
				n = miter.node
			}
			miter.node = miter.nextnotdeleted()
		default:
			return n
		}
	}
	panic("unreachable code")
}

func (miter *lsmiterator) next() Node {
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
			if iternode != nil && miter.after(nextkey, iternode.Key()) {
				continue
			}
			break
		}
		if iternode != nil && miter.after(nextkey, iternode.Key()) {
			till++
		}
		copy(miter.iterators, miter.iterators[1:till+1])
		copy(miter.nexts, miter.nexts[1:till+1])
		miter.iterators[till], miter.nexts[till] = iterator, nextnode
	}
	return node
}

func (miter *lsmiterator) nextnotdeleted() (node Node) {
	for node = miter.next(); node != nil && node.IsDeleted(); {
		node = miter.next()
	}
	return
}

func (miter *lsmiterator) Close() {
	for i, it := range miter.iterators {
		it.Close()
		miter.iterators[i], miter.nexts[i] = nil, nil
	}
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

func (miter *lsmiterator) after(nextkey, key []byte) bool {
	if miter.reverse {
		return bytes.Compare(nextkey, key) < 0
	}
	return bytes.Compare(nextkey, key) > 0
}

func (miter *lsmiterator) compare(nodei, nodej Node) (cmpkey, cmpts int) {
	keyi, keyj := nodei.Key(), nodej.Key()
	if miter.reverse {
		if cmpkey = bytes.Compare(keyj, keyi); cmpkey != 0 {
			return cmpkey, cmpkey
		}
	} else {
		if cmpkey = bytes.Compare(keyi, keyj); cmpkey != 0 {
			return cmpkey, cmpkey
		}
	}
	seqnoi, deadi := nodei.Bornseqno(), nodei.Deadseqno()
	seqnoj, deadj := nodej.Bornseqno(), nodej.Deadseqno()
	if seqnoi < deadi {
		seqnoi = deadi
	}
	if seqnoj < deadj {
		seqnoj = deadj
	}
	if seqnoi < seqnoj {
		return 0, -1
	} else if seqnoi == seqnoj {
		return 0, 0
	}
	return 0, 1
}
