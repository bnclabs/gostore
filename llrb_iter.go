package storage

import "sync/atomic"

type iterStackItem struct {
	left, right bool
	nd          *Llrbnode
}

type llrbIterator struct {
	llrb       *LLRB
	endkey     []byte // can be highkey or lowkey
	cmp        int
	reverse    bool
	stack      [128]iterStackItem
	stackoff   int
	closed     bool
	activeiter *int64
}

func (iter *llrbIterator) buildstack(key []byte) bool {
	for i := 0; i < 128; i++ {
		iter.stack[i].left, iter.stack[i].right = false, false
		iter.stack[i].nd = nil
	}

	nd, off := iter.llrb.root, 0
	for nd != nil {
		iter.stack[off].nd = nd
		if key == nil && iter.reverse {
			iter.stack[off].right = true
			nd = nd.right
		} else if key == nil {
			iter.stack[off].left = true
			nd = nd.left
		} else if nd.gtkey(iter.llrb.mdsize, key) {
			iter.stack[off].left = true
			nd = nd.left
		} else if nd.ltkey(iter.llrb.mdsize, key) {
			iter.stack[off].right = true
			nd = nd.right
		} else {
			if iter.reverse {
				iter.stack[off].right = true
			} else {
				iter.stack[off].left = true
			}
			break
		}
		off++
	}
	if nd != nil {
		iter.stack[off].nd = nd
		iter.stackoff = off
		return true
	}
	iter.stackoff = off - 1
	return false
}

// Next implement IndexIterator{} interface.
func (iter *llrbIterator) Next() Node {
	if iter.closed {
		panic("cannot iterate over a closed iterator")
	}
	if iter.stackoff < 0 {
		return nil
	}

	nd := iter.stack[iter.stackoff].nd

	if iter.reverse {
		if iter.stack[iter.stackoff].right == false && nd.right != nil {
			iter.stack[iter.stackoff].right = true
			iter.stackoff++
			iter.stack[iter.stackoff].left = false
			iter.stack[iter.stackoff].right = false
			iter.stack[iter.stackoff].nd = nd.right

		} else if iter.stack[iter.stackoff].right == false {
			iter.stack[iter.stackoff].right = true
			iter.stack[iter.stackoff].left = true
			if iter.stack[iter.stackoff].left == false && nd.left != nil {
				iter.stackoff++
				iter.stack[iter.stackoff].left = false
				iter.stack[iter.stackoff].right = false
				iter.stack[iter.stackoff].nd = nd.left

			} else if iter.stack[iter.stackoff].left == false {
				iter.stackoff--
			}
		}

	} else {
		nd = iter.stack[iter.stackoff].nd
		if iter.stack[iter.stackoff].left == false && nd.left != nil {
			iter.stack[iter.stackoff].left = true
			iter.stackoff++
			iter.stack[iter.stackoff].left = false
			iter.stack[iter.stackoff].right = false
			iter.stack[iter.stackoff].nd = nd.left

		} else if iter.stack[iter.stackoff].left == false {
			iter.stack[iter.stackoff].left = true
			iter.stack[iter.stackoff].right = true
			if iter.stack[iter.stackoff].right == false && nd.right != nil {
				iter.stackoff++
				iter.stack[iter.stackoff].left = false
				iter.stack[iter.stackoff].right = false
				iter.stack[iter.stackoff].nd = nd.right

			} else if iter.stack[iter.stackoff].right == false {
				iter.stackoff--
			}
		}
	}
	return nd
}

// Close implement IndexIterator{} interface.
func (iter *llrbIterator) Close() {
	iter.closed = true
	atomic.AddInt64(iter.activeiter, -1)
}
