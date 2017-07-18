// Package lsm implements log structured merge between two or more indexes.
// LSM algorithm uses following attributes for merging entries:
//
//  * If same key is found in more than one index, one with higher born-seqno
//    or higher dead-seqno is picked.
//  * After the lastest mutation on the key is picked remaining entries for
//    the same key are de-duplicated.
//  * If entry is found deleted, after applying above logic, it won't be
//    included in output of LSMRange. But included in output of LSMMerge.
package lsm

import "bytes"
import "sort"
import "strings"
import "fmt"

import "github.com/prataprc/gostore/api"

// TODO: Implement LSMPool as make(chan *lsmiterator, N) to recycle
// lsmiterator object.

type lsmiterator struct {
	merge     bool
	iterators []api.IndexIterator
	nexts     []api.Node
	reverse   bool
}

// LSMRange take one or more input iterators, where each iterator return items
// in sort order, and return a new iterator that can merge results from
// input iterators and return items in sort order.
func LSMRange(reverse bool, iterators ...api.IndexIterator) api.IndexIterator {
	return newLSMIterator(reverse, false /*merge*/, iterators...)
}

// LSMMerge is same as LSMRange, except that deleted items from input
// iterators are included in the output.
func LSMMerge(reverse bool, iterators ...api.IndexIterator) api.IndexIterator {
	return newLSMIterator(reverse, true /*merge*/, iterators...)
}

func newLSMIterator(
	reverse, merge bool, iterators ...api.IndexIterator) api.IndexIterator {

	miter := &lsmiterator{merge: merge, reverse: reverse}
	if miter.iterators == nil || cap(miter.iterators) < len(iterators) {
		miter.iterators = make([]api.IndexIterator, len(iterators))
		miter.nexts = make([]api.Node, len(iterators))
	}
	miter.iterators, miter.nexts = miter.iterators[:0], miter.nexts[:0]

	for _, iterator := range iterators {
		if iterator == nil {
			continue
		}
		miter.iterators = append(miter.iterators, iterator)
		miter.nexts = append(miter.nexts, iterator.Next())
	}
	if len(miter.iterators) == 0 {
		miter.Close()
		return nil
	}
	return miter.sort().dedup()
}

// Next implement api.IndexIterator interface.
func (miter *lsmiterator) Next() (n api.Node) {
	if miter == nil {
		//fmt.Println("Next", nil)
		return nil
	}
	n = miter.nextnode()
	//if n != nil {
	//	fmt.Printf("Next %q-%v-%v\n", n.Key(), n.Bornseqno(), n.Deadseqno())
	//}
	return n
}

// Close implement api.IndexIterator interface.
func (miter *lsmiterator) Close() {
	if miter == nil {
		return
	}
	for i, it := range miter.iterators {
		it.Close()
		miter.iterators[i], miter.nexts[i] = nil, nil
	}
}

func (miter *lsmiterator) sort() *lsmiterator {
	if miter.reverse {
		sort.Sort(sort.Reverse(miter))
	} else {
		sort.Sort(miter)
	}
	//miter.printnexts("sort")
	return miter
}

func (miter *lsmiterator) nextnode() (node api.Node) {
	if miter.merge {
		// if lsm is used for merging two orders, then return deleted nodes.
		// as well.
		node = miter.next()

	} else {
		// else if lsm is used for quering active entries, then filter
		// deleted nodes.
		for node = miter.next(); node != nil && node.IsDeleted(); {
			node = miter.next()
		}
	}
	return
}

func (miter *lsmiterator) next() api.Node {
	if miter.nexts[0] == nil {
		return nil
	}
	node := miter.nexts[0]
	//fmsg := "Next %s-%v-%v-%v\n"
	//key, bseqno, dseqno := node.Key(), node.Bornseqno(), node.Deadseqno()
	//fmt.Printf(fmsg, key, bseqno, dseqno, node.IsDeleted())
	miter.nexts[0] = miter.iterators[0].Next()
	firstnode, iterator := miter.nexts[0], miter.iterators[0]
	if firstnode == nil {
		if len(miter.iterators) > 1 {
			// this iterator is fully exhausted, move it to tail.
			copy(miter.iterators, miter.iterators[1:])
			copy(miter.nexts, miter.nexts[1:])
			till := len(miter.iterators) - 1
			miter.iterators[till], miter.nexts[till] = iterator, nil
		}
	}
	if len(miter.iterators) > 1 && miter.nexts[1] != nil {
		// check whether the sort order has changed.
		cmpafter := miter.compare(miter.nexts[0], miter.nexts[1])
		if cmpafter >= 0 { // if so,
			miter.sort().dedup()
		}
	}
	return node
}

func (miter *lsmiterator) dedup() *lsmiterator {
	var till, cmpafter int
	var iternode api.Node

	firstnode := miter.nexts[0]
	if firstnode != nil && len(miter.iterators) > 1 {
		for till, iternode = range miter.nexts[1:] {
			if iternode != nil {
				cmpafter = miter.compare(firstnode, iternode)
				if cmpafter == 0 { // same key
					miter.nexts[till+1] = miter.iterators[till+1].Next()
					continue
				}
				break
			}
		}
		if till > 0 {
			miter.sort()
		}
	}
	//miter.printnexts("dedp")
	return miter
}

func (miter *lsmiterator) compare(nodei, nodej api.Node) int {
	keyi, keyj := nodei.Key(), nodej.Key()
	if miter.reverse {
		return bytes.Compare(keyj, keyi)
	}
	return bytes.Compare(keyi, keyj)
}

// Len implement sort.Sort interface{}
func (miter *lsmiterator) Len() int {
	return len(miter.iterators)
}

// Less implement sort.Sort interface{}
func (miter *lsmiterator) Less(i, j int) bool {
	nodei, nodej := miter.nexts[i], miter.nexts[j]
	if miter.reverse {
		if nodei == nil {
			return true
		} else if nodej == nil {
			return false
		}
	} else {
		if nodei == nil {
			return false
		} else if nodej == nil {
			return true
		}
	}
	keyi, keyj := nodei.Key(), nodej.Key()
	cmp := bytes.Compare(keyi, keyj)
	if cmp < 0 {
		return true
	} else if cmp > 0 {
		return false
	}
	/* cmp == 0 */
	seqnoi, deadi := nodei.Bornseqno(), nodei.Deadseqno()
	seqnoj, deadj := nodej.Bornseqno(), nodej.Deadseqno()
	if seqnoi < deadi {
		seqnoi = deadi // nodei is already marked deleted
	}
	if seqnoj < deadj {
		seqnoj = deadj // nodej is already marked deleted
	}
	if miter.reverse {
		if seqnoj > seqnoi {
			return true
		}
		return false
	}
	if seqnoi > seqnoj {
		return true
	}
	return false
}

// Swap implement sort.Sort interface{}
func (miter *lsmiterator) Swap(i, j int) {
	miter.iterators[i], miter.iterators[j] =
		miter.iterators[j], miter.iterators[i]
	miter.nexts[i], miter.nexts[j] = miter.nexts[j], miter.nexts[i]
}

//---- debug functions

func (miter *lsmiterator) printnexts(prefix string) {
	ss := []string{}
	for _, next := range miter.nexts {
		if next == nil {
			ss = append(ss, "nil")
		} else if next.IsDeleted() {
			fmsg := "%s-%s-%v-D"
			ss = append(ss,
				fmt.Sprintf(fmsg, next.Key(), next.Value(), next.Deadseqno()))
		} else {
			fmsg := "%s-%s-%v-U"
			ss = append(ss,
				fmt.Sprintf(fmsg, next.Key(), next.Value(), next.Bornseqno()))
		}
	}
	fmt.Printf("%s %s\n", prefix, strings.Join(ss, " "))
}
