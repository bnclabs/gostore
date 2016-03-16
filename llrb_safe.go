// llrb methods in this file are safe to be shared between
// single threaded and mvcc flavours.

package storage

import "fmt"
import "math"
import "bytes"

// low <= (keys) <= high
func (llrb *LLRB) rangeFromFind(
	nd *Llrbnode, lk, hk []byte, iter NodeIterator) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk) {
		return llrb.rangeFromFind(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk) {
		return llrb.rangeFromFind(nd.right, lk, hk, iter)
	}
	if !llrb.rangeFromFind(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd) {
		return false
	}
	return llrb.rangeFromFind(nd.right, lk, hk, iter)
}

// low <= (keys) < hk
func (llrb *LLRB) rangeFromTill(
	nd *Llrbnode, lk, hk []byte, iter NodeIterator) bool {

	if nd == nil {
		return true
	}
	if hk != nil && !nd.ltkey(llrb.mdsize, hk) {
		return llrb.rangeFromTill(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk) {
		return llrb.rangeFromTill(nd.right, lk, hk, iter)
	}
	if !llrb.rangeFromTill(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd) {
		return false
	}
	return llrb.rangeFromTill(nd.right, lk, hk, iter)
}

// low < (keys) <= hk
func (llrb *LLRB) rangeAfterFind(
	nd *Llrbnode, lk, hk []byte, iter NodeIterator) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk) {
		return llrb.rangeAfterFind(nd.left, lk, hk, iter)
	}
	if lk != nil && !nd.gtkey(llrb.mdsize, lk) {
		return llrb.rangeAfterFind(nd.right, lk, hk, iter)
	}
	if !llrb.rangeAfterFind(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd) {
		return false
	}
	return llrb.rangeAfterFind(nd.right, lk, hk, iter)
}

// low < (keys) < hk
func (llrb *LLRB) rangeAfterTill(
	nd *Llrbnode, lk, hk []byte, iter NodeIterator) bool {

	if nd == nil {
		return true
	}
	if hk != nil && !nd.ltkey(llrb.mdsize, hk) {
		return llrb.rangeAfterTill(nd.left, lk, hk, iter)
	}
	if lk != nil && !nd.gtkey(llrb.mdsize, lk) {
		return llrb.rangeAfterTill(nd.right, lk, hk, iter)
	}
	if !llrb.rangeAfterTill(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd) {
		return false
	}
	return llrb.rangeAfterTill(nd.right, lk, hk, iter)
}

func (llrb *LLRB) validate(root *Llrbnode) {
	h := newhistorgramInt64(1, 256, 1)
	llrb.validatetree(root, isred(root), 0 /*blacks*/, 1 /*depth*/, h)
	if h.samples() > 0 {
		nf := float64(llrb.Count())
		if float64(h.max()) > (3 * math.Log2(nf)) {
			fmsg := "validate(): max height %v exceeds log2(llrb.count) %v"
			panic(fmt.Errorf(fmsg, float64(h.max()), nf))
		}
	}

	//llrb.validatestats()
}

func (llrb *LLRB) validatetree(
	nd *Llrbnode, fromred bool, blacks, depth int64, h *histogramInt64) int64 {

	if nd != nil {
		h.add(depth)
		if fromred && isred(nd) {

			panic("validate(): consequetive red spotted")
		}
		if !isred(nd) {
			blacks++
		}
		lblacks := llrb.validatetree(nd.left, isred(nd), blacks, depth+1, h)
		rblacks := llrb.validatetree(nd.right, isred(nd), blacks, depth+1, h)
		if lblacks != rblacks {
			fmsg := "validate(): no. of blacks {left,right} is {%v,%v}\n"
			panic(fmt.Errorf(fmsg, lblacks, rblacks))
		}
		key := nd.Key()
		if nd.left != nil && bytes.Compare(nd.left.Key(), key) >= 0 {
			fmsg := "validate(): sort order, left node %v is >= node %v"
			panic(fmt.Errorf(fmsg, nd.left.Key(), key))
		}
		if nd.left != nil && bytes.Compare(nd.left.Key(), key) >= 0 {
			fmsg := "validate(): sort order, node %v is >= right node %v"
			panic(fmt.Errorf(fmsg, nd.right.Key(), key))
		}
		return lblacks
	}
	return blacks
}

func (llrb *LLRB) heightStats(nd *Llrbnode, depth int64, h *histogramInt64) {
	if nd == nil {
		return
	}
	h.add(depth)
	llrb.heightStats(nd.left, depth+1, h)
	llrb.heightStats(nd.right, depth+1, h)
}

func (llrb *LLRB) countblacks(nd *Llrbnode, count int) int {
	if nd != nil {
		if !isred(nd) {
			count++
		}
		x := llrb.countblacks(nd.left, count)
		y := llrb.countblacks(nd.right, count)
		if x != y {
			fmsg := "countblacks(): no. of blacks {left,right} : {%v,%v}"
			panic(fmt.Errorf(fmsg, x, y))
		}
		return x
	}
	return count
}
