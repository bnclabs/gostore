// llrb methods in this file are safe to be shared between
// single threaded and mvcc flavours.

package storage

import "fmt"
import "math"

// low <= (keys) <= high
func (llrb *LLRB) rangeFromFind(
	nd *Llrbnode, lk, hk []byte, iter NodeIterator) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(hk) {
		return llrb.rangeFromFind(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(lk) {
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
	if hk != nil && !nd.ltkey(hk) {
		return llrb.rangeFromTill(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(lk) {
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
	if hk != nil && nd.gtkey(hk) {
		return llrb.rangeAfterFind(nd.left, lk, hk, iter)
	}
	if lk != nil && !nd.gtkey(lk) {
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
	if hk != nil && !nd.ltkey(hk) {
		return llrb.rangeAfterTill(nd.left, lk, hk, iter)
	}
	if lk != nil && !nd.gtkey(lk) {
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

func (llrb *LLRB) heightStats(nd *Llrbnode, d int64, av *averageInt) {
	if nd == nil {
		return
	}
	d++
	av.add(d)
	if nd.left != nil {
		llrb.heightStats(nd.left, d, av)
	}
	if nd.right != nil {
		llrb.heightStats(nd.right, d, av)
	}
}

func (llrb *LLRB) countblacks(nd *Llrbnode, count int) int {
	if nd != nil {
		if !isred(nd) {
			count++
		}
		x := llrb.countblacks(nd.left, count)
		y := llrb.countblacks(nd.right, count)
		if x != y {
			panic(fmt.Errorf("no. of blacks {left,right} : {%v,%v}\n", x, y))
		}
		return x
	}
	return count
}

func (llrb *LLRB) validate(root *Llrbnode) {
	llrb.validatereds(root, isred(root))
	llrb.countblacks(root, 0)

	heightav := &averageInt{}
	llrb.validateheight(root, heightav)

	llrb.rangeFromFind(
		root, nil, nil,
		func(nd Node) bool {
			if nd.(*Llrbnode).metadata().isdirty() {
				panic("unexpected dirty node on a full tree scan")
			}
			return true
		})
}

func (llrb *LLRB) validatereds(nd *Llrbnode, fromred bool) {
	if nd != nil {
		if fromred && isred(nd) {
			panic("consequetive red spotted")
		}
		llrb.validatereds(nd.left, isred(nd))
		llrb.validatereds(nd.right, isred(nd))
	}
}

func (llrb *LLRB) validateheight(nd *Llrbnode, av *averageInt) bool {
	llrb.heightStats(nd, 0, av)
	nf := float64(llrb.Count())
	return float64(av.max()) < (3 * math.Log2(nf))
}
