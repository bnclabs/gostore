// llrb methods in this file are safe to be shared between
// single threaded and mvcc flavours.

package llrb

import "fmt"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

// low <= (keys) <= high
func (llrb *LLRB) rangehele(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rangehele(nd.left, lk, hk, callb)
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rangehele(nd.right, lk, hk, callb)
	}
	if !llrb.rangehele(nd.left, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rangehele(nd.right, lk, hk, callb)
}

// low <= (keys) < hk
func (llrb *LLRB) rangehelt(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rangehelt(nd.left, lk, hk, callb)
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rangehelt(nd.right, lk, hk, callb)
	}
	if !llrb.rangehelt(nd.left, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rangehelt(nd.right, lk, hk, callb)
}

// low < (keys) <= hk
func (llrb *LLRB) rangehtle(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rangehtle(nd.left, lk, hk, callb)
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rangehtle(nd.right, lk, hk, callb)
	}
	if !llrb.rangehtle(nd.left, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rangehtle(nd.right, lk, hk, callb)
}

// low < (keys) < hk
func (llrb *LLRB) rangehtlt(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rangehtlt(nd.left, lk, hk, callb)
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rangehtlt(nd.right, lk, hk, callb)
	}
	if !llrb.rangehtlt(nd.left, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rangehtlt(nd.right, lk, hk, callb)
}

// high >= (keys) >= low
func (llrb *LLRB) rvrslehe(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rvrslehe(nd.right, lk, hk, callb)
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rvrslehe(nd.left, lk, hk, callb)
	}
	if !llrb.rvrslehe(nd.right, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rvrslehe(nd.left, lk, hk, callb)
}

// high >= (keys) > low
func (llrb *LLRB) rvrsleht(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rvrsleht(nd.right, lk, hk, callb)
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rvrsleht(nd.left, lk, hk, callb)
	}
	if !llrb.rvrsleht(nd.right, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rvrsleht(nd.left, lk, hk, callb)
}

// high > (keys) >= low
func (llrb *LLRB) rvrslthe(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rvrslthe(nd.right, lk, hk, callb)
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rvrslthe(nd.left, lk, hk, callb)
	}
	if !llrb.rvrslthe(nd.right, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rvrslthe(nd.left, lk, hk, callb)
}

// high > (keys) > low
func (llrb *LLRB) rvrsltht(
	nd *Llrbnode, lk, hk []byte, callb api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rvrsltht(nd.right, lk, hk, callb)
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rvrsltht(nd.left, lk, hk, callb)
	}
	if !llrb.rvrsltht(nd.right, lk, hk, callb) {
		return false
	}
	if callb != nil && !callb(llrb, 0, nd, nd, nil) {
		return false
	}
	return llrb.rvrsltht(nd.left, lk, hk, callb)
}

// costly operation, don't call this on active trees.
func (llrb *LLRB) treecheck(
	nd *Llrbnode, depth int64, h *lib.HistogramInt64, count int64) int64 {

	if nd != nil {
		h.Add(depth)
		if !isred(nd) {
			count++
		}

		x := llrb.treecheck(nd.left, depth+1, h, count)
		y := llrb.treecheck(nd.right, depth+1, h, count)
		if x != y {
			fmsg := "invariant failed, no. of blacks : {%v,%v}"
			panic(fmt.Errorf(fmsg, x, y))
		}
		return x
	}
	return count
}
