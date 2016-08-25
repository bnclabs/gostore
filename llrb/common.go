// llrb methods in this file are safe to be shared between
// single threaded and mvcc flavours.

package llrb

import "fmt"
import "math"
import "bytes"

import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

// low <= (keys) <= high
func (llrb *LLRB) rangehele(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rangehele(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rangehele(nd.right, lk, hk, iter)
	}
	if !llrb.rangehele(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rangehele(nd.right, lk, hk, iter)
}

// low <= (keys) < hk
func (llrb *LLRB) rangehelt(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {

	if nd == nil {
		return true
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rangehelt(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rangehelt(nd.right, lk, hk, iter)
	}
	if !llrb.rangehelt(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rangehelt(nd.right, lk, hk, iter)
}

// low < (keys) <= hk
func (llrb *LLRB) rangehtle(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {
	if nd == nil {
		return true
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rangehtle(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rangehtle(nd.right, lk, hk, iter)
	}
	if !llrb.rangehtle(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rangehtle(nd.right, lk, hk, iter)
}

// low < (keys) < hk
func (llrb *LLRB) rangehtlt(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {
	if nd == nil {
		return true
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rangehtlt(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rangehtlt(nd.right, lk, hk, iter)
	}
	if !llrb.rangehtlt(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rangehtlt(nd.right, lk, hk, iter)
}

// high >= (keys) >= low
func (llrb *LLRB) rvrslehe(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {
	if nd == nil {
		return true
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rvrslehe(nd.right, lk, hk, iter)
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rvrslehe(nd.left, lk, hk, iter)
	}
	if !llrb.rvrslehe(nd.right, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rvrslehe(nd.left, lk, hk, iter)
}

// high >= (keys) > low
func (llrb *LLRB) rvrsleht(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {
	if nd == nil {
		return true
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rvrsleht(nd.right, lk, hk, iter)
	}
	if hk != nil && nd.gtkey(llrb.mdsize, hk, true) {
		return llrb.rvrsleht(nd.left, lk, hk, iter)
	}
	if !llrb.rvrsleht(nd.right, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rvrsleht(nd.left, lk, hk, iter)
}

// high > (keys) >= low
func (llrb *LLRB) rvrslthe(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {
	if nd == nil {
		return true
	}
	if lk != nil && nd.ltkey(llrb.mdsize, lk, true) {
		return llrb.rvrslthe(nd.right, lk, hk, iter)
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rvrslthe(nd.left, lk, hk, iter)
	}
	if !llrb.rvrslthe(nd.right, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rvrslthe(nd.left, lk, hk, iter)
}

// high > (keys) > low
func (llrb *LLRB) rvrsltht(nd *Llrbnode, lk, hk []byte, iter api.NodeCallb) bool {
	if nd == nil {
		return true
	}
	if lk != nil && nd.lekey(llrb.mdsize, lk, true) {
		return llrb.rvrsltht(nd.right, lk, hk, iter)
	}
	if hk != nil && nd.gekey(llrb.mdsize, hk, true) {
		return llrb.rvrsltht(nd.left, lk, hk, iter)
	}
	if !llrb.rvrsltht(nd.right, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(llrb, 0, nd, nd) {
		return false
	}
	return llrb.rvrsltht(nd.left, lk, hk, iter)
}

func (llrb *LLRB) validate(root *Llrbnode) {
	h := lib.NewhistorgramInt64(1, 256, 1)

	_, km, vm := llrb.validatetree(root, isred(root), 0 /*blck*/, 1 /*dep*/, h)
	if km != llrb.keymemory {
		fmsg := "validate(): keymemory:%v != actual:%v"
		panic(fmt.Errorf(fmsg, llrb.keymemory, km))
	} else if vm != llrb.valmemory {
		fmsg := "validate(): valmemory:%v != actual:%v"
		panic(fmt.Errorf(fmsg, llrb.valmemory, vm))
	}

	// `h_height`.max should not exceed certain limit
	if h.Samples() > 8 {
		nf := float64(llrb.Count())
		if float64(h.Max()) > (3 * (math.Log2(nf) + 1)) {
			fmsg := "validate(): max height %v exceeds log2(%v)"
			panic(fmt.Errorf(fmsg, float64(h.Max()), nf))
		}
	}

	llrb.validatemem()
	llrb.validatestats()
}

func (llrb *LLRB) validatemem() {
	stats := llrb.statsmem(make(map[string]interface{}))
	memory := float64(llrb.keymemory)
	allocated := float64(stats["node.allocated"].(int64))
	ratio := memory / allocated
	if ratio < llrb.memratio {
		fmsg := "validatemem(): ratio: %v {%v/%v}"
		panic(fmt.Errorf(fmsg, ratio, memory, allocated))
	}
	memory = float64(llrb.valmemory)
	allocated = float64(stats["value.allocated"].(int64))
	ratio = memory / allocated
	if ratio < llrb.memratio {
		fmsg := "validatemem(): ratio: %v {%v/%v}"
		panic(fmt.Errorf(fmsg, ratio, memory, allocated))
	}
}

func (llrb *LLRB) validatetree(
	nd *Llrbnode, fromred bool, blacks, depth int64,
	h *lib.HistogramInt64) (nblacks, keymem, valmem int64) {

	if nd != nil {
		h.Add(depth)
		if fromred && isred(nd) {
			panic("validate(): consequetive red spotted")
		}
		if !isred(nd) {
			blacks++
		}
		lblacks, lkm, lvm := llrb.validatetree(
			nd.left, isred(nd), blacks, depth+1, h)
		rblacks, rkm, rvm := llrb.validatetree(
			nd.right, isred(nd), blacks, depth+1, h)
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
		keymem = lkm + rkm + int64(len(nd.Key()))
		valmem = lvm + rvm + int64(len(nd.Value()))
		return lblacks, keymem, valmem
	}
	return blacks, 0, 0
}

func (llrb *LLRB) heightStats(nd *Llrbnode, depth int64, h *lib.HistogramInt64) {
	if nd == nil {
		return
	}
	h.Add(depth)
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
