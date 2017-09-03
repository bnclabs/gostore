// llrb methods in this file are safe to be shared between
// single threaded and mvcc flavours.

package llrb

import "bytes"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

// lift Get() call, either from LLRB or from LLRBSnapshot.
func getkey(
	llrb *LLRB, root *Llrbnode,
	key []byte, callb api.NodeCallb) (*Llrbnode, bool) {

	nd := root
	for nd != nil {
		if nd.gtkey(llrb.mdsize, key, false) {
			nd = nd.left
		} else if nd.ltkey(llrb.mdsize, key, false) {
			nd = nd.right
		} else {
			if callb != nil {
				callb(llrb, 0, nd, nd, nil)
			}
			return nd, true
		}
	}
	if callb != nil { // and nd == nil
		callb(llrb, 0, nil, nil, api.ErrorKeyMissing)
	}
	return nd, false
}

func fixrangeargs(lk, hk []byte, incl string) ([]byte, []byte, string, bool) {
	if len(lk) == 0 {
		lk = nil
	}
	if len(hk) == 0 {
		hk = nil
	}
	if lk != nil && hk != nil && bytes.Compare(lk, hk) == 0 {
		if incl == "none" {
			return lk, hk, incl, true
		} else if incl == "low" || incl == "high" {
			return lk, hk, "both", false
		}
	}
	return lk, hk, incl, false
}

func dorange(
	llrb *LLRB, root *Llrbnode,
	lkey, hkey []byte, incl string, r bool, callb api.NodeCallb) {

	if r {
		switch incl {
		case "both":
			llrb.rvrslehe(root, lkey, hkey, callb)
		case "high":
			llrb.rvrsleht(root, lkey, hkey, callb)
		case "low":
			llrb.rvrslthe(root, lkey, hkey, callb)
		default:
			llrb.rvrsltht(root, lkey, hkey, callb)
		}

	} else {
		switch incl {
		case "both":
			llrb.rangehele(root, lkey, hkey, callb)
		case "high":
			llrb.rangehtle(root, lkey, hkey, callb)
		case "low":
			llrb.rangehelt(root, lkey, hkey, callb)
		default:
			llrb.rangehtlt(root, lkey, hkey, callb)
		}
	}
}

func inititerator(
	llrb *LLRB, reader api.IndexReader,
	lk, hk []byte, incl string, r bool) api.IndexIterator {

	// NOTE: always re-initialize, because we are getting it back from pool.
	iter := llrb.getiterator()
	iter.tree, iter.llrb = reader, llrb
	iter.continuate = false
	iter.nodes, iter.index, iter.limit = iter.nodes[:0], 0, startlimit
	// startkey
	iter.startkey = lib.Fixbuffer(iter.startkey, 256 /*typical keysize*/)
	n := copy(iter.startkey, lk)
	iter.startkey = iter.startkey[:n]
	// endkey
	iter.endkey = lib.Fixbuffer(iter.endkey, 256 /*typical keysize*/)
	n = copy(iter.endkey, hk)
	iter.endkey = iter.endkey[:n]
	// other params
	iter.incl, iter.reverse = incl, r
	iter.closed = false
	// get n_activeiter address
	switch tree := reader.(type) {
	case *LLRB:
		iter.n_activeiter = &tree.n_activeiter
	case *LLRBSnapshot:
		iter.n_activeiter = &tree.n_activeiter
	default:
		panic("unreachable code")
	}

	iter.rangefill()

	if r {
		switch iter.incl {
		case "none":
			iter.incl = "high"
		case "low":
			iter.incl = "both"
		}
	} else {
		switch iter.incl {
		case "none":
			iter.incl = "low"
		case "high":
			iter.incl = "both"
		}
	}
	// If iteration is called on snapshot, hold a reference.
	if snapshot, ok := iter.tree.(*LLRBSnapshot); ok {
		snapshot.Refer()
	}
	return iter
}

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
