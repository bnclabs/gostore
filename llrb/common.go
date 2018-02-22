package llrb

import "fmt"
import "math"
import "bytes"
import "errors"
import "unsafe"

import "github.com/bnclabs/gostore/lib"

type llrbstats struct { // TODO: add json tags.
	n_count   int64 // number of nodes in the tree
	n_inserts int64
	n_updates int64
	n_deletes int64
	n_nodes   int64
	n_frees   int64
	n_clones  int64
	n_txns    int64
	n_commits int64
	n_aborts  int64
	keymemory int64 // memory used by all keys
	valmemory int64 // memory used by all values

	// mvcc statistics
	n_reclaims  int64
	n_snapshots int64
	n_purgedss  int64
	n_activess  int64
	tm_lastsnap int64
	tm_snapmax  int64
}

// height of the tree cannot exceed a certain limit. For example if the tree
// holds 1-million entries, a fully balanced tree shall have a height of 20
// levels. maxheight provide some breathing space on top of ideal height.
func maxheight(entries int64) float64 {
	if entries == 0 {
		return 0
	} else if entries < 5 {
		return (3 * (math.Log2(float64(entries)) + 1)) // 3x breathing space.
	}
	return 2 * math.Log2(float64(entries)) // 2x breathing space
}

func validatetree(root *Llrbnode, logprefix string, n, kmem, vmem int64) {
	if root == nil {
		return
	}

	n_count, keymemory, valmemory := n, kmem, vmem

	h := lib.NewhistorgramInt64(1, 256, 1)
	blacks, depth, fromred := int64(0), int64(1), root.isred()
	nblacks, km, vm := validatellrbtree(root, fromred, blacks, depth, h)
	if km != keymemory {
		fmsg := "validate(): keymemory:%v != actual:%v"
		panic(fmt.Errorf(fmsg, keymemory, km))
	} else if vm != valmemory {
		fmsg := "validate(): valmemory:%v != actual:%v"
		panic(fmt.Errorf(fmsg, valmemory, vm))
	}
	if samples := h.Samples(); samples != n_count {
		fmsg := "expected h_height.samples:%v to be same as Count():%v"
		panic(fmt.Errorf(fmsg, samples, n_count))
	}
	infof("%v found %v blacks on both sides\n", logprefix, nblacks)
	// `h_height`.max should not exceed certain limit, maxheight
	// gives some breathing room.
	if h.Samples() > 8 {
		if float64(h.Max()) > maxheight(n_count) {
			fmsg := "validate(): max height %v exceeds <factor>*log2(%v)"
			panic(fmt.Errorf(fmsg, float64(h.Max()), n_count))
		}
	}
}

/*
following expectations on the tree should be met.
* If current node is red, parent node should be black.
* At each level, number of black-links on the left subtree should be
  equal to number of black-links on the right subtree.
* Make sure that the tree is in sort order.
* Return number of blacks, cummulative memory consumed by keys,
  cummulative memory consumed by values.
*/
func validatellrbtree(
	nd *Llrbnode, fromred bool, blacks, depth int64,
	h *lib.HistogramInt64) (nblacks, keymem, valmem int64) {

	if nd == nil {
		return blacks, 0, 0
	} else if nd.isdirty() {
		panic(errors.New("node cannot be dirty"))
	}

	h.Add(depth)
	if fromred && nd.isred() {
		panic(errors.New("consecutive red spotted"))
	}
	if !nd.isred() {
		blacks++
	}

	lblacks, lkm, lvm := validatellrbtree(
		nd.left, nd.isred(), blacks, depth+1, h)
	rblacks, rkm, rvm := validatellrbtree(
		nd.right, nd.isred(), blacks, depth+1, h)

	if lblacks != rblacks {
		fmsg := "unbalancedblacks Left:%v Right:%v}"
		panic(fmt.Errorf(fmsg, lblacks, rblacks))
	}

	key := nd.getkey()
	if nd.left != nil && bytes.Compare(nd.left.getkey(), key) >= 0 {
		fmsg := "validate(): sort order, left node %v is >= node %v"
		panic(fmt.Errorf(fmsg, nd.left.getkey(), key))
	}
	if nd.left != nil && bytes.Compare(nd.left.getkey(), key) >= 0 {
		fmsg := "validate(): sort order, node %v is >= right node %v"
		panic(fmt.Errorf(fmsg, nd.right.getkey(), key))
	}

	keymem = lkm + rkm + int64(len(nd.getkey()))
	valmem = lvm + rvm + int64(len(nd.Value()))
	return lblacks, keymem, valmem
}

//---- embed

type txnsmeta struct {
	records   chan *record
	cursors   chan *Cursor
	txncache  chan *Txn
	viewcache chan *View
}

func (meta *txnsmeta) inittxns() {
	maxtxns := 1000 // TODO: no magic number
	meta.txncache = make(chan *Txn, maxtxns)
	meta.viewcache = make(chan *View, maxtxns)
	meta.cursors = make(chan *Cursor, maxtxns*2)
	meta.records = make(chan *record, maxtxns*5)
}

func (meta *txnsmeta) gettxn(id uint64, db, snap interface{}) (txn *Txn) {
	select {
	case txn = <-meta.txncache:
	default:
		txn = newtxn(id, db, snap, meta.records, meta.cursors)
	}
	txn.db, txn.snapshot = db, snap
	if txn.id = id; txn.id == 0 {
		switch snap := txn.snapshot.(type) {
		case *LLRB:
			txn.id = (uint64)((uintptr)(unsafe.Pointer(snap.root)))
		case *mvccsnapshot:
			txn.id = (uint64)((uintptr)(unsafe.Pointer(snap.root)))
		}
	}
	return
}

func (meta *txnsmeta) puttxn(txn *Txn) {
	for index, head := range txn.writes { // free all records in this txn.
		for head != nil {
			next := head.next
			txn.putrecord(head)
			head = next
		}
		delete(txn.writes, index)
	}
	for _, cur := range txn.cursors {
		txn.putcursor(cur)
	}
	txn.cursors = txn.cursors[:0]
	select {
	case meta.txncache <- txn:
	default: // Left for GC
	}
}

func (meta *txnsmeta) getview(id uint64, db, snap interface{}) (view *View) {
	select {
	case view = <-meta.viewcache:
	default:
		view = newview(id, snap, meta.cursors)
	}
	view.id, view.snapshot = id, snap
	if view.id == 0 {
		switch snap := view.snapshot.(type) {
		case *LLRB:
			view.id = (uint64)((uintptr)(unsafe.Pointer(snap.root)))
		case *mvccsnapshot:
			view.id = (uint64)((uintptr)(unsafe.Pointer(snap.root)))
		}
	}
	return
}

func (meta *txnsmeta) putview(view *View) {
	for _, cur := range view.cursors {
		view.putcursor(cur)
	}
	view.cursors = view.cursors[:0]
	select {
	case meta.viewcache <- view:
	default: // Left for GC
	}
}
