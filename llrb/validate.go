package llrb

import "math"
import "fmt"
import "bytes"
import "errors"
import "sync/atomic"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/lib"
import gohumanize "github.com/dustin/go-humanize"

// height of the tree cannot exceed a certain limit. For example if the tree
// holds 1-million entries, a fully balanced tree shall have a height of 20
// levels. maxheight provide some breathing space on top of ideal height.
func maxheight(entries int64) float64 {
	if entries < 5 {
		return (3 * (math.Log2(float64(entries)) + 1)) // 3x breathing space.
	}
	return 2 * math.Log2(float64(entries)) // 2x breathing space
}

// in mvcc mode, a single mutation op can create several garbage nodes due to
// CoW, set an upper limit on that.
func maxreclaim(entries int64) float64 {
	return 7 * math.Log2(float64(entries)) // 7x the height
}

// in mvcc mode, a single mutation op can create several garbage nodes due to
// CoW, set an average limit on that.
func meanreclaim(entries int64) float64 {
	return 3 * math.Log2(float64(entries)) // 3x the height
}

// LLRB rule, from sedgewick's paper.
var redafterred = errors.New("consecutive red spotted")

// LLRB rule, from sedgewick's paper.
func unbalancedblacks(lblacks, rblacks int64) error {
	return fmt.Errorf("unbalancedblacks {%v,%v}", lblacks, rblacks)
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
		entries := llrb.Count()
		if float64(h.Max()) > maxheight(entries) {
			fmsg := "validate(): max height %v exceeds <factor>*log2(%v)"
			panic(fmt.Errorf(fmsg, float64(h.Max()), entries))
		}
	}

	llrb.validatemem()
	llrb.validatestats()
}

func (llrb *LLRB) validatemem() {
	stats := llrb.statsval(llrb.statskey(make(map[string]interface{})))
	entries := llrb.Count()
	memuz := llrb.memutilization
	if memuz < 0.01 { // skip if less than 1%
		return
	}

	meminfo := func(args ...string) string {
		vals := []interface{}{entries}
		for _, arg := range args {
			vals = append(vals, gohumanize.Bytes(uint64(stats[arg].(int64))))
		}
		fmsg := "entries(%v/%v): cap: %v {heap:%v,alloc:%v,overhd:%v}"
		return fmt.Sprintf(fmsg, vals...)
	}

	memory := float64(llrb.keymemory)
	heap := float64(stats["node.heap"].(int64))
	ratio := memory / heap
	if ratio < memuz {
		info := meminfo(
			"keymemory", "node.capacity", "node.heap", "node.alloc",
			"node.overhead",
		)
		log.Infof("%v %v\n", llrb.logprefix, info)
		panic(fmt.Errorf("%v/%v=%.2f < %v", memory, heap, ratio*100, memuz*100))
	}

	memory = float64(llrb.valmemory)
	heap = float64(stats["value.heap"].(int64))
	ratio = memory / heap
	if ratio < memuz {
		info := meminfo(
			"valmemory", "value.capacity", "value.heap", "value.alloc",
			"value.overhead",
		)
		log.Infof("%v %v\n", llrb.logprefix, info)
		panic(fmt.Errorf("%v/%v=%.2f < %v", memory, heap, ratio*100, memuz*100))
	}
}

func (llrb *LLRB) validatetree(
	nd *Llrbnode, fromred bool, blacks, depth int64,
	h *lib.HistogramInt64) (nblacks, keymem, valmem int64) {

	if nd != nil {
		h.Add(depth)
		if fromred && isred(nd) {
			panic(redafterred)
		}
		if !isred(nd) {
			blacks++
		}

		lblacks, lkm, lvm := llrb.validatetree(
			nd.left, isred(nd), blacks, depth+1, h)
		rblacks, rkm, rvm := llrb.validatetree(
			nd.right, isred(nd), blacks, depth+1, h)

		if lblacks != rblacks {
			panic(unbalancedblacks(lblacks, rblacks))
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

func (llrb *LLRB) validatestats() error {
	// n_count should match (n_inserts - n_deletes)
	n_count := llrb.n_count
	n_inserts, n_deletes := llrb.n_inserts, llrb.n_deletes
	if n_count != (n_inserts - n_deletes) {
		fmsg := "validatestats(): n_count:%v != (n_inserts:%v - n_deletes:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_inserts, n_deletes))
	}
	// n_nodes should match n_inserts
	n_nodes := llrb.n_nodes
	if n_inserts != n_nodes {
		fmsg := "validatestats(): n_inserts:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_inserts, n_nodes))
	}
	// n_deletes + reclaim should match (n_frees - n_clones)
	total_reclaim := int64(len(llrb.mvcc.reclaim))
	total_reclaim += llrb.mvcc.snapshot.countreclaimnodes()
	n_frees, n_clones := llrb.n_frees, llrb.n_clones
	if lib.AbsInt64(n_deletes-total_reclaim) != lib.AbsInt64(n_clones-n_frees) {
		fmsg := "validatestats(): abs(n_deletes:%v - reclaim:%v) != " +
			"abs(n_clones:%v - n_frees:%v)"
		panic(fmt.Errorf(fmsg, n_deletes, total_reclaim, n_clones, n_frees))
	}
	// n_snapshots should match (n_activess + n_purgedss)
	n_snapshots := atomic.LoadInt64(&llrb.n_snapshots)
	n_purgedss := atomic.LoadInt64(&llrb.n_purgedss)
	n_activess := atomic.LoadInt64(&llrb.n_activess)
	if n_snapshots != (n_purgedss + n_activess) {
		fmsg := "validatestats(): " +
			"n_snapshots:%v != (n_activess:%v + n_purgedss:%v)"
		panic(fmt.Errorf(fmsg, n_snapshots, n_activess, n_purgedss))
	}

	entries := llrb.Count()
	for k, h_reclaim := range llrb.mvcc.h_reclaims {
		if max := float64(h_reclaim.Max()); max > 0 {
			if max > maxreclaim(entries) {
				fmsg := "validatestats(): max %v reclaim %v exceeds 7*log2(%v)"
				panic(fmt.Errorf(fmsg, k, max, entries))
			}
		}
		if mean := float64(h_reclaim.Mean()); mean > 0 {
			if mean > meanreclaim(entries) {
				fmsg := "validatestats(): mean %v reclaim %v exceeds 3*log2(%v)"
				panic(fmt.Errorf(fmsg, k, mean, entries))
			}
		}
	}

	return nil
}
