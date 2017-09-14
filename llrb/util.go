package llrb

import "fmt"
import "math"
import "bytes"
import "errors"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/lib"

// height of the tree cannot exceed a certain limit. For example if the tree
// holds 1-million entries, a fully balanced tree shall have a height of 20
// levels. maxheight provide some breathing space on top of ideal height.
func maxheight(entries int64) float64 {
	if entries < 5 {
		return (3 * (math.Log2(float64(entries)) + 1)) // 3x breathing space.
	}
	return 2 * math.Log2(float64(entries)) // 2x breathing space
}

func validatetree(root *Llrbnode1, logprefix string, n, kmem, vmem int64) {
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
	log.Infof("%v found %v blacks on both sides\n", logprefix, nblacks)
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
// TODO: move this to file common to LLRB1 and MVCC.
func validatellrbtree(
	nd *Llrbnode1, fromred bool, blacks, depth int64,
	h *lib.HistogramInt64) (nblacks, keymem, valmem int64) {

	if nd == nil {
		return blacks, 0, 0
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
