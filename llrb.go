// Package llrb
//
// configuration:
//
//	"nodemem.minblock"
//  "nodemem.maxblock"
//  "nodemem.capacity"
//  "valmem.minblock"
//  "valmem.maxblock"
//  "valmem.capacity"

package llrb

import "fmt"
import "unsafe"
import "time"
import "bytes"
import "sync/atomic"

const MinKeymem = 96
const MaxKeymem = 4096
const MinValmem = 32
const MaxValmem = 10 * 1024 * 1024

// NdIterator callback function while ranging from
// low-key and high-key, return false to stop iteration.
type NdIterator func(nd *node) bool

type LLRB struct { // tree container
	nodearena *memarena
	valarena  *memarena
	root      unsafe.Pointer // root *node of LLRB tree
	config    map[string]interface{}
	// statistics
	count     int64 // number of nodes in the tree
	keymemory int64 // memory used by all keys
	valmemory int64 // memory used by all values
}

func NewLLRB(config map[string]interface{}) *LLRB {
	validateConfig(config)

	llrb := &LLRB{}

	minblock := int64(config["nodemem.minblock"].(int))
	maxblock := int64(config["nodemem.maxblock"].(int))
	capacity := int64(config["nodemem.capacity"].(int))
	llrb.nodearena = newmemarena(minblock, maxblock, capacity)
	minblock = int64(config["valmem.minblock"].(int))
	maxblock = int64(config["valmem.maxblock"].(int))
	capacity = int64(config["valmem.capacity"].(int))
	llrb.valarena = newmemarena(minblock, maxblock, capacity)

	llrb.config = config

	return llrb
}

//---- Maintanence APIs.

func (llrb *LLRB) SetRoot(r *node) {
	atomic.StorePointer(&llrb.root, unsafe.Pointer(r))
}

func (llrb *LLRB) Root() *node {
	return (*node)(atomic.LoadPointer(&llrb.root))
}

func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.count)
}

func (llrb *LLRB) Memory() int64 { // needs an Rlock
	return llrb.nodearena.memory() + llrb.valarena.memory()
}

func (llrb *LLRB) Allocated() int64 { // needs an Rlock
	return llrb.nodearena.allocated() + llrb.valarena.allocated()
}

func (llrb *LLRB) Available() int64 { // needs an Rlock
	return llrb.nodearena.available() + llrb.valarena.available()
}

func (llrb *LLRB) KeyMemory() int64 {
	return atomic.LoadInt64(&llrb.keymemory)
}

func (llrb *LLRB) ValueMemory() int64 {
	return atomic.LoadInt64(&llrb.valmemory)
}

func (llrb *LLRB) NodeBlocks() []int64 {
	return llrb.nodearena.blocksizes
}

func (llrb *LLRB) ValueBlocks() []int64 {
	return llrb.valarena.blocksizes
}

func (llrb *LLRB) Freenode(nd *node) {
	if nd != nil {
		nv := nd.nodevalue()
		if nv != nil {
			nv.pool.free(unsafe.Pointer(nv))
		}
		nd.pool.free(unsafe.Pointer(nd))
	}
}

func (llrb *LLRB) PPrint() {
	nd := llrb.Root()
	fmt.Printf("root: ")
	nd.pprint("  ")
}

//---- LLRB read operations.

func (llrb *LLRB) Has(key []byte) bool {
	nd := llrb.Get(key)
	return nd != nil
}

func (llrb *LLRB) Get(lookupkey []byte) (nd *node) {
	nd = (*node)(atomic.LoadPointer(&llrb.root))
	for nd != nil {
		if nd.gtkey(lookupkey) {
			nd = nd.left
		} else if nd.ltkey(lookupkey) {
			nd = nd.right
		} else {
			nd.settimestamp(time.Now().UnixNano())
			return nd
		}
	}
	return nil // key is not present in the tree
}

func (llrb *LLRB) Min() *node {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return nil
	}
	for nd.left != nil {
		nd = nd.left
	}
	nd.settimestamp(time.Now().UnixNano())
	return nd
}

func (llrb *LLRB) Max() *node {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return nil
	}
	for nd.right != nil {
		nd = nd.right
	}
	nd.settimestamp(time.Now().UnixNano())
	return nd
}

// Range from lowkey to highkey, incl can be "both", "low", "high", "none"
func (llrb *LLRB) Range(lowkey, highkey []byte, incl string, iter NdIterator) {
	if iter == nil {
		panic("Range(): iter argument is nil")
	}
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	switch incl {
	case "both":
		llrb.rangeFromFind(nd, lowkey, highkey, iter)
	case "high":
		llrb.rangeAfterFind(nd, lowkey, highkey, iter)
	case "low":
		llrb.rangeFromTill(nd, lowkey, highkey, iter)
	default:
		llrb.rangeAfterTill(nd, lowkey, highkey, iter)
	}
}

// low <= (keys) <= high
func (llrb *LLRB) rangeFromFind(nd *node, lk, hk []byte, iter NdIterator) bool {
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
	nd.settimestamp(time.Now().UnixNano())
	return llrb.rangeFromFind(nd.right, lk, hk, iter)
}

// low <= (keys) < hk
func (llrb *LLRB) rangeFromTill(nd *node, lk, hk []byte, iter NdIterator) bool {
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
	nd.settimestamp(time.Now().UnixNano())
	return llrb.rangeFromTill(nd.right, lk, hk, iter)
}

// low < (keys) <= hk
func (llrb *LLRB) rangeAfterFind(nd *node, lk, hk []byte, iter NdIterator) bool {
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
	nd.settimestamp(time.Now().UnixNano())
	return llrb.rangeAfterFind(nd.right, lk, hk, iter)
}

// low < (keys) < hk
func (llrb *LLRB) rangeAfterTill(nd *node, lk, hk []byte, iter NdIterator) bool {
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
	nd.settimestamp(time.Now().UnixNano())
	return llrb.rangeAfterTill(nd.right, lk, hk, iter)
}

//---- LLRB write operations.

func (llrb *LLRB) Upsert(k, v []byte, vbno uint16, vbuuid, seqno uint64) *node {
	if k == nil {
		panic("upserting nil key")
	}
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, oldn := llrb.upsert(nd, k, v, vbno, vbuuid, seqno)
	root.setblack()
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if oldn == nil {
		llrb.count++
	} else {
		atomic.AddInt64(&llrb.keymemory, -int64(len(oldn.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(oldn.nodevalue().value())))
	}
	atomic.AddInt64(&llrb.keymemory, int64(len(k)))
	atomic.AddInt64(&llrb.valmemory, int64(len(v)))
	return oldn
}

func (llrb *LLRB) upsert(
	nd *node, key, value []byte,
	vbno uint16, vbuuid, seqno uint64) (root, oldn *node) {

	if nd == nil {
		return llrb.newnode(key, value, vbno, vbuuid, seqno), nil
	}

	nd = llrb.walkdownrot23(nd)

	if nd.gtkey(key) {
		nd.left, oldn = llrb.upsert(nd.left, key, value, vbno, vbuuid, seqno)
	} else if nd.ltkey(key) {
		nd.right, oldn = llrb.upsert(nd.right, key, value, vbno, vbuuid, seqno)
	} else {
		oldn = llrb.clone(nd)
		if nv := nd.nodevalue(); nv != nil { // free the old value block
			nv.pool.free(unsafe.Pointer(nv))
		}
		nd.settimestamp(time.Now().UnixNano())
		if value != nil { // and new value block if need be
			ptr, mpool := llrb.valarena.alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			nd = nd.setnodevalue(nv.setvalue(value))
		}
	}

	nd = llrb.walkuprot23(nd)
	return nd, oldn
}

func (llrb *LLRB) DeleteMin() *node {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.deletemin(nd)
	if root != nil {
		root.setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted != nil {
		atomic.AddInt64(&llrb.keymemory, -int64(len(deleted.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(deleted.nodevalue().value())))
		llrb.count--
	}
	return deleted
}

// using 2-3 trees
func (llrb *LLRB) deletemin(nd *node) (newnd, deleted *node) {
	if nd == nil {
		return nil, nil
	}
	if nd.left == nil {
		return nil, nd
	}
	if !nd.left.isred() && !nd.left.left.isred() {
		nd = moveredleft(nd)
	}
	nd.left, deleted = llrb.deletemin(nd.left)
	return fixup(nd), deleted
}

func (llrb *LLRB) DeleteMax() *node {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.deletemax(nd)
	if root != nil {
		root.setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted != nil {
		atomic.AddInt64(&llrb.keymemory, -int64(len(deleted.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(deleted.nodevalue().value())))
		llrb.count--
	}
	return deleted
}

// using 2-3 trees
func (llrb *LLRB) deletemax(nd *node) (newnd, deleted *node) {
	if nd == nil {
		return nil, nil
	}
	if nd.left.isred() {
		nd = rotateright(nd)
	}
	if nd.right == nil {
		return nil, nd
	}
	if !nd.right.isred() && !nd.right.left.isred() {
		nd = moveredright(nd)
	}
	nd.right, deleted = llrb.deletemax(nd.right)
	return fixup(nd), deleted
}

func (llrb *LLRB) Delete(key []byte) *node {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.delete(nd, key)
	if root != nil {
		root.setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted != nil {
		atomic.AddInt64(&llrb.keymemory, -int64(len(deleted.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(deleted.nodevalue().value())))
		llrb.count--
	}
	return deleted
}

func (llrb *LLRB) delete(nd *node, key []byte) (newnd, deleted *node) {
	if nd == nil {
		return nil, nil
	}

	if nd.gtkey(key) {
		if nd.left == nil { // key not present. Nothing to delete
			return nd, nil
		}
		if !nd.left.isred() && !nd.left.left.isred() {
			nd = moveredleft(nd)
		}
		nd.left, deleted = llrb.delete(nd.left, key)

	} else {
		if nd.left.isred() {
			nd = rotateright(nd)
		}
		// If @key equals @h.Item and no right children at @h
		if !nd.ltkey(key) && nd.right == nil {
			return nil, nd
		}
		if nd.right != nil && !nd.right.isred() && !nd.right.left.isred() {
			nd = moveredright(nd)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !nd.ltkey(key) {
			var subdeleted *node
			nd.right, subdeleted = llrb.deletemin(nd.right)
			if subdeleted == nil {
				panic("logic")
			}
			deleted = nd
			// copy subdeleted as the current node
			sdkey := subdeleted.key()
			vbno, vbuuid := subdeleted.vbno(), subdeleted.vbuuid
			seqno := subdeleted.seqno
			if nv := subdeleted.nodevalue(); nv != nil {
				nd = llrb.newnode(sdkey, nv.value(), vbno, vbuuid, seqno)
			} else {
				nd = llrb.newnode(sdkey, nil, vbno, vbuuid, seqno)
			}
			if deleted.isdirty() {
				nd.setdirty()
			} else {
				nd.cleardirty()
			}
			if deleted.isblack() {
				nd.setblack()
			} else {
				nd.setred()
			}
			nd.left, nd.right = deleted.left, deleted.right
			// free the subdeleted node.
			llrb.Freenode(subdeleted)
		} else { // Else, @key is bigger than @nd
			nd.right, deleted = llrb.delete(nd.right, key)
		}
	}
	return fixup(nd), deleted
}

// rotation routines for 2-3 algorithm

func (llrb *LLRB) walkdownrot23(nd *node) *node {
	return nd
}

func (llrb *LLRB) walkuprot23(nd *node) *node {
	if nd.right.isred() && !nd.left.isred() {
		nd = rotateleft(nd)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd = rotateright(nd)
	}
	if nd.left.isred() && nd.right.isred() {
		flip(nd)
	}
	return nd
}

// rotation routines for 2-3-4 algorithm

func walkdownrot234(nd *node) *node {
	if nd.left.isred() && nd.right.isred() {
		flip(nd)
	}
	return nd
}

func walkuprot234(nd *node) *node {
	if nd.right.isred() && !nd.left.isred() {
		nd = rotateleft(nd)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd = rotateright(nd)
	}
	return nd
}

//---- local functions

func (llrb *LLRB) newnode(k, v []byte, vbno uint16, vbuuid, seqno uint64) *node {
	ptr, mpool := llrb.nodearena.alloc(int64(nodesize + len(k)))
	nd := (*node)(ptr)
	nd = nd.setdirty().setred()
	nd = nd.setvbno(vbno)
	nd.vbuuid, nd.seqno = vbuuid, seqno
	nd.pool, nd.left, nd.right = mpool, nil, nil

	ptr, mpool = llrb.valarena.alloc(int64(nvaluesize + len(v)))
	nv := (*nodevalue)(ptr)
	nv.pool = mpool
	nd.setnodevalue(nv.setvalue(v))

	nd.fpos = -1
	nd = nd.settimestamp(time.Now().UnixNano()).setkey(k)
	return nd
}

func (llrb *LLRB) clone(nd *node) (newnd *node) {
	key := nd.key()
	if nv := nd.nodevalue(); nv != nil {
		newnd = llrb.newnode(key, nv.value(), nd.vbno(), nd.vbuuid, nd.seqno)
	} else {
		newnd = llrb.newnode(key, nil, nd.vbno(), nd.vbuuid, nd.seqno)
	}
	if nd.isdirty() {
		newnd.setdirty()
	} else {
		newnd.cleardirty()
	}
	if nd.isblack() {
		newnd.setblack()
	} else {
		newnd.setred()
	}
	newnd.left, newnd.right = nd.left, nd.right
	return
}

func (llrb *LLRB) equivalent(n1, n2 *node) bool {
	return n1.isdirty() == n2.isdirty() &&
		n1.isblack() == n2.isblack() &&
		n1.vbno() == n2.vbno() &&
		n1.vbuuid == n2.vbuuid &&
		n1.seqno == n2.seqno &&
		n1.left == n2.left &&
		n1.right == n2.right &&
		bytes.Compare(n1.key(), n2.key()) == 0 &&
		bytes.Compare(n1.nodevalue().value(), n2.nodevalue().value()) == 0 &&
		n1.timestamp() == n2.timestamp()
}

func validateConfig(config map[string]interface{}) {
	minblock := config["nodemem.minblock"].(int)
	maxblock := config["nodemem.maxblock"].(int)
	capacity := config["nodemem.capacity"].(int)
	if minblock < MinKeymem {
		fmsg := "nodemem.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, MinKeymem))
	} else if maxblock > MaxKeymem {
		fmsg := "nodemem.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, MaxKeymem))
	} else if capacity == 0 {
		panic("nodemem.capacity cannot be ZERO")
	}

	minblock = config["valmem.minblock"].(int)
	maxblock = config["valmem.maxblock"].(int)
	capacity = config["valmem.capacity"].(int)
	if minblock < MinValmem {
		fmsg := "valmem.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, MinValmem))
	} else if maxblock > MaxValmem {
		fmsg := "valmem.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, MaxValmem))
	} else if capacity == 0 {
		panic("valmem.capacity cannot be ZERO")
	}
}
