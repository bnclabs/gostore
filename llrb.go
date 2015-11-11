package llrb

import "fmt"
import "unsafe"
import "time"
import "sync/atomic"

const minKeymem = 64
const maxKeymem = 4096

// KeyIterator callback function while ranging from low-key and high-key.
type KeyIterator func(key, value []byte) bool

type LLRB struct { // tree container
	nodearena *memarena
	valarena  *memarena
	root      unsafe.Pointer // root *node of LLRB tree
	// scratch pad
	tmpk []byte
	tmpv []byte
	// statistics
	count     int64 // number of nodes in the tree
	keymemory int64 // memory used by all keys
	valmemory int64 // memory used by all values
}

func NewLLRB(config map[string]interface{}) *LLRB {
	validateConfig(config)
	llrb := &LLRB{}
	minblock := config["nodemem.minblock"].(int)
	maxblock := config["nodemem.maxblock"].(int)
	capacity := config["nodemem.capacity "].(int)
	llrb.nodearena = newmemarena(minblock, maxblock, capacity)
	minblock = config["valmem.minblock"].(int)
	maxblock = config["valmem.maxblock"].(int)
	capacity = config["valmem.capacity"].(int)
	llrb.valarena = newmemarena(minblock, maxblock, capacity)
	// scratchpad
	llrb.tmpk = make([]byte, config["keymem.maxblock"].(int))
	llrb.tmpv = make([]byte, config["valmem.maxblock"].(int))
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

func (llrb *LLRB) KeyMemory() int64 {
	return atomic.LoadInt64(&llrb.keymemory)
}

func (llrb *LLRB) ValueMemory() int64 {
	return atomic.LoadInt64(&llrb.valmemory)
}

//---- LLRB read operations.

func (llrb *LLRB) Has(key []byte) bool {
	_, _, rv := llrb.Get(key, nil, nil)
	return rv
}

func (llrb *LLRB) Get(lookupkey, key, value []byte) (n int, m int, ok bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	for nd != nil {
		if !nd.lekey(lookupkey) {
			nd = nd.left
		} else if nd.ltkey(lookupkey) {
			nd = nd.right
		} else {
			ok = true
			if key != nil {
				n = copy(key, nd.key())
			}
			if value != nil {
				m = copy(value, nd.nodevalue().value())
			}
			return
		}
	}
	return 0, 0, false // key is not present in the tree
}

func (llrb *LLRB) Min(key, value []byte) (int, int, bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return 0, 0, false
	}
	for nd.left != nil {
		nd = nd.left
	}
	return copy(key, nd.key()), copy(value, nd.nodevalue().value()), true
}

func (llrb *LLRB) Max(key, value []byte) (int, int, bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return 0, 0, false
	}
	for nd.right != nil {
		nd = nd.right
	}
	return copy(key, nd.key()), copy(value, nd.nodevalue().value()), true
}

// Range from lowkey to highkey, incl can be "both", "low", "high", "none"
func (llrb *LLRB) Range(lowkey, highkey []byte, incl string, iter KeyIterator) {
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
func (llrb *LLRB) rangeFromFind(nd *node, lk, hk []byte, iter KeyIterator) bool {
	if nd == nil {
		return true
	}
	if hk != nil && !nd.lekey(hk) {
		return llrb.rangeFromFind(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.ltkey(lk) {
		return llrb.rangeFromFind(nd.right, lk, hk, iter)
	}
	if !llrb.rangeFromFind(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd.key(), nd.nodevalue().value()) {
		return false
	}
	return llrb.rangeFromFind(nd.right, lk, hk, iter)
}

// low <= (keys) < hk
func (llrb *LLRB) rangeFromTill(nd *node, lk, hk []byte, iter KeyIterator) bool {
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
	if iter != nil && !iter(nd.key(), nd.nodevalue().value()) {
		return false
	}
	return llrb.rangeFromTill(nd.right, lk, hk, iter)
}

// low < (keys) <= hk
func (llrb *LLRB) rangeAfterFind(nd *node, lk, hk []byte, iter KeyIterator) bool {
	if nd == nil {
		return true
	}
	if hk != nil && !nd.lekey(hk) {
		return llrb.rangeAfterFind(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.lekey(lk) {
		return llrb.rangeAfterFind(nd.right, lk, hk, iter)
	}
	if !llrb.rangeAfterFind(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd.key(), nd.nodevalue().value()) {
		return false
	}
	return llrb.rangeAfterFind(nd.right, lk, hk, iter)
}

// low < (keys) < hk
func (llrb *LLRB) rangeAfterTill(nd *node, lk, hk []byte, iter KeyIterator) bool {
	if nd == nil {
		return true
	}
	if hk != nil && !nd.ltkey(hk) {
		return llrb.rangeAfterTill(nd.left, lk, hk, iter)
	}
	if lk != nil && nd.lekey(lk) {
		return llrb.rangeAfterTill(nd.right, lk, hk, iter)
	}
	if !llrb.rangeAfterTill(nd.left, lk, hk, iter) {
		return false
	}
	if iter != nil && !iter(nd.key(), nd.nodevalue().value()) {
		return false
	}
	return llrb.rangeAfterTill(nd.right, lk, hk, iter)
}

//---- LLRB write operations.

func (llrb *LLRB) Upsert(key, value, oldkey, oldvalue []byte) (int, int, bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, n, m, replaced := llrb.upsert(nd, key, value, oldkey, oldvalue)
	root.setblack()
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if replaced == false {
		atomic.AddInt64(&llrb.count, 1)
	}
	return n, m, replaced
}

func (llrb *LLRB) upsert(
	nd *node, key, value, oldk, oldv []byte) (*node, int, int, bool) {

	if nd == nil {
		return llrb.newnode(key, value), 0, 0, false
	}

	nd = llrb.walkdownrot23(nd)

	var n, m int
	var replaced bool

	if nd.gekey(key) == false {
		nd.left, n, m, replaced = llrb.upsert(nd.left, key, value, oldk, oldv)
	} else if nd.ltkey(key) {
		nd.right, n, m, replaced = llrb.upsert(nd.right, key, value, oldk, oldv)
	} else {
		k, v := nd.key(), nd.nodevalue().value()
		n, m, replaced = copy(oldk, k), copy(oldv, v), true
	}

	nd = llrb.walkuprot23(nd)

	return nd, n, m, replaced
}

func (llrb *LLRB) DeleteMin(oldkey, oldvalue []byte) (int, int, bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, n, m, deleted := llrb.deletemin(nd, oldkey, oldvalue)
	if root != nil {
		root.setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted {
		atomic.AddInt64(&llrb.count, -1)
	}
	return n, m, deleted
}

func (llrb *LLRB) deletemin( // using 2-3 trees
	nd *node, oldk, oldv []byte) (newnd *node, n int, m int, deleted bool) {

	if nd == nil {
		return nil, 0, 0, false
	}
	if nd.left == nil {
		k, v := nd.key(), nd.nodevalue().value()
		if oldk != nil {
			n = copy(oldk, k)
		}
		m = copy(oldv, v)
		return nil, n, m, true
	}

	if !nd.left.isred() && !nd.left.left.isred() {
		nd = moveredleft(nd)
	}

	nd.left, n, m, deleted = llrb.deletemin(nd.left, oldk, oldv)
	return fixup(nd), n, m, deleted
}

func (llrb *LLRB) DeleteMax(oldkey, oldvalue []byte) (int, int, bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, n, m, deleted := llrb.deletemax(nd, oldkey, oldvalue)
	if root != nil {
		root.setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted {
		atomic.AddInt64(&llrb.count, -1)
	}
	return n, m, deleted
}

func (llrb *LLRB) deletemax( // using 2-3 trees
	nd *node, oldk, oldv []byte) (newnd *node, n int, m int, deleted bool) {

	if nd == nil {
		return nil, 0, 0, false
	}
	if nd.left.isred() {
		nd = rotateright(nd)
	}
	if nd.right == nil {
		k, v := nd.key(), nd.nodevalue().value()
		n, m := copy(oldk, k), copy(oldv, v)
		return nil, n, m, true
	}
	if !nd.right.isred() && !nd.right.left.isred() {
		nd = moveredright(nd)
	}

	nd.left, n, m, deleted = llrb.deletemax(nd.right, oldk, oldv)
	return fixup(nd), n, m, deleted
}

func (llrb *LLRB) Delete(key, oldvalue []byte) (int, bool) {
	nd := (*node)(atomic.LoadPointer(&llrb.root))
	root, _, m, deleted := llrb.delete(nd, key, oldvalue)
	if root != nil {
		root.setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted {
		atomic.AddInt64(&llrb.count, -1)
	}
	return m, deleted
}

func (llrb *LLRB) delete(
	nd *node, key, oldv []byte) (newnd *node, n, m int, deleted bool) {

	if nd == nil {
		return nil, 0, 0, false
	}

	if nd.gekey(key) == false {
		if nd.left == nil { // key not present. Nothing to delete
			return nd, 0, 0, false
		}
		if !nd.left.isred() && !nd.left.left.isred() {
			nd = moveredleft(nd)
		}
		nd.left, _, m, deleted = llrb.delete(nd.left, key, oldv)

	} else {
		if nd.left.isred() {
			nd = rotateright(nd)
		}
		if !nd.ltkey(key) && nd.right == nil {
			return nil, 0, copy(oldv, nd.nodevalue().value()), true
		}
		if nd.right != nil && !nd.right.isred() && !nd.right.left.isred() {
			nd = moveredright(nd)
		}
		if !nd.ltkey(key) {
			nd.right, n, m, deleted = llrb.deletemin(nd.right, llrb.tmpk, oldv)
			if deleted == false {
				panic("logic")
			}
			copy(llrb.tmpv, nd.nodevalue().value())
			nd.setkey(llrb.tmpk[:n])
			nd.nodevalue().setvalue(oldv[:m])
			copy(oldv, llrb.tmpv[:m])

		} else { // Else, @key is bigger than @nd
			nd.right, n, m, deleted = llrb.delete(nd.right, key, oldv)
		}
	}

	return fixup(nd), n, m, deleted
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

func (llrb *LLRB) newnode(key, value []byte) *node {
	ptr, mpool := llrb.nodearena.alloc(nodesize + len(key))
	nd := (*node)(ptr)
	nd.pool, nd.left, nd.right = mpool, nil, nil
	nd = nd.setblocksize(mpool.size).setdirty().setred()

	ptr, mpool = llrb.valarena.alloc(nvaluesize + len(value))
	nv := (*nodevalue)(ptr)
	nv.pool = mpool
	nv = nv.setblocksize(mpool.size).setvalue(value)

	nd.mvalue, nd.fpos = nv, -1
	nd = nd.settimestamp(time.Now().UnixNano()).setkey(key)
	return nd
}

func validateConfig(config map[string]interface{}) {
	minblock := config["nodemem.minblock"].(int)
	maxblock := config["nodemem.maxblock"].(int)
	capacity := config["nodemem.capacity"].(int)
	if minblock < minKeymem {
		fmsg := "nodemem.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, minKeymem))
	} else if maxblock > maxKeymem {
		fmsg := "nodemem.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, maxKeymem))
	} else if capacity == 0 {
		panic("nodemem.capacity cannot be ZERO")
	}

	minblock = config["valmem.minblock"].(int)
	maxblock = config["valmem.maxblock"].(int)
	capacity = config["valmem.capacity"].(int)
	if minblock < minKeymem {
		fmsg := "valmem.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, minKeymem))
	} else if maxblock > maxKeymem {
		fmsg := "valmem.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, maxKeymem))
	} else if capacity == 0 {
		panic("valmem.capacity cannot be ZERO")
	}
}
