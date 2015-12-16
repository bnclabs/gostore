// Package llrb
//
// configuration:
//
//	"nodearena.minblock"
//  "nodearena.maxblock"
//  "nodearena.capacity"
//  "nodepool.capacity"
//  "valarena.minblock"
//  "valarena.maxblock"
//  "valarena.capacity"
//  "valpool.capacity"
//  "metadata.bornseqno"
//  "metadata.deadseqno"
//  "metadata.mvalue"
//  "metadata.vbuuid"
//  "log.level"
//  "log.file"

package llrb

import "fmt"
import "unsafe"
import "sort"
import "bytes"
import "sync/atomic"

const MinKeymem = 96
const MaxKeymem = 4096
const MinValmem = 32
const MaxValmem = 10 * 1024 * 1024

// NdIterator callback function while ranging from
// low-key and high-key, return false to stop iteration.
type NdIterator func(nd *llrbnode) bool

type LLRB struct { // tree container
	name      string
	nodearena *memarena
	valarena  *memarena
	root      unsafe.Pointer // root *llrbnode of LLRB tree
	fmask     metadataMask   // only 12 bits
	config    map[string]interface{}
	logPrefix string
	// statistics
	count     int64 // number of nodes in the tree
	keymemory int64 // memory used by all keys
	valmemory int64 // memory used by all values
	upserthgt *averageInt
}

func NewLLRB(name string, config map[string]interface{}, logg Logger) *LLRB {
	validateConfig(config)
	llrb := &LLRB{}
	// setup nodearena for key and metadata
	minblock := int64(config["nodearena.minblock"].(int))
	maxblock := int64(config["nodearena.maxblock"].(int))
	capacity := int64(config["nodearena.capacity"].(int))
	pcapacity := int64(config["nodepool.capacity"].(int))
	llrb.nodearena = newmemarena(minblock, maxblock, capacity, pcapacity)
	// setup value arena
	minblock = int64(config["valarena.minblock"].(int))
	maxblock = int64(config["valarena.maxblock"].(int))
	capacity = int64(config["valarena.capacity"].(int))
	pcapacity = int64(config["valpool.capacity"].(int))
	llrb.valarena = newmemarena(minblock, maxblock, capacity, pcapacity)
	// set up logger
	setLogger(logg, config)
	llrb.logPrefix = fmt.Sprintf("[LLRB-%s]", name)
	llrb.fmask = metadataMask(0)
	if conf, ok := config["metadata.bornseqno"]; ok && conf.(bool) {
		llrb.fmask = llrb.fmask.enableBornSeqno()
	}
	if conf, ok := config["metadata.deadseqno"]; ok && conf.(bool) {
		llrb.fmask = llrb.fmask.enableDeadSeqno()
	}
	if conf, ok := config["metadata.mvalue"]; ok && conf.(bool) {
		llrb.fmask = llrb.fmask.enableMvalue()
	}
	if conf, ok := config["metadata.vbuuid"]; ok && conf.(bool) {
		llrb.fmask = llrb.fmask.enableVbuuid()
	}
	llrb.config = config
	// statistics
	llrb.upserthgt = &averageInt{}
	return llrb
}

//---- Maintanence APIs.

func (llrb *LLRB) SetRoot(r *llrbnode) {
	atomic.StorePointer(&llrb.root, unsafe.Pointer(r))
}

func (llrb *LLRB) Root() *llrbnode {
	return (*llrbnode)(atomic.LoadPointer(&llrb.root))
}

func (llrb *LLRB) Release() {
	llrb.nodearena.release()
	llrb.valarena.release()
}

func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.count)
}

func (llrb *LLRB) NodeArena() (overhead, useful int64) { // needs an Rlock
	return llrb.nodearena.memory()
}

func (llrb *LLRB) ValueArena() (overhead, useful int64) { // needs an Rlock
	return llrb.valarena.memory()
}

func (llrb *LLRB) NodeAllocated() int64 { // needs an Rlock
	return llrb.nodearena.allocated()
}

func (llrb *LLRB) ValueAllocated() int64 { // needs an Rlock
	return llrb.valarena.allocated()
}

func (llrb *LLRB) NodeAvailable() int64 { // needs an Rlock
	return llrb.nodearena.available()
}

func (llrb *LLRB) ValueAvailable() int64 { // needs an Rlock
	return llrb.valarena.available()
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

func (llrb *LLRB) Freenode(nd *llrbnode) { // TODO: should this be exported ?
	if nd != nil {
		nv := nd.nodevalue()
		if nv != nil {
			nv.pool.free(unsafe.Pointer(nv))
		}
		nd.pool.free(unsafe.Pointer(nd))
	}
}

func (llrb *LLRB) LogNodeutilz() {
	log.Infof("%v Node utilization:\n", llrb.logPrefix)
	arenapools := llrb.nodearena.mpools
	sizes := []int{}
	for size := range arenapools {
		sizes = append(sizes, int(size))
	}
	sort.Ints(sizes)
	for _, size := range sizes {
		mpools := arenapools[int64(size)]
		allocated, capacity := int64(0), int64(0)
		if len(mpools) > 0 {
			for _, mpool := range mpools {
				allocated += mpool.allocated()
				capacity += mpool.capacity
			}
			z := (float64(allocated) / float64(capacity)) * 100
			log.Infof("%v  %v %v %2.2f\n", llrb.logPrefix, size, capacity, z)
		}
	}
}

func (llrb *LLRB) LogValueutilz() {
	log.Infof("%v Value utilization:\n", llrb.logPrefix)
	arenapools := llrb.valarena.mpools
	sizes := []int{}
	for size := range arenapools {
		sizes = append(sizes, int(size))
	}
	sort.Ints(sizes)
	for _, size := range sizes {
		mpools := arenapools[int64(size)]
		allocated, capacity := int64(0), int64(0)
		if len(mpools) > 0 {
			for _, mpool := range mpools {
				allocated += mpool.allocated()
				capacity += mpool.capacity
			}
			z := (float64(allocated) / float64(capacity)) * 100
			log.Infof("%v  %v %v %2.2f\n", llrb.logPrefix, size, capacity, z)
		}
	}
}

func (llrb *LLRB) HeightStats() map[string]interface{} {
	av := &averageInt{}
	root := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	heightStats(root, 0, av)
	return map[string]interface{}{
		"samples":     av.samples(),
		"min":         av.min(),
		"max":         av.max(),
		"mean":        av.mean(),
		"variance":    av.variance(),
		"stddeviance": av.sd(),
	}
}

func heightStats(nd *llrbnode, d int64, av *averageInt) {
	if nd == nil {
		return
	}
	av.add(d)
	if nd.left != nil {
		heightStats(nd.left, d+1, av)
	}
	if nd.right != nil {
		heightStats(nd.right, d+1, av)
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

func (llrb *LLRB) Get(lookupkey []byte) (nd *llrbnode) {
	nd = (*llrbnode)(atomic.LoadPointer(&llrb.root))
	for nd != nil {
		if nd.gtkey(lookupkey) {
			nd = nd.left
		} else if nd.ltkey(lookupkey) {
			nd = nd.right
		} else {
			return nd
		}
	}
	return nil // key is not present in the tree
}

func (llrb *LLRB) Min() *llrbnode {
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return nil
	}
	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

func (llrb *LLRB) Max() *llrbnode {
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return nil
	}
	for nd.right != nil {
		nd = nd.right
	}
	return nd
}

// Range from lowkey to highkey, incl can be "both", "low", "high", "none"
func (llrb *LLRB) Range(lowkey, highkey []byte, incl string, iter NdIterator) {
	if iter == nil {
		panic("Range(): iter argument is nil")
	}
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
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
func (llrb *LLRB) rangeFromFind(
	nd *llrbnode, lk, hk []byte, iter NdIterator) bool {

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
	nd *llrbnode, lk, hk []byte, iter NdIterator) bool {

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
	nd *llrbnode, lk, hk []byte, iter NdIterator) bool {

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
	nd *llrbnode, lk, hk []byte, iter NdIterator) bool {

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

//---- LLRB write operations.

// caller should free old-llrbnode if it is not null.
func (llrb *LLRB) Upsert(k, v []byte) (newnd, oldnd *llrbnode) {
	var root *llrbnode

	if k == nil {
		panic("upserting nil key")
	}
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	root, newnd, oldnd = llrb.upsert(nd, 1 /*depth*/, k, v)
	root.metadata().setblack()
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if oldnd == nil {
		llrb.count++
	} else {
		atomic.AddInt64(&llrb.keymemory, -int64(len(oldnd.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(oldnd.nodevalue().value())))
	}
	atomic.AddInt64(&llrb.keymemory, int64(len(k)))
	atomic.AddInt64(&llrb.valmemory, int64(len(v)))
	return newnd, oldnd
}

// returns root, newnd, oldnd
func (llrb *LLRB) upsert(
	nd *llrbnode, depth int64,
	key, value []byte) (*llrbnode, *llrbnode, *llrbnode) {

	var oldnd, newnd *llrbnode

	if nd == nil {
		newnd := llrb.newnode(key, value)
		llrb.upserthgt.add(depth)
		return newnd, newnd, nil
	}

	nd = llrb.walkdownrot23(nd)

	if nd.gtkey(key) {
		nd.left, newnd, oldnd = llrb.upsert(nd.left, depth+1, key, value)
	} else if nd.ltkey(key) {
		nd.right, newnd, oldnd = llrb.upsert(nd.right, depth+1, key, value)
	} else {
		oldnd = llrb.clone(nd)
		if nv := nd.nodevalue(); nv != nil { // free the value if present
			nv.pool.free(unsafe.Pointer(nv))
		}
		if value != nil { // and new value if need be
			ptr, mpool := llrb.valarena.alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			nd = nd.setnodevalue(nv.setvalue(value))
		}
		newnd = nd
		llrb.upserthgt.add(depth)
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd
}

func (llrb *LLRB) DeleteMin() *llrbnode {
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.deletemin(nd)
	if root != nil {
		root.metadata().setblack()
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
func (llrb *LLRB) deletemin(nd *llrbnode) (newnd, deleted *llrbnode) {
	if nd == nil {
		return nil, nil
	}
	if nd.left == nil {
		return nil, nd
	}
	if !isred(nd.left) && !isred(nd.left.left) {
		nd = moveredleft(nd)
	}
	nd.left, deleted = llrb.deletemin(nd.left)
	return fixup(nd), deleted
}

func (llrb *LLRB) DeleteMax() *llrbnode {
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.deletemax(nd)
	if root != nil {
		root.metadata().setblack()
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
func (llrb *LLRB) deletemax(nd *llrbnode) (newnd, deleted *llrbnode) {
	if nd == nil {
		return nil, nil
	}
	if isred(nd.left) {
		nd = rotateright(nd)
	}
	if nd.right == nil {
		return nil, nd
	}
	if !isred(nd.right) && !isred(nd.right.left) {
		nd = moveredright(nd)
	}
	nd.right, deleted = llrb.deletemax(nd.right)
	return fixup(nd), deleted
}

func (llrb *LLRB) Delete(key []byte) *llrbnode {
	nd := (*llrbnode)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.delete(nd, key)
	if root != nil {
		root.metadata().setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
	if deleted != nil {
		atomic.AddInt64(&llrb.keymemory, -int64(len(deleted.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(deleted.nodevalue().value())))
		llrb.count--
	}
	return deleted
}

func (llrb *LLRB) delete(nd *llrbnode, key []byte) (newnd, deleted *llrbnode) {
	if nd == nil {
		return nil, nil
	}

	if nd.gtkey(key) {
		if nd.left == nil { // key not present. Nothing to delete
			return nd, nil
		}
		if !isred(nd.left) && !isred(nd.left.left) {
			nd = moveredleft(nd)
		}
		nd.left, deleted = llrb.delete(nd.left, key)

	} else {
		if isred(nd.left) {
			nd = rotateright(nd)
		}
		// If @key equals @h.Item and no right children at @h
		if !nd.ltkey(key) && nd.right == nil {
			return nil, nd
		}
		if nd.right != nil && !isred(nd.right) && !isred(nd.right.left) {
			nd = moveredright(nd)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !nd.ltkey(key) {
			var subdeleted *llrbnode
			nd.right, subdeleted = llrb.deletemin(nd.right)
			if subdeleted == nil {
				panic("logic")
			}
			newnd := llrb.clone(subdeleted)
			newnd.left, newnd.right = nd.left, nd.right
			if nd.metadata().isdirty() {
				newnd.metadata().setdirty()
			}
			if nd.metadata().isblack() {
				newnd.metadata().setblack()
			} else {
				newnd.metadata().setred()
			}
			newnd.nodevalue().setvalue(subdeleted.nodevalue().value())
			deleted, nd = nd, newnd
			llrb.Freenode(subdeleted)
		} else { // Else, @key is bigger than @nd
			nd.right, deleted = llrb.delete(nd.right, key)
		}
	}
	return fixup(nd), deleted
}

// rotation routines for 2-3 algorithm

func (llrb *LLRB) walkdownrot23(nd *llrbnode) *llrbnode {
	return nd
}

func (llrb *LLRB) walkuprot23(nd *llrbnode) *llrbnode {
	if isred(nd.right) && !isred(nd.left) {
		nd = rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = rotateright(nd)
	}
	if isred(nd.left) && isred(nd.right) {
		flip(nd)
	}
	return nd
}

// rotation routines for 2-3-4 algorithm

func walkdownrot234(nd *llrbnode) *llrbnode {
	if isred(nd.left) && isred(nd.right) {
		flip(nd)
	}
	return nd
}

func walkuprot234(nd *llrbnode) *llrbnode {
	if isred(nd.right) && !isred(nd.left) {
		nd = rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = rotateright(nd)
	}
	return nd
}

//---- local functions

func (llrb *LLRB) newnode(k, v []byte) *llrbnode {
	mdsize := (&metadata{}).initMetadata(0, llrb.fmask).sizeof()
	ptr, mpool := llrb.nodearena.alloc(int64(llrbnodesize + mdsize + len(k)))
	nd := (*llrbnode)(ptr)
	nd.metadata().initMetadata(0, llrb.fmask).setdirty().setred()
	nd.setkey(k)
	nd.pool, nd.left, nd.right = mpool, nil, nil

	if v != nil && nd.metadata().ismvalue() {
		ptr, mpool = llrb.valarena.alloc(int64(nvaluesize + len(v)))
		nv := (*nodevalue)(ptr)
		nv.pool = mpool
		nvarg := (uintptr)(unsafe.Pointer(nv.setvalue(v)))
		nd.metadata().setmvalue((uint64)(nvarg), 0)
	} else if v != nil {
		panic("llrb tree not configured for accepting value")
	}
	return nd
}

func (llrb *LLRB) clone(nd *llrbnode) (newnd *llrbnode) {
	// clone llrbnode.
	newndu, mpool := llrb.nodearena.alloc(nd.pool.size)
	newnd = (*llrbnode)(newndu)
	memcpy(unsafe.Pointer(newnd), unsafe.Pointer(nd), int(nd.pool.size))
	newnd.pool = mpool
	// clone value if value is present.
	if nd.metadata().ismvalue() {
		if mvalue, level := nd.metadata().mvalue(); level == 0 && mvalue != 0 {
			nv := (*nodevalue)(unsafe.Pointer((uintptr)(mvalue)))
			newnvu, mpool := llrb.valarena.alloc(nv.pool.size)
			memcpy(newnvu, unsafe.Pointer(nv), int(nv.pool.size))
			newnv := (*nodevalue)(newnvu)
			newnv.pool = mpool
			newnd.setnodevalue(newnv)
		}
	}
	return
}

func (llrb *LLRB) equivalent(n1, n2 *llrbnode) bool {
	md1, md2 := n1.metadata(), n2.metadata()
	if md1.isdirty() != md2.isdirty() {
		//fmt.Println("dirty mismatch")
		return false
	} else if md1.isblack() != md2.isblack() {
		//fmt.Println("black mismatch")
		return false
	} else if md1.vbno() != md2.vbno() {
		//fmt.Println("vbno mismatch")
		return false
	} else if md1.isvbuuid() && (md1.vbuuid() != md2.vbuuid()) {
		//fmt.Println("vbuuid mismatch")
		return false
	} else if md1.isbnseq() && (md1.bnseq() != md2.bnseq()) {
		//fmt.Println("isbnseq mismatch")
		return false
	} else if md1.access() != md2.access() {
		//fmt.Println("access mismatch", md1.access())
		return false
	} else if n1.left != n2.left || n1.right != n2.right {
		//fmt.Println("left mismatch")
		return false
	} else if bytes.Compare(n1.key(), n2.key()) != 0 {
		//fmt.Println("key mismatch")
		return false
	} else if md1.ismvalue() {
		if bytes.Compare(n1.nodevalue().value(), n2.nodevalue().value()) != 0 {
			//fmt.Println("dirty mismatch")
			return false
		}
	}
	return true
}

func validateConfig(config map[string]interface{}) {
	minblock := config["nodearena.minblock"].(int)
	maxblock := config["nodearena.maxblock"].(int)
	capacity := config["nodearena.capacity"].(int)
	if minblock < MinKeymem {
		fmsg := "nodearena.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, MinKeymem))
	} else if maxblock > MaxKeymem {
		fmsg := "nodearena.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, MaxKeymem))
	} else if capacity == 0 {
		panic("nodearena.capacity cannot be ZERO")
	}

	minblock = config["valarena.minblock"].(int)
	maxblock = config["valarena.maxblock"].(int)
	capacity = config["valarena.capacity"].(int)
	if minblock < MinValmem {
		fmsg := "valarena.minblock < %v configuration"
		panic(fmt.Errorf(fmsg, MinValmem))
	} else if maxblock > MaxValmem {
		fmsg := "valarena.maxblock > %v configuration"
		panic(fmt.Errorf(fmsg, MaxValmem))
	} else if capacity == 0 {
		panic("valarena.capacity cannot be ZERO")
	}
}
