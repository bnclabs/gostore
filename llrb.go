// LLRB algorithm
//
// * index key, value (value is optional).
// * custom memory management, application to use copy on GC to fix
//   memory fragmentation.
// * configurable metadata like
//   vbno, bornseqno, deadseqno, vbuuid
// * supports multi-version-concurrency-control, where writes are
//   serialized and there can be zero or more concurrent
//	 read-snapshots.
//  * iterator callbacks are used in Range() APIs
//
// non-mvcc use case:
//
// * LLRB tree cannot be shared between two routines. It is upto the
//   application to make sure that all methods on LLRB is serialized.
//
// * non-mvcc maintanence APIs:
//
//		llrb.LogNodeutilz(), llrb.LogNodememory(), llrb.LogValueutilz()
//		llrb.LogValuememory() llrb.LogUpsertdepth(), llrb.LogTreeheight(),
//		llrb.PPrint() llrb.Dotdump()
//
//		llrb.StatsMem(), llrb.StatsUpsert(), llrb.StatsHeight()
//		llrb.ValidateReds(), llrb.ValidateBlacks()
//
// * non-mvcc Tree/Memory APIs:
//
//		llrb.Count(), llrb.Destroy()
//      llrb.Has(), llrb.Get(), llrb.Min(), llrb.Max(), llrb.Range()
//		llrb.Upsert(), llrb.DeleteMin(), llrb.DeleteMax(), llrb.Delete()
//
// mvcc use case:
//
// * all write operations shall be serialized in a single routine.
// * one or more routines can do concurrent read operation.
//
//		llrb.Count(), llrb.Destroy()
//
// * mvcc APIs with concurrency support:
//
//		snapshot.Has(), snapshot.Get(), snapshot.Min(), snapshot.Max(),
//		snapshot.Range()
//		snapshot.ValidateReds(), snapshot.ValidateBlacks()
//
//      snapshot.Dotdump()
//
// * mvcc APIs that needs to be serialized, on write-snapshot:
//
//		writer.Upsert(), writer.DeleteMin(), writer.DeleteMax(), writer.Delete()
//		writer.MakeSnapshot(), writer.GetSnapshot()
//
//		writer.StatsMem(), writer.StatsUpsert(), writer.StatsHeight()
//
//		writer.LogNodeutilz(), writer.LogNodememory(), writer.LogValueutilz(),
//		writer.LogValuememory() writer.LogUpsertdepth(), writer.LogTreeheight()
//
//      writer.Dotdump()
//
// * Upsert() and Delete() APIs accept callbacks for applications to set
//   node's metadata fields like vbno, vbuuid, seqno etc...
// * In cases of {vbno,seqno} based mutations, application can use the
//   callback to directly set the clock informations.
//
// configuration:
//
// "maxvb" - integer
//		maximum number of vbuckets that will use this llrb tree.
//
// "log.level" - string
//		one of the following
//		"ignore", "fatal", "error", "warn", "info", "verbose", "debug", "trace"
//
// "log.file" - string
//		log to file, if empty log to console
//
// "nodearena.minblock" - integer
//		minimum node-block size that shall be requested from the arena.
//
// "nodearena.maxblock" - integer
//		maximum node-block size that shall be requested from the arena.
//
// "nodearena.capacity" - integer
//		capacity in bytes that the arena shall manage for node-blocks
//		using one or more pools.
//
// "nodearena.pool.capacity" - integer
//		limit the size of a pool, irrespective of pool's block size.
//
// "valarena.minblock" - integer
//		minimum value-block size that shall be requested from the arena.
//
// "valarena.maxblock" - integer
//		maximum value-block size that shall be requested from the arena.
//
// "valarena.capacity" - integer
//		capacity in bytes that the arena shall manage for node-blocks
//		using one or more pools.
//
// "valarena.pool.capacity" - integer
//		limit the size of a pool, irrespective of pool's block size.
//
// "metadata.bornseqno" - boolean
//		use metadata field to book-keep node's born sequence number.
//
// "metadata.deadseqno" - boolean
//		use metadata field to book-keep node's dead sequence number.
//
// "metadata.mvalue" - boolean
//		value is not nil and its blocks allocated from value-arena
//
// "metadata.vbuuid" - boolean
//		use metadata field to book-keep node's vbuuid.
//
// "mvcc.enabled" - boolean
//		consume LLRB as Multi-Version-Concurrency-Control-led tree.
//
// "mvcc.snapshot.tick" - int
//		interval in milli-second for generating read-snapshots
//
// "mvcc.writer.chanbuffer" - int
//		buffer size for mvcc writer's i/p channel
package storage

import "fmt"
import "unsafe"
import "sort"
import "io"
import "strings"
import "bytes"
import "sync/atomic"

import humanize "github.com/dustin/go-humanize"

// MinKeymem minimum key size that is valid across the system.
const MinKeymem = 96

// MaxKeymem maximum key size that is valid across the system.
const MaxKeymem = 4096

// MinValmem minimum value size that is valid across the system.
const MinValmem = 32

// MaxValmem maximum value size that is valid across the system.
const MaxValmem = 10 * 1024 * 1024

// LLRBNodeIterator callback from Range API.
type LLRBNodeIterator func(nd *Llrbnode) bool

// LLRBUpsertCallback callback from Upsert API. Don't keep any reference
// to newnd and oldnd:
// * oldnd can only be read.
// * newnd can be read or updated.
type LLRBUpsertCallback func(llrb *LLRB, newnd, oldnd *Llrbnode)

// LLRBDeleteCallback callback from Delete API. Don't keep any reference
// to nd:
// * deleted node can only be read.
type LLRBDeleteCallback func(llrb *LLRB, deleted *Llrbnode)

type LLRB struct { // tree container
	name      string
	nodearena *memarena
	valarena  *memarena
	root      unsafe.Pointer // root *Llrbnode of LLRB tree
	dead      bool
	clock     *vectorclock // current clock

	// config
	fmask     metadataMask // only 12 bits
	maxvb     int
	config    map[string]interface{}
	logPrefix string

	// statistics
	count       int64 // number of nodes in the tree
	keymemory   int64 // memory used by all keys
	valmemory   int64 // memory used by all values
	upsertdepth *averageInt

	// scratch pad
	strsl []string

	// mvcc
	mvcc struct {
		enabled  bool
		reclaim  []*Llrbnode
		writer   *LLRBWriter
		snapshot unsafe.Pointer // *LLRBSnapshot

		// stats
		reclaimstats map[string]*averageInt
	}
}

func NewLLRB(name string, config map[string]interface{}, logg Logger) *LLRB {
	llrb := &LLRB{name: name}

	llrb.validateConfig(config)

	llrb.maxvb = config["maxvb"].(int)
	llrb.clock = newvectorclock(llrb.maxvb)

	// setup arena for nodes and node-values.
	llrb.nodearena = llrb.newnodearena(config)
	llrb.valarena = llrb.newvaluearena(config)

	// set up logger
	setLogger(logg, config)
	llrb.logPrefix = fmt.Sprintf("[LLRB-%s]", name)

	// set up metadata options
	llrb.fmask = llrb.setupfmask(config)
	llrb.config = config

	// statistics
	llrb.upsertdepth = &averageInt{}

	// scratch pads
	llrb.strsl = make([]string, 0)

	// mvcc
	llrb.mvcc.enabled = config["mvcc.enabled"].(bool)
	if llrb.mvcc.enabled {
		llrb.mvcc.reclaim = make([]*Llrbnode, 0, 64)
		llrb.mvcc.reclaimstats = map[string]*averageInt{
			"upsert": &averageInt{},
			"delmin": &averageInt{},
			"delmax": &averageInt{},
			"delete": &averageInt{},
		}
	}

	log.Infof("%v configuration %v\n", llrb.logPrefix, config)
	log.Infof("%v started ...\n", llrb.logPrefix)
	return llrb
}

//---- LLRB read operations.

func (llrb *LLRB) Has(key []byte) bool {
	return llrb.Get(key) != nil
}

func (llrb *LLRB) Get(key []byte) (nd *Llrbnode) {
	nd = (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	for nd != nil {
		if nd.gtkey(key) {
			nd = nd.left
		} else if nd.ltkey(key) {
			nd = nd.right
		} else {
			return nd
		}
	}
	return nil // key is not present in the tree
}

func (llrb *LLRB) Min() (nd *Llrbnode) {
	if nd = (*Llrbnode)(atomic.LoadPointer(&llrb.root)); nd == nil {
		return nil
	}

	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

func (llrb *LLRB) Max() (nd *Llrbnode) {
	if nd = (*Llrbnode)(atomic.LoadPointer(&llrb.root)); nd == nil {
		return nil
	}

	for nd.right != nil {
		nd = nd.right
	}
	return nd
}

// Range from lkey to hkey, incl can be "both", "low", "high", "none"
func (llrb *LLRB) Range(lkey, hkey []byte, incl string, iter LLRBNodeIterator) {
	if iter == nil {
		panic("Range(): iter argument is nil")
	}

	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	switch incl {
	case "both":
		llrb.rangeFromFind(nd, lkey, hkey, iter)
	case "high":
		llrb.rangeAfterFind(nd, lkey, hkey, iter)
	case "low":
		llrb.rangeFromTill(nd, lkey, hkey, iter)
	default:
		llrb.rangeAfterTill(nd, lkey, hkey, iter)
	}
}

// low <= (keys) <= high
func (llrb *LLRB) rangeFromFind(
	nd *Llrbnode, lk, hk []byte, iter LLRBNodeIterator) bool {

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
	nd *Llrbnode, lk, hk []byte, iter LLRBNodeIterator) bool {

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
	nd *Llrbnode, lk, hk []byte, iter LLRBNodeIterator) bool {

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
	nd *Llrbnode, lk, hk []byte, iter LLRBNodeIterator) bool {

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

func (llrb *LLRB) Upsert(key, value []byte, callb LLRBUpsertCallback) {
	var root, newnd, oldnd *Llrbnode

	if key == nil {
		panic("upserting nil key")
	}
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	root, newnd, oldnd = llrb.upsert(nd, 1 /*depth*/, key, value)
	root.metadata().setblack()
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

	llrb.upsertcounts(key, value, oldnd)

	if callb != nil {
		callb(llrb, newnd, oldnd)
	}
	llrb.freenode(oldnd)
}

// returns root, newnd, oldnd
func (llrb *LLRB) upsert(
	nd *Llrbnode, depth int64,
	key, value []byte) (*Llrbnode, *Llrbnode, *Llrbnode) {

	var oldnd, newnd *Llrbnode
	var dirty bool

	if nd == nil {
		newnd := llrb.newnode(key, value)
		llrb.upsertdepth.add(depth)
		return newnd, newnd, nil
	}

	nd = llrb.walkdownrot23(nd)

	if nd.gtkey(key) {
		nd.left, newnd, oldnd = llrb.upsert(nd.left, depth+1, key, value)
	} else if nd.ltkey(key) {
		nd.right, newnd, oldnd = llrb.upsert(nd.right, depth+1, key, value)
	} else {
		oldnd, dirty = llrb.clone(nd), false
		if nv := nd.nodevalue(); nv != nil { // free the value if present
			nv.pool.free(unsafe.Pointer(nv))
			nd, dirty = nd.setnodevalue(nil), true
		}
		if value != nil { // add new value if need be
			ptr, mpool := llrb.valarena.alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			nd, dirty = nd.setnodevalue(nv.setvalue(value)), true
		}
		newnd = nd
		if dirty {
			nd.metadata().setdirty()
		}
		llrb.upsertdepth.add(depth)
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd
}

func (llrb *LLRB) DeleteMin(callb LLRBDeleteCallback) {
	var root, deleted *Llrbnode
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	root, deleted = llrb.deletemin(nd)
	if root != nil {
		root.metadata().setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

	llrb.delcount(deleted)

	if callb != nil {
		callb(llrb, deleted)
	}
	llrb.freenode(deleted)
}

// using 2-3 trees
func (llrb *LLRB) deletemin(nd *Llrbnode) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}
	if nd.left == nil {
		return nil, nd
	}
	if !isred(nd.left) && !isred(nd.left.left) {
		nd = llrb.moveredleft(nd)
	}
	nd.left, deleted = llrb.deletemin(nd.left)
	return llrb.fixup(nd), deleted
}

func (llrb *LLRB) DeleteMax(callb LLRBDeleteCallback) {
	var root, deleted *Llrbnode

	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	root, deleted = llrb.deletemax(nd)
	if root != nil {
		root.metadata().setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

	llrb.delcount(deleted)

	if callb != nil {
		callb(llrb, deleted)
	}
	llrb.freenode(deleted)
}

// using 2-3 trees
func (llrb *LLRB) deletemax(nd *Llrbnode) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}
	if isred(nd.left) {
		nd = llrb.rotateright(nd)
	}
	if nd.right == nil {
		return nil, nd
	}
	if !isred(nd.right) && !isred(nd.right.left) {
		nd = llrb.moveredright(nd)
	}
	nd.right, deleted = llrb.deletemax(nd.right)
	return llrb.fixup(nd), deleted
}

func (llrb *LLRB) Delete(key []byte, callb LLRBDeleteCallback) {
	var deleted *Llrbnode

	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	root, deleted := llrb.delete(nd, key)
	if root != nil {
		root.metadata().setblack()
	}
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))

	llrb.delcount(deleted)

	if callb != nil {
		callb(llrb, deleted)
	}
	llrb.freenode(deleted)
}

func (llrb *LLRB) delete(nd *Llrbnode, key []byte) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}

	if nd.gtkey(key) {
		if nd.left == nil { // key not present. Nothing to delete
			return nd, nil
		}
		if !isred(nd.left) && !isred(nd.left.left) {
			nd = llrb.moveredleft(nd)
		}
		nd.left, deleted = llrb.delete(nd.left, key)

	} else {
		if isred(nd.left) {
			nd = llrb.rotateright(nd)
		}
		// If @key equals @h.Item and no right children at @h
		if !nd.ltkey(key) && nd.right == nil {
			return nil, nd
		}
		if nd.right != nil && !isred(nd.right) && !isred(nd.right.left) {
			nd = llrb.moveredright(nd)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !nd.ltkey(key) {
			var subdeleted *Llrbnode
			nd.right, subdeleted = llrb.deletemin(nd.right)
			if subdeleted == nil {
				panic("fatal logic, call the programmer")
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
			llrb.freenode(subdeleted)
		} else { // Else, @key is bigger than @nd
			nd.right, deleted = llrb.delete(nd.right, key)
		}
	}
	return llrb.fixup(nd), deleted
}

// rotation routines for 2-3 algorithm

func (llrb *LLRB) walkdownrot23(nd *Llrbnode) *Llrbnode {
	return nd
}

func (llrb *LLRB) walkuprot23(nd *Llrbnode) *Llrbnode {
	if isred(nd.right) && !isred(nd.left) {
		nd = llrb.rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = llrb.rotateright(nd)
	}
	if isred(nd.left) && isred(nd.right) {
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) rotateleft(nd *Llrbnode) *Llrbnode {
	y := nd.right
	if y.metadata().isblack() {
		panic("rotating a black link ? call the programmer")
	}
	nd.right = y.left
	y.left = nd
	if nd.metadata().isblack() {
		y.metadata().setblack()
	} else {
		y.metadata().setred()
	}
	nd.metadata().setred()
	return y
}

func (llrb *LLRB) rotateright(nd *Llrbnode) *Llrbnode {
	x := nd.left
	if x.metadata().isblack() {
		panic("rotating a black link ? call the programmer")
	}
	nd.left = x.right
	x.right = nd
	if nd.metadata().isblack() {
		x.metadata().setblack()
	} else {
		x.metadata().setred()
	}
	nd.metadata().setred()
	return x
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB) flip(nd *Llrbnode) {
	nd.left.metadata().togglelink()
	nd.right.metadata().togglelink()
	nd.metadata().togglelink()
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB) moveredleft(nd *Llrbnode) *Llrbnode {
	llrb.flip(nd)
	if isred(nd.right.left) {
		nd.right = llrb.rotateright(nd.right)
		nd = llrb.rotateleft(nd)
		llrb.flip(nd)
	}
	return nd
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB) moveredright(nd *Llrbnode) *Llrbnode {
	llrb.flip(nd)
	if isred(nd.left.left) {
		nd = llrb.rotateright(nd)
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) fixup(nd *Llrbnode) *Llrbnode {
	if isred(nd.right) {
		nd = llrb.rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = llrb.rotateright(nd)
	}
	if isred(nd.left) && isred(nd.right) {
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) Destroy() error {
	if llrb.dead == false {
		if llrb.mvcc.enabled {
			llrb.mvcc.writer.destroy()
		}
		llrb.nodearena.release()
		llrb.valarena.release()
		llrb.dead = true
		llrb.root, llrb.clock = nil, nil
		return nil
	}
	panic("Destroy() on a dead tree")
}

//---- Maintanence APIs.

func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.count) // concurrency safe.
}

func (llrb *LLRB) StatsMem() map[string]interface{} {
	mstats := map[string]interface{}{}
	overhead, useful := llrb.nodearena.memory()
	mstats["node.overhead"] = overhead
	mstats["node.useful"] = useful
	mstats["node.allocated"] = llrb.nodearena.allocated()
	mstats["node.available"] = llrb.nodearena.available()
	mstats["node.blocks"] = llrb.nodearena.blocksizes
	overhead, useful = llrb.valarena.memory()
	mstats["value.overhead"] = overhead
	mstats["value.useful"] = useful
	mstats["value.allocated"] = llrb.valarena.allocated()
	mstats["value.available"] = llrb.valarena.available()
	mstats["value.blocks"] = llrb.valarena.blocksizes
	mstats["keymemory"] = atomic.LoadInt64(&llrb.keymemory)
	mstats["valmemory"] = atomic.LoadInt64(&llrb.valmemory)
	return mstats
}

func (llrb *LLRB) StatsUpsert() map[string]interface{} {
	return map[string]interface{}{
		"upsertdepth.samples":     llrb.upsertdepth.samples(),
		"upsertdepth.min":         llrb.upsertdepth.min(),
		"upsertdepth.max":         llrb.upsertdepth.max(),
		"upsertdepth.mean":        llrb.upsertdepth.mean(),
		"upsertdepth.variance":    llrb.upsertdepth.variance(),
		"upsertdepth.stddeviance": llrb.upsertdepth.sd(),
	}
}

func (llrb *LLRB) StatsHeight() map[string]interface{} {
	heightav := &averageInt{}
	root := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	heightStats(root, 0, heightav)
	return map[string]interface{}{
		"samples":     heightav.samples(),
		"min":         heightav.min(),
		"max":         heightav.max(),
		"mean":        heightav.mean(),
		"variance":    heightav.variance(),
		"stddeviance": heightav.sd(),
	}
}

func heightStats(nd *Llrbnode, d int64, av *averageInt) {
	if nd == nil {
		return
	}
	d++
	av.add(d)
	if nd.left != nil {
		heightStats(nd.left, d, av)
	}
	if nd.right != nil {
		heightStats(nd.right, d, av)
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
			fmsg := "%v  %6v %10v/%-4v %2.2f%%\n"
			log.Infof(fmsg, llrb.logPrefix, size, capacity, len(mpools), z)
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
			fmsg := "%v  %6v %10v/%-4v %2.2f%%\n"
			log.Infof(fmsg, llrb.logPrefix, size, capacity, len(mpools), z)
		}
	}
}

func (llrb *LLRB) LogNodememory() {
	stats := llrb.StatsMem()
	min := humanize.Bytes(uint64(llrb.config["nodearena.minblock"].(int)))
	max := humanize.Bytes(uint64(llrb.config["nodearena.maxblock"].(int)))
	cp := humanize.Bytes(uint64(llrb.config["nodearena.capacity"].(int)))
	pcp := humanize.Bytes(uint64(llrb.config["nodearena.pool.capacity"].(int)))
	overh := humanize.Bytes(uint64(stats["node.overhead"].(int64)))
	use := humanize.Bytes(uint64(stats["node.useful"].(int64)))
	alloc := humanize.Bytes(uint64(stats["node.allocated"].(int64)))
	avail := humanize.Bytes(uint64(stats["node.available"].(int64)))
	nblocks := len(stats["node.blocks"].([]int64))
	kmem := humanize.Bytes(uint64(stats["keymemory"].(int64)))
	fmsg := "%v Nodes blksz:{%v-%v / %v} cap:{%v/%v}\n"
	log.Infof(fmsg, llrb.logPrefix, min, max, nblocks, cp, pcp)
	fmsg = "%v Nodes mem:{%v,%v - %v,%v} avail - %v\n"
	log.Infof(fmsg, llrb.logPrefix, use, overh, alloc, kmem, avail)
}

func (llrb *LLRB) LogValuememory() {
	stats := llrb.StatsMem()
	min := humanize.Bytes(uint64(llrb.config["valarena.minblock"].(int)))
	max := humanize.Bytes(uint64(llrb.config["valarena.maxblock"].(int)))
	cp := humanize.Bytes(uint64(llrb.config["valarena.capacity"].(int)))
	pcp := humanize.Bytes(uint64(llrb.config["valarena.pool.capacity"].(int)))
	overh := humanize.Bytes(uint64(stats["value.overhead"].(int64)))
	use := humanize.Bytes(uint64(stats["value.useful"].(int64)))
	alloc := humanize.Bytes(uint64(stats["value.allocated"].(int64)))
	avail := humanize.Bytes(uint64(stats["value.available"].(int64)))
	vblocks := len(stats["value.blocks"].([]int64))
	vmem := humanize.Bytes(uint64(stats["valmemory"].(int64)))
	fmsg := "%v Value blksz:{%v-%v / %v} cap:{%v/%v}\n"
	log.Infof(fmsg, llrb.logPrefix, min, max, vblocks, cp, pcp)
	fmsg = "%v Value mem:{%v,%v - %v,%v} avail - %v\n"
	log.Infof(fmsg, llrb.logPrefix, use, overh, alloc, vmem, avail)
}

func (llrb *LLRB) LogUpsertdepth() {
	stats := llrb.StatsUpsert()
	samples := stats["upsertdepth.samples"].(int64)
	min := stats["upsertdepth.min"].(int64)
	max := stats["upsertdepth.max"].(int64)
	mean := stats["upsertdepth.mean"]
	varn, sd := stats["upsertdepth.variance"], stats["upsertdepth.stddeviance"]
	fmsg := "%v UpsertDepth (%v) %v-%v %v/%2.2f/%2.2f\n"
	log.Infof(fmsg, llrb.logPrefix, samples, min, max, mean, varn, sd)
}

func (llrb *LLRB) LogTreeheight() {
	// log height statistics
	stats := llrb.StatsHeight()
	samples := stats["samples"]
	min, max := stats["min"], stats["max"]
	mean := stats["mean"]
	varn, sd := stats["variance"], stats["stddeviance"]
	fmsg := "%v HeightStats (%v) %v-%v %v/%2.2f/%2.2f\n"
	log.Infof(fmsg, llrb.logPrefix, samples, min, max, mean, varn, sd)
}

func (llrb *LLRB) ValidateReds() bool {
	root := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	if validatereds(root, isred(root)) != true {
		return false
	}
	return true
}

func validatereds(nd *Llrbnode, fromred bool) bool {
	if nd == nil {
		return true
	}
	if fromred && isred(nd) {
		panic("consequetive red spotted")
	}
	if validatereds(nd.left, isred(nd)) == false {
		return false
	}
	return validatereds(nd.right, isred(nd))
}

func (llrb *LLRB) ValidateBlacks() int {
	root := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	return validateblacks(root, 0)
}

func validateblacks(nd *Llrbnode, count int) int {
	if nd == nil {
		return count
	}
	if !isred(nd) {
		count++
	}
	x := validateblacks(nd.left, count)
	y := validateblacks(nd.right, count)
	if x != y {
		panic(fmt.Errorf("blacks on left %v, on right %v\n", x, y))
	}
	return x
}

func (llrb *LLRB) PPrint() {
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	fmt.Printf("root: ")
	nd.pprint("  ")
}

func (llrb *LLRB) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	nd.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

//---- local functions

func (llrb *LLRB) newnode(k, v []byte) *Llrbnode {
	mdsize := (&metadata{}).initMetadata(0, llrb.fmask).sizeof()
	ptr, mpool := llrb.nodearena.alloc(int64(llrbnodesize + mdsize + len(k)))
	nd := (*Llrbnode)(ptr)
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

func (llrb *LLRB) freenode(nd *Llrbnode) {
	if nd != nil {
		nv := nd.nodevalue()
		if nv != nil {
			nv.pool.free(unsafe.Pointer(nv))
		}
		nd.pool.free(unsafe.Pointer(nd))
	}
}

func (llrb *LLRB) clone(nd *Llrbnode) (newnd *Llrbnode) {
	// clone Llrbnode.
	newndptr, mpool := llrb.nodearena.alloc(nd.pool.size)
	newnd = (*Llrbnode)(newndptr)
	memcpy(unsafe.Pointer(newnd), unsafe.Pointer(nd), int(nd.pool.size))
	newnd.pool = mpool
	// clone value if value is present.
	if nd.metadata().ismvalue() {
		if mvalue, level := nd.metadata().mvalue(); level == 0 && mvalue != 0 {
			nv := (*nodevalue)(unsafe.Pointer((uintptr)(mvalue)))
			newnvptr, mpool := llrb.valarena.alloc(nv.pool.size)
			memcpy(newnvptr, unsafe.Pointer(nv), int(nv.pool.size))
			newnv := (*nodevalue)(newnvptr)
			newnv.pool = mpool
			newnd.setnodevalue(newnv)
		}
	}
	return
}

func (llrb *LLRB) upsertcounts(key, value []byte, oldnd *Llrbnode) {
	if oldnd == nil {
		atomic.AddInt64(&llrb.count, 1)
	} else {
		atomic.AddInt64(&llrb.keymemory, -int64(len(oldnd.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(oldnd.nodevalue().value())))
	}
	atomic.AddInt64(&llrb.keymemory, int64(len(key)))
	atomic.AddInt64(&llrb.valmemory, int64(len(value)))
}

func (llrb *LLRB) delcount(nd *Llrbnode) {
	if nd != nil {
		atomic.AddInt64(&llrb.keymemory, -int64(len(nd.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(nd.nodevalue().value())))
		atomic.AddInt64(&llrb.count, -1)
	}
}

func (llrb *LLRB) equivalent(n1, n2 *Llrbnode) bool {
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

// rotation routines for 2-3-4 algorithm, not used.

func (llrb *LLRB) walkdownrot234(nd *Llrbnode) *Llrbnode {
	if isred(nd.left) && isred(nd.right) {
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) walkuprot234(nd *Llrbnode) *Llrbnode {
	if isred(nd.right) && !isred(nd.left) {
		nd = llrb.rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = llrb.rotateright(nd)
	}
	return nd
}
