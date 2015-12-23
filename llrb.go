// configuration:
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
// "log.level" - string
//		one of the following
//		"ignore", "fatal", "error", "warn", "info", "verbose", "debug", "trace"
//
// "log.file" - string
//		log to file, if empty log to console
//
// "mvcc.enabled" - boolean
//		consume LLRB as Multi-Version-Concurrency-Control-led tree.
//
// "mvcc.snapshot" - int
//		interval in milli-second for generating read-snapshots

package storage

import "fmt"
import "unsafe"
import "time"
import "sort"
import "bytes"
import "sync/atomic"

import humanize "github.com/dustin/go-humanize"

const MinKeymem = 96
const MaxKeymem = 4096
const MinValmem = 32
const MaxValmem = 10 * 1024 * 1024

type NdIterator func(nd *Llrbnode) bool

type LLRB struct { // tree container
	name      string
	nodearena *memarena
	valarena  *memarena
	root      unsafe.Pointer // root *Llrbnode of LLRB tree

	// config
	fmask     metadataMask // only 12 bits
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
		enabled    bool
		snaphead   unsafe.Pointer // *LLRBSnapshot
		cowednodes []*Llrbnode

		// config
		snapshotTick time.Duration

		// stats
		reclaimstats map[string]*averageInt

		// scratch pad
		reclaim []*Llrbnode
	}
}

func NewLLRB(name string, config map[string]interface{}, logg Logger) *LLRB {
	validateConfig(config)
	llrb := &LLRB{name: name}

	llrb.mvcc.enabled = config["mvcc.enabled"].(bool)
	llrb.mvcc.snapshotTick = time.Duration(config["mvcc.snapshotTick"].(int))
	llrb.mvcc.cowednodes = make([]*Llrbnode, 0, 64)

	// setup nodearena for key and metadata
	minblock := int64(config["nodearena.minblock"].(int))
	maxblock := int64(config["nodearena.maxblock"].(int))
	capacity := int64(config["nodearena.capacity"].(int))
	pcapacity := int64(config["nodearena.pool.capacity"].(int))
	llrb.nodearena = newmemarena(minblock, maxblock, capacity, pcapacity)

	// setup value arena
	minblock = int64(config["valarena.minblock"].(int))
	maxblock = int64(config["valarena.maxblock"].(int))
	capacity = int64(config["valarena.capacity"].(int))
	pcapacity = int64(config["valarena.pool.capacity"].(int))
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
	llrb.upsertdepth = &averageInt{}
	llrb.mvcc.reclaimstats = map[string]*averageInt{
		"upsert": &averageInt{},
		"delmin": &averageInt{},
		"delmax": &averageInt{},
		"delete": &averageInt{},
	}
	// scratch pads
	llrb.strsl = make([]string, 0)
	llrb.mvcc.reclaim = make([]*Llrbnode, 0, 64)
	return llrb
}

//---- Maintanence APIs.

func (llrb *LLRB) SetRoot(r *Llrbnode) {
	atomic.StorePointer(&llrb.root, unsafe.Pointer(r))
}

func (llrb *LLRB) Root() *Llrbnode {
	return (*Llrbnode)(atomic.LoadPointer(&llrb.root))
}

func (llrb *LLRB) Release() {
	llrb.nodearena.release()
	llrb.valarena.release()
}

func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.count)
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

func (llrb *LLRB) Freenode(nd *Llrbnode) { // TODO: should this be exported ?
	if nd != nil {
		nv := nd.nodevalue()
		if nv != nil {
			nv.pool.free(unsafe.Pointer(nv))
		}
		nd.pool.free(unsafe.Pointer(nd))
	}
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

func (llrb *LLRB) Min() *Llrbnode {
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	if nd == nil {
		return nil
	}
	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

func (llrb *LLRB) Max() *Llrbnode {
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
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
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
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
	nd *Llrbnode, lk, hk []byte, iter NdIterator) bool {

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
	nd *Llrbnode, lk, hk []byte, iter NdIterator) bool {

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
	nd *Llrbnode, lk, hk []byte, iter NdIterator) bool {

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
	nd *Llrbnode, lk, hk []byte, iter NdIterator) bool {

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

// caller should free old-Llrbnode if it is not null.
func (llrb *LLRB) Upsert(k, v []byte) (newnd, oldnd *Llrbnode) {
	var root *Llrbnode

	if k == nil {
		panic("upserting nil key")
	}
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
	root, newnd, oldnd = llrb.upsert(nd, 0 /*depth*/, k, v)
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
	nd *Llrbnode, depth int64,
	key, value []byte) (*Llrbnode, *Llrbnode, *Llrbnode) {

	var oldnd, newnd *Llrbnode

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
		llrb.upsertdepth.add(depth)
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd
}

func (llrb *LLRB) DeleteMin() *Llrbnode {
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
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
func (llrb *LLRB) deletemin(nd *Llrbnode) (newnd, deleted *Llrbnode) {
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

func (llrb *LLRB) DeleteMax() *Llrbnode {
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
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
func (llrb *LLRB) deletemax(nd *Llrbnode) (newnd, deleted *Llrbnode) {
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

func (llrb *LLRB) Delete(key []byte) *Llrbnode {
	nd := (*Llrbnode)(atomic.LoadPointer(&llrb.root))
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

func (llrb *LLRB) delete(nd *Llrbnode, key []byte) (newnd, deleted *Llrbnode) {
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
			var subdeleted *Llrbnode
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

func (llrb *LLRB) walkdownrot23(nd *Llrbnode) *Llrbnode {
	return nd
}

func (llrb *LLRB) walkuprot23(nd *Llrbnode) *Llrbnode {
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

func walkdownrot234(nd *Llrbnode) *Llrbnode {
	if isred(nd.left) && isred(nd.right) {
		flip(nd)
	}
	return nd
}

func walkuprot234(nd *Llrbnode) *Llrbnode {
	if isred(nd.right) && !isred(nd.left) {
		nd = rotateleft(nd)
	}
	if isred(nd.left) && isred(nd.left.left) {
		nd = rotateright(nd)
	}
	return nd
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

func (llrb *LLRB) clone(nd *Llrbnode) (newnd *Llrbnode) {
	// clone Llrbnode.
	newndu, mpool := llrb.nodearena.alloc(nd.pool.size)
	newnd = (*Llrbnode)(newndu)
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
