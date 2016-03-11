// Package storage implement a collection of indexing and persistance
// algorithm.
//
// LLRB:
//
//   * index key, value (value is optional).
//	 * each key shall be unique within the index sample-set.
//   * custom memory management
//   * copy on GC to control memory fragmentation.
//   * configurable metadata - like vbno, bornseqno, deadseqno, vbuuid etc..
//   * in single-threaded configuration, reads and writes are serialzed.
//   * supports multi-version-concurrency-control, where writes are
//     serialized, even if there are concurrent writers but there can be
//     zero or more concurrent readers.
//
// metadata fields are part of index entry, and describes them as:
//
//   a. 16 bit vbucket-number virtual bucket for the key.
//   b. 20 bit access time bits time.Now()[44:24].
//   i. and upto 12 optional fields that are configured.
//   1. 64 bit unique vbucket id for the vbucket number a.
//   2. 64 bit born-seqno vbucket seqno in which this entry was upserted.
//   3. 64 bit dead-seqno vbucket seqno in which this entry was deleted.
//   4. 64 bit mvalue either pointer to memory or fpos to disk to pick value.
//
//   few more will to be added...
//
// mvalue:
//
//   * value can be kept in memory or backed by disk.
//   * if value is kept in memory mvalue[:3] describes the memory offset to
//     fetch the entry value.
//   * if value is backed by disk then mvalue[2:], whose value might be 1-7
//     will pick a file that contains the value and mvalue[:3] describes the
//     file-position to fetch the entry from file.
//
// llrb hard limits:
//
//   * maximum vbuckets   - 65535
//   * maximum access     - 2^20 counted in steps of 16.7mS for 290 minutes
//   * maximum key size   - 4096 bytes
//   * maximum born seqno - (2^64 - 1)
//   * maximum dead seqno - (2^64 - 1)
//
// configuration:
//
//   "maxvb" - integer
//
//      maximum number of vbuckets that will used in this llrb tree.
//
//   "log.level" - string
//
//     can be "ignore", "fatal", "error", "warn", "info", "verbose",
//     "debug", or "trace"
//
//   "log.file" - string
//
//     log to file, if empty log to console
//
//   "nodearena.minblock" - integer
//
//     minimum node-block size that shall be requested from the arena.
//
//   "nodearena.maxblock" - integer
//
//     maximum node-block size that shall be requested from the arena.
//
//   "nodearena.capacity" - integer
//
//     capacity in bytes that the arena shall manage for node-blocks
//     using one or more pools.
//
//   "nodearena.pool.capacity" - integer
//     limit the size of a pool, irrespective of pool's block size.
//
//   "valarena.minblock" - integer
//     minimum value-block size that shall be requested from the arena.
//
//   "valarena.maxblock" - integer
//     maximum value-block size that shall be requested from the arena.
//
//   "valarena.capacity" - integer
//     capacity in bytes that the arena shall manage for node-blocks
//     using one or more pools.
//
//   "valarena.pool.capacity" - integer
//     limit the size of a pool, irrespective of pool's block size.
//
//   "metadata.bornseqno" - boolean
//     if true, use metadata field to book-keep node's born sequence number.
//
//   "metadata.deadseqno" - boolean
//     if true, use metadata field to book-keep node's dead sequence number.
//
//   "metadata.mvalue" - boolean
//     if true, and expect to index value for each key entry.
//
//   "metadata.vbuuid" - boolean
//     if true, use metadata field to book-keep node's vbuuid.
//
//   "mvcc.enabled" - boolean
//     consume LLRB as Multi-Version-Concurrency-Control-led tree.
//
//   "mvcc.snapshot.tick" - int
//     interval in milli-second for generating read-snapshots
//
//   "mvcc.writer.chanbuffer" - int
//     buffer size for mvcc writer's i/p channel
package storage

import "fmt"
import "unsafe"
import "io"
import "strings"
import "bytes"
import "sync"
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

// LLRB to manage in-memory sorted index using left-leaning-red-black trees.
type LLRB struct { // tree container
	name      string
	nodearena *memarena
	valarena  *memarena
	root      *Llrbnode
	dead      bool
	clock     *vectorclock // current clock
	rw        sync.RWMutex

	// config
	fmask     metadataMask // only 12 bits
	maxvb     int
	config    map[string]interface{}
	logPrefix string

	// reader statistics
	n_lookups int64
	n_ranges  int64

	// writer statistics
	n_count       int64 // number of nodes in the tree
	n_inserts     int64
	n_updates     int64
	n_deletes     int64
	n_allocs      int64
	n_frees       int64
	n_clones      int64
	keymemory     int64 // memory used by all keys
	valmemory     int64 // memory used by all values
	h_upsertdepth *histogramInt64

	// scratch pad
	strsl []string

	// mvcc
	mvcc struct {
		enabled  bool
		reclaim  []*Llrbnode
		writer   *LLRBWriter
		snapshot *LLRBSnapshot

		// stats
		n_snapshots int64
		n_purgedss  int64
		h_bulkfree  *histogramInt64
		h_reclaims  map[string]*histogramInt64
	}
}

// NewLLRB a new instance of in-memory sorted index.
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
	llrb.h_upsertdepth = newhistorgramInt64(1, 256, 1)

	// scratch pads
	llrb.strsl = make([]string, 0)

	// mvcc
	llrb.mvcc.enabled = config["mvcc.enabled"].(bool)
	if llrb.mvcc.enabled {
		llrb.mvcc.reclaim = make([]*Llrbnode, 0, 64)
		llrb.mvcc.h_bulkfree = newhistorgramInt64(1024, 1024*1024, 4*1024)
		llrb.mvcc.h_reclaims = map[string]*histogramInt64{
			"upsert": newhistorgramInt64(4, 1024, 4),
			"delmin": newhistorgramInt64(4, 1024, 4),
			"delmax": newhistorgramInt64(4, 1024, 4),
			"delete": newhistorgramInt64(4, 1024, 4),
		}
		llrb.MVCCWriter()
	}

	log.Infof("%v started ...\n", llrb.logPrefix)
	llrb.logconfig(config)
	return llrb
}

// ---- Index{} interface

// Id implement Index{} interface.
func (llrb *LLRB) Id() string {
	return llrb.name
}

// Count implement Index{} interface.
func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.n_count)
}

// Isactive implement Index{} interface.
func (llrb *LLRB) Isactive() bool {
	return llrb.dead == false
}

// Refer implement Snapshot{} interface.
func (llrb *LLRB) Refer() {
	panic("call Refer() on snapshot")
}

// Release implement Snapshot{} interface.
func (llrb *LLRB) Release() {
	panic("call Release() on snapshot")
}

// RSnapshot implement Index{} interface.
func (llrb *LLRB) RSnapshot() (Snapshot, error) {
	if llrb.mvcc.enabled {
		snapshot, err := llrb.mvcc.writer.getSnapshot()
		if err != nil {
			atomic.AddInt64(&llrb.mvcc.n_snapshots, 1)
		}
		return snapshot, err
	}
	panic("RSnapshot(): mvcc is not enabled")
}

// Destroy implement Index{} interface.
func (llrb *LLRB) Destroy() error {
	if llrb.dead == false {
		if llrb.mvcc.enabled {
			llrb.mvcc.writer.destroy()
			llrb.mvcc.reclaim, llrb.mvcc.writer = nil, nil
			llrb.mvcc.h_reclaims = nil
		}
		llrb.nodearena.release()
		llrb.valarena.release()
		llrb.root, llrb.clock = nil, nil
		llrb.config, llrb.strsl = nil, nil
		llrb.dead = true
		return nil
	}
	panic("Destroy() on already dead tree")
}

// Stats implement Indexer{} interface.
func (llrb *LLRB) Stats(involved int) (map[string]interface{}, error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.stats(involved)
	}
	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	return llrb.stats(involved)
}

// Validate implement Indexer{} interface.
func (llrb *LLRB) Validate() {
	if llrb.mvcc.enabled {
		if err := llrb.mvcc.writer.validate(); err != nil {
			panic(err)
		}
	}
	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	llrb.validate(llrb.root)
}

// Log implement Indexer{} interface.
func (llrb *LLRB) Log(involved int, humanize bool) {
	if llrb.mvcc.enabled {
		llrb.mvcc.writer.log(involved, humanize)
		return
	}
	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	llrb.log(involved, humanize)
}

//---- Reader{} interface.

// Has implement Reader{} interface.
func (llrb *LLRB) Has(key []byte) bool {
	if llrb.mvcc.enabled {
		panic("mvcc enabled, use snapshots for reading")
	}
	return llrb.Get(key) != nil
}

// Get implement Reader{} interface.
func (llrb *LLRB) Get(key []byte) Node {
	if llrb.mvcc.enabled {
		panic("mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	defer atomic.AddInt64(&llrb.n_lookups, 1)

	nd := llrb.root
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

// Min implement Reader{} interface.
func (llrb *LLRB) Min() Node {
	if llrb.mvcc.enabled {
		panic("mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	defer atomic.AddInt64(&llrb.n_lookups, 1)

	var nd *Llrbnode
	if nd = llrb.root; nd == nil {
		return nil
	}

	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

// Max implement Reader{} interface.
func (llrb *LLRB) Max() Node {
	if llrb.mvcc.enabled {
		panic("mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	defer atomic.AddInt64(&llrb.n_lookups, 1)

	var nd *Llrbnode
	if nd = llrb.root; nd == nil {
		return nil
	}

	for nd.right != nil {
		nd = nd.right
	}
	return nd
}

// Range from lkey to hkey, incl can be "both", "low", "high", "none"
func (llrb *LLRB) Range(lkey, hkey []byte, incl string, iter NodeIterator) {
	if llrb.mvcc.enabled {
		panic("mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	defer atomic.AddInt64(&llrb.n_ranges, 1)

	switch incl {
	case "both":
		llrb.rangeFromFind(llrb.root, lkey, hkey, iter)
	case "high":
		llrb.rangeAfterFind(llrb.root, lkey, hkey, iter)
	case "low":
		llrb.rangeFromTill(llrb.root, lkey, hkey, iter)
	default:
		llrb.rangeAfterTill(llrb.root, lkey, hkey, iter)
	}
}

//---- Writer{} interface

// Upsert implement Writer{} interface.
func (llrb *LLRB) Upsert(key, value []byte, callb UpsertCallback) error {
	if key == nil {
		panic("upserting nil key")
	}

	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wupsert(key, value, callb)
	}

	llrb.rw.Lock()
	defer llrb.rw.Unlock()

	root, newnd, oldnd := llrb.upsert(llrb.root, 1 /*depth*/, key, value)
	root.metadata().setblack()
	llrb.root = root

	llrb.upsertcounts(key, value, oldnd)

	if callb != nil {
		callb(llrb, llndornil(newnd), llndornil(oldnd))
	}
	if newnd.metadata().isdirty() {
		newnd.metadata().cleardirty()
	} else {
		panic("expected this to be dirty")
	}
	llrb.freenode(oldnd)
	return nil
}

// returns root, newnd, oldnd
func (llrb *LLRB) upsert(
	nd *Llrbnode, depth int64,
	key, value []byte) (*Llrbnode, *Llrbnode, *Llrbnode) {

	var oldnd, newnd *Llrbnode
	var dirty bool

	if nd == nil {
		newnd := llrb.newnode(key, value)
		llrb.h_upsertdepth.add(depth)
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
		llrb.h_upsertdepth.add(depth)
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd
}

// DeleteMin implement Writer{} interface.
func (llrb *LLRB) DeleteMin(callb DeleteCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdeleteMin(callb)
	}

	llrb.rw.Lock()
	defer llrb.rw.Unlock()

	root, deleted := llrb.deletemin(llrb.root)
	if root != nil {
		root.metadata().setblack()
	}
	llrb.root = root

	llrb.delcount(deleted)

	if callb != nil {
		callb(llrb, llndornil(deleted))
	}
	llrb.freenode(deleted)
	return nil
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

// DeleteMax implements Writer{} interface.
func (llrb *LLRB) DeleteMax(callb DeleteCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdeleteMax(callb)
	}

	llrb.rw.Lock()
	defer llrb.rw.Unlock()

	root, deleted := llrb.deletemax(llrb.root)
	if root != nil {
		root.metadata().setblack()
	}
	llrb.root = root

	llrb.delcount(deleted)

	if callb != nil {
		callb(llrb, llndornil(deleted))
	}
	llrb.freenode(deleted)
	return nil
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

// Delete implement Writer{} interface.
func (llrb *LLRB) Delete(key []byte, callb DeleteCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdelete(key, callb)
	}

	llrb.rw.Lock()
	defer llrb.rw.Unlock()

	root, deleted := llrb.delete(llrb.root, key)
	if root != nil {
		root.metadata().setblack()
	}
	llrb.root = root

	llrb.delcount(deleted)

	if callb != nil {
		callb(llrb, llndornil(deleted))
	}
	llrb.freenode(deleted)
	return nil
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
				//newnd.metadata().setdirty()
				panic("unexpected dirty node, call the programmer")
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

func (llrb *LLRB) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	llrb.root.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

//---- local functions

func (llrb *LLRB) newnode(k, v []byte) *Llrbnode {
	defer atomic.AddInt64(&llrb.n_allocs, 1)

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
		defer atomic.AddInt64(&llrb.n_frees, 1)
		nv := nd.nodevalue()
		if nv != nil {
			nv.pool.free(unsafe.Pointer(nv))
		}
		nd.pool.free(unsafe.Pointer(nd))
	}
}

func (llrb *LLRB) clone(nd *Llrbnode) (newnd *Llrbnode) {
	defer atomic.AddInt64(&llrb.n_clones, 1)

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
		atomic.AddInt64(&llrb.n_count, 1)
		atomic.AddInt64(&llrb.n_inserts, 1)
	} else {
		atomic.AddInt64(&llrb.keymemory, -int64(len(oldnd.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(oldnd.nodevalue().value())))
		atomic.AddInt64(&llrb.n_updates, 1)
	}
	atomic.AddInt64(&llrb.keymemory, int64(len(key)))
	atomic.AddInt64(&llrb.valmemory, int64(len(value)))
}

func (llrb *LLRB) delcount(nd *Llrbnode) {
	if nd != nil {
		atomic.AddInt64(&llrb.keymemory, -int64(len(nd.key())))
		atomic.AddInt64(&llrb.valmemory, -int64(len(nd.nodevalue().value())))
		atomic.AddInt64(&llrb.n_count, -1)
		atomic.AddInt64(&llrb.n_deletes, 1)
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

func (llrb *LLRB) logconfig(config map[string]interface{}) {
	// key arena
	stats, err := llrb.stats(1)
	if err != nil {
		panic(err)
	}
	kblocks := len(stats["node.blocks"].([]int64))
	min := humanize.Bytes(uint64(llrb.config["nodearena.minblock"].(int)))
	max := humanize.Bytes(uint64(llrb.config["nodearena.maxblock"].(int)))
	cp := humanize.Bytes(uint64(llrb.config["nodearena.capacity"].(int)))
	pcp := humanize.Bytes(uint64(llrb.config["nodearena.pool.capacity"].(int)))
	fmsg := "%v key arena %v blocks over {%v %v} cap %v poolcap %v\n"
	log.Infof(fmsg, llrb.logPrefix, kblocks, min, max, cp, pcp)

	// value arena
	vblocks := len(stats["value.blocks"].([]int64))
	min = humanize.Bytes(uint64(llrb.config["valarena.minblock"].(int)))
	max = humanize.Bytes(uint64(llrb.config["valarena.maxblock"].(int)))
	cp = humanize.Bytes(uint64(llrb.config["valarena.capacity"].(int)))
	pcp = humanize.Bytes(uint64(llrb.config["valarena.pool.capacity"].(int)))
	fmsg = "%v val arena %v blocks over {%v %v} cap %v poolcap %v\n"
	log.Infof(fmsg, llrb.logPrefix, vblocks, min, max, cp, pcp)
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
