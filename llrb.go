package storage

import "fmt"
import "time"
import "unsafe"
import "io"
import "strings"
import "bytes"
import "sync"
import "sync/atomic"

import humanize "github.com/dustin/go-humanize"

// MinKeymem minimum key size.
const MinKeymem = 32

// MaxKeymem maximum key size.
const MaxKeymem = 4096

// MinValmem minimum value size.
const MinValmem = 0

// MaxValmem maximum value size.
const MaxValmem = 10 * 1024 * 1024

var memratio = 0.5 // keymemory / allocated for each arena

// LLRB to manage in-memory sorted index using left-leaning-red-black trees.
type LLRB struct { // tree container
	// 64-bit aligned reader statistics
	n_lookups int64
	n_ranges  int64

	// 64-bit aligned writer statistics
	n_count   int64 // number of nodes in the tree
	n_inserts int64
	n_updates int64
	n_deletes int64
	n_nodes   int64
	n_frees   int64
	n_clones  int64
	keymemory int64 // memory used by all keys
	valmemory int64 // memory used by all values

	// mvcc
	mvcc struct {
		ismut int64
		// 64-bit aligned statistics
		n_snapshots int64
		n_purgedss  int64
		n_activess  int64
		n_cclookups int64
		n_ccranges  int64

		// can be unaligned fields

		enabled    bool
		reclaim    []*Llrbnode
		writer     *LLRBWriter
		snapshot   *LLRBSnapshot
		n_reclaims int64
		h_bulkfree *histogramInt64
		h_reclaims map[string]*histogramInt64
		h_versions *histogramInt64
	}

	h_upsertdepth *histogramInt64

	// can be unaligned fields

	name      string
	nodearena *memarena
	valarena  *memarena
	root      *Llrbnode
	borntime  time.Time
	dead      bool
	clock     *vectorclock // current clock
	rw        sync.RWMutex

	// config
	fmask     metadataMask // only 12 bits
	mdsize    int
	maxvb     int
	config    map[string]interface{}
	logPrefix string

	// scratch pad
	strsl []string
}

// NewLLRB a new instance of in-memory sorted index.
func NewLLRB(name string, config map[string]interface{}, logg Logger) *LLRB {
	config = mixinconfig(llrbConfig(), config)

	llrb := &LLRB{name: name, borntime: time.Now()}

	llrb.validateConfig(config)

	llrb.maxvb = config["maxvb"].(int)
	llrb.clock = newvectorclock(llrb.maxvb)

	// setup arena for nodes and node-values.
	llrb.nodearena = llrb.newnodearena(config)
	llrb.valarena = llrb.newvaluearena(config)

	// set up logger
	llrb.logPrefix = fmt.Sprintf("[LLRB-%s]", name)

	// set up metadata options
	llrb.fmask = llrb.setupfmask(config)
	llrb.mdsize = (&metadata{}).initMetadata(0, llrb.fmask).sizeof()
	llrb.config = config

	// statistics
	llrb.h_upsertdepth = newhistorgramInt64(1, 256, 1)

	// scratch pads
	llrb.strsl = make([]string, 0)

	// mvcc
	llrb.mvcc.enabled = config["mvcc.enable"].(bool)
	if llrb.mvcc.enabled {
		llrb.mvcc.reclaim = make([]*Llrbnode, 0, 64)
		llrb.mvcc.h_bulkfree = newhistorgramInt64(200000, 500000, 100000)
		llrb.mvcc.h_reclaims = map[string]*histogramInt64{
			"upsert": newhistorgramInt64(4, 1024, 4),
			"upmany": newhistorgramInt64(4, 1024, 4),
			"delmin": newhistorgramInt64(4, 1024, 4),
			"delmax": newhistorgramInt64(4, 1024, 4),
			"delete": newhistorgramInt64(4, 1024, 4),
		}
		llrb.mvcc.h_versions = newhistorgramInt64(0, 32, 1)
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

// Setaccess implement Index{} interface.
func (llrb *LLRB) Setaccess(nd Node) {
	nd.Setaccess(uint64(time.Now().UnixNano() >> 30))
}

// Isactive implement Index{} interface.
func (llrb *LLRB) Isactive() bool {
	return llrb.dead == false
}

// Refer implement Snapshot{} interface.
func (llrb *LLRB) Refer() {
	panic("Refer(): only allowed on snapshot")
}

// Release implement Snapshot{} interface.
func (llrb *LLRB) Release() {
	panic("Release(): only allowed on snapshot")
}

// RSnapshot implement Index{} interface.
func (llrb *LLRB) RSnapshot(snapch chan IndexSnapshot) error {
	if llrb.mvcc.enabled {
		err := llrb.mvcc.writer.getSnapshot(snapch)
		if err != nil {
			return err
		}
		return nil
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
	panic("Destroy(): already dead tree")
}

// Stats implement Indexer{} interface.
func (llrb *LLRB) Stats() (map[string]interface{}, error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.stats()
	}

	llrb.rw.RLock()

	stats, err := llrb.stats()

	llrb.rw.RUnlock()
	return stats, err
}

// Fullstats implement Indexer{} interface.
func (llrb *LLRB) Fullstats() (map[string]interface{}, error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.fullstats()
	}

	llrb.rw.RLock()

	stats, err := llrb.fullstats()

	llrb.rw.RUnlock()
	return stats, err
}

// Validate implement Indexer{} interface.
func (llrb *LLRB) Validate() {
	if llrb.mvcc.enabled {
		if err := llrb.mvcc.writer.validate(); err != nil {
			panic(fmt.Errorf("Validate(): %v", err))
		}
	}
	llrb.rw.RLock()

	llrb.validate(llrb.root)

	llrb.rw.RUnlock()
}

// Log implement Indexer{} interface.
func (llrb *LLRB) Log(involved int, humanize bool) {
	if llrb.mvcc.enabled {
		llrb.mvcc.writer.log(involved, humanize)
		return
	}
	llrb.rw.RLock()

	llrb.log(involved, humanize)

	llrb.rw.RUnlock()
}

//---- IndexReader{} interface.

// Has implement IndexReader{} interface.
func (llrb *LLRB) Has(key []byte) bool {
	if llrb.mvcc.enabled {
		panic("Has(): mvcc enabled, use snapshots for reading")
	}
	return llrb.Get(key) != nil
}

// Get implement IndexReader{} interface.
func (llrb *LLRB) Get(key []byte) Node {
	if llrb.mvcc.enabled {
		panic("Get(): mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()

	nd := llrb.get(key)

	llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_lookups, 1)
	return nd
}

func (llrb *LLRB) get(key []byte) Node {
	nd := llrb.root
	for nd != nil {
		if nd.gtkey(llrb.mdsize, key) {
			nd = nd.left
		} else if nd.ltkey(llrb.mdsize, key) {
			nd = nd.right
		} else {
			return nd
		}
	}
	return nil // key is not present in the tree
}

// Min implement IndexReader{} interface.
func (llrb *LLRB) Min() Node {
	if llrb.mvcc.enabled {
		panic("Min(): mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()

	nd := llrb.min()

	llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_lookups, 1)
	return nd
}

func (llrb *LLRB) min() Node {
	var nd *Llrbnode
	if nd = llrb.root; nd == nil {
		return nil
	}
	for nd.left != nil {
		nd = nd.left
	}
	return nd
}

// Max implement IndexReader{} interface.
func (llrb *LLRB) Max() Node {
	if llrb.mvcc.enabled {
		panic("Max(): mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()

	nd := llrb.max()

	llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_lookups, 1)
	return nd
}

func (llrb *LLRB) max() Node {
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
func (llrb *LLRB) Range(lkey, hkey []byte, incl string, iter RangeCallb) {
	if llrb.mvcc.enabled {
		panic("Range(): mvcc enabled, use snapshots for reading")
	}

	llrb.rw.RLock()

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

	llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_ranges, 1)
}

//---- IndexWriter{} interface

// Upsert implement IndexWriter{} interface.
func (llrb *LLRB) Upsert(key, value []byte, callb UpsertCallback) error {
	if key == nil {
		panic("Upsert(): upserting nil key")
	}

	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wupsert(key, value, callb)
	}

	llrb.rw.Lock()

	root, newnd, oldnd := llrb.upsert(llrb.root, 1 /*depth*/, key, value)
	root.metadata().setblack()
	llrb.root = root
	llrb.upsertcounts(key, value, oldnd)

	if callb != nil {
		callb(llrb, 0, llndornil(newnd), llndornil(oldnd))
	}
	newnd.metadata().cleardirty()
	llrb.freenode(oldnd)

	llrb.rw.Unlock()
	return nil
}

// UpsertMany implement IndexWriter{} interface.
func (llrb *LLRB) UpsertMany(keys, values [][]byte, callb UpsertCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wupsertmany(keys, values, callb)
	}

	llrb.rw.Lock()

	for i, key := range keys {
		var value []byte
		value = nil
		if len(values) > 0 {
			value = values[i]
		}
		root, newnd, oldnd := llrb.upsert(llrb.root, 1 /*depth*/, key, value)
		root.metadata().setblack()
		llrb.root = root
		llrb.upsertcounts(key, value, oldnd)

		if callb != nil {
			callb(llrb, int64(i), llndornil(newnd), llndornil(oldnd))
		}
		newnd.metadata().cleardirty()
		llrb.freenode(oldnd)
	}

	llrb.rw.Unlock()
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

	if nd.gtkey(llrb.mdsize, key) {
		nd.left, newnd, oldnd = llrb.upsert(nd.left, depth+1, key, value)
	} else if nd.ltkey(llrb.mdsize, key) {
		nd.right, newnd, oldnd = llrb.upsert(nd.right, depth+1, key, value)
	} else {
		oldnd, dirty = llrb.clone(nd), false
		if nd.metadata().ismvalue() {
			if nv := nd.nodevalue(); nv != nil { // free the value if present
				nv.pool.free(unsafe.Pointer(nv))
				nd, dirty = nd.setnodevalue(nil), true
			}
		}
		if nd.metadata().ismvalue() && value != nil { // add new value if req.
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

// DeleteMin implement IndexWriter{} interface.
func (llrb *LLRB) DeleteMin(callb DeleteCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdeleteMin(callb)
	}

	llrb.rw.Lock()

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
	llrb.rw.Unlock()
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

// DeleteMax implements IndexWriter{} interface.
func (llrb *LLRB) DeleteMax(callb DeleteCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdeleteMax(callb)
	}

	llrb.rw.Lock()

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

	llrb.rw.Unlock()
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

// Delete implement IndexWriter{} interface.
func (llrb *LLRB) Delete(key []byte, callb DeleteCallback) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdelete(key, callb)
	}

	llrb.rw.Lock()

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

	llrb.rw.Unlock()
	return nil
}

func (llrb *LLRB) delete(nd *Llrbnode, key []byte) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}

	if nd.gtkey(llrb.mdsize, key) {
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
		if !nd.ltkey(llrb.mdsize, key) && nd.right == nil {
			return nil, nd
		}
		if nd.right != nil && !isred(nd.right) && !isred(nd.right.left) {
			nd = llrb.moveredright(nd)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !nd.ltkey(llrb.mdsize, key) {
			var subdeleted *Llrbnode
			nd.right, subdeleted = llrb.deletemin(nd.right)
			if subdeleted == nil {
				panic("delete(): fatal logic, call the programmer")
			}
			newnd := llrb.clone(subdeleted)
			newnd.left, newnd.right = nd.left, nd.right
			if nd.metadata().isdirty() {
				//newnd.metadata().setdirty()
				panic("delete(): unexpected dirty node, call the programmer")
			}
			if nd.metadata().isblack() {
				newnd.metadata().setblack()
			} else {
				newnd.metadata().setred()
			}
			if newnd.metadata().ismvalue() {
				newnd.nodevalue().setvalue(subdeleted.nodevalue().value())
			}
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
		panic("rotateleft(): rotating a black link ? call the programmer")
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
		panic("rotateright(): rotating a black link ? call the programmer")
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
	ptr, mpool := llrb.nodearena.alloc(int64(llrbnodesize + llrb.mdsize + len(k)))
	nd := (*Llrbnode)(ptr)
	nd.metadata().initMetadata(0, llrb.fmask).setdirty().setred()
	nd.setkey(llrb.mdsize, k)
	nd.pool, nd.left, nd.right = mpool, nil, nil

	if v != nil && nd.metadata().ismvalue() {
		ptr, mpool = llrb.valarena.alloc(int64(nvaluesize + len(v)))
		nv := (*nodevalue)(ptr)
		nv.pool = mpool
		nvarg := (uintptr)(unsafe.Pointer(nv.setvalue(v)))
		nd.metadata().setmvalue((uint64)(nvarg), 0)
	} else if v != nil {
		panic("newnode(): llrb tree not configured for accepting value")
	}

	llrb.n_nodes += 1
	return nd
}

func (llrb *LLRB) freenode(nd *Llrbnode) {
	if nd != nil {
		if nd.metadata().ismvalue() {
			nv := nd.nodevalue()
			if nv != nil {
				nv.pool.free(unsafe.Pointer(nv))
			}
		}
		nd.pool.free(unsafe.Pointer(nd))
		llrb.n_frees += 1
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
	llrb.n_clones += 1
	return
}

func (llrb *LLRB) upsertcounts(key, value []byte, oldnd *Llrbnode) {
	if oldnd == nil {
		llrb.n_count += 1
		llrb.n_inserts += 1
	} else {
		llrb.keymemory -= int64(len(oldnd.key(llrb.mdsize)))
		if oldnd.metadata().ismvalue() {
			llrb.valmemory -= int64(len(oldnd.nodevalue().value()))
		}
		llrb.n_updates += 1
	}
	llrb.keymemory += int64(len(key))
	llrb.valmemory += int64(len(value))
}

func (llrb *LLRB) delcount(nd *Llrbnode) {
	if nd != nil {
		llrb.keymemory -= int64(len(nd.key(llrb.mdsize)))
		if nd.metadata().ismvalue() {
			llrb.valmemory -= int64(len(nd.nodevalue().value()))
		}
		llrb.n_count -= 1
		llrb.n_deletes += 1
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
	} else if bytes.Compare(n1.key(llrb.mdsize), n2.key(llrb.mdsize)) != 0 {
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
	stats, err := llrb.stats()
	if err != nil {
		panic(fmt.Errorf("logconfig(): %v", err))
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
