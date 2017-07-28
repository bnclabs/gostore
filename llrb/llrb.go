package llrb

import "fmt"
import "time"
import "unsafe"
import "io"
import "strings"
import "bytes"
import "sync"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import humanize "github.com/dustin/go-humanize"

// LLRB manage a single instance of in-memory sorted index using
// left-leaning-red-black tree.
type LLRB struct { // tree container
	// all are 64-bit aligned
	llrbstats

	// mvcc
	mvcc struct {
		ismut int64 // need to be 64-bit aligned
		// can be unaligned fields
		enabled    bool // comes from mvcc.enable settings
		reclaim    []*Llrbnode
		writer     *LLRBWriter
		snapshot   *LLRBSnapshot
		h_bulkfree *lib.HistogramInt64
		h_reclaims map[string]*lib.HistogramInt64
		h_versions *lib.HistogramInt64
	}
	h_upsertdepth *lib.HistogramInt64

	// can be unaligned fields

	name      string
	nodearena api.Mallocer
	valarena  api.Mallocer
	root      unsafe.Pointer // *Llrbnode
	borntime  time.Time
	clock     unsafe.Pointer // api.Clock
	dead      bool
	rw        sync.RWMutex
	iterpool  chan *iterator

	// settings
	fmask          metadataMask // only 12 bits
	mdsize         int
	iterpoolsize   int64 // iterpool.size
	lsm            bool
	minkeysize     int64
	maxkeysize     int64
	minvalsize     int64
	maxvalsize     int64
	keycapacity    int64
	valcapacity    int64
	maxlimit       int64
	snaptick       int64 // mvcc settings
	writechansz    int64 // mvcc settings
	memutilization float64
	setts          s.Settings
	logprefix      string

	// scratch pad
	strsl []string
}

// NewLLRB a new instance of in-memory sorted index.
func NewLLRB(name string, setts s.Settings) *LLRB {
	llrb := &LLRB{name: name, borntime: time.Now()}
	llrb.logprefix = fmt.Sprintf("LLRB [%s]", name)

	setts = make(s.Settings).Mixin(Defaultsettings(), setts)
	llrb.readsettings(setts)
	llrb.iterpool = make(chan *iterator, llrb.iterpoolsize)

	// setup arena for nodes and node-values.
	llrb.nodearena = llrb.newnodearena(setts)
	llrb.valarena = llrb.newvaluearena(setts)

	// set up metadata options
	llrb.fmask = setupfmask(setts)
	llrb.mdsize = (&metadata{}).initMetadata(0, llrb.fmask).sizeof()
	llrb.setts = setts

	// statistics
	llrb.h_upsertdepth = lib.NewhistorgramInt64(10, 100, 10)
	llrb.initmvccstats()

	// scratch pads
	llrb.strsl = make([]string, 0)

	log.Infof("%v started ...\n", llrb.logprefix)
	llrb.logarenasettings()
	// mvcc
	if llrb.mvcc.enabled {
		llrb.enableMVCC()
	}
	return llrb
}

func (llrb *LLRB) getroot() *Llrbnode {
	return (*Llrbnode)(atomic.LoadPointer(&llrb.root))
}

func (llrb *LLRB) setroot(root *Llrbnode) {
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
}

// ExpectedUtilization for validating memory consumption.
// Set this to minimum expected ratio of keymemory / allocated,
// before calling llrb.Validate().
func (llrb *LLRB) ExpectedUtilization(ut float64) {
	llrb.memutilization = ut
}

// EnableMVCC will spawn the writer routine and snapshot routine. MVCC
// can slow down write operation, and for initial data load LLRM can be
// instantiated with mvcc disabled. And subsequently enabled when it
// switches to active/incremental load.
func (llrb *LLRB) EnableMVCC() {
	llrb.setts = (s.Settings{}).Mixin(
		llrb.setts,
		s.Settings{"mvcc.enable": true},
	)
	llrb.mvcc.enabled = true
	llrb.enableMVCC()
}

// Dotdump to convert whole tree into dot script that can be visualized using
// graphviz.
func (llrb *LLRB) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	llrb.getroot().dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

// ---- api.IndexMeta{} interface

// ID implement api.IndexMeta interface.
func (llrb *LLRB) ID() string {
	return llrb.name
}

// Count implement api.IndexMeta interface.
func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.n_count)
}

// Isactive implement api.IndexMeta interface.
func (llrb *LLRB) Isactive() bool {
	return llrb.dead == false
}

// Getclock implement api.IndexMeta interface.
func (llrb *LLRB) Getclock() api.Clock {
	clock := (*api.Clock)(atomic.LoadPointer(&llrb.clock))
	if clock == nil {
		return nil
	}
	return *clock
}

// Metadata implement api.IndexMeta interface. LLRB instances
// are transient and don't support backup and restore of context.
// Return nil.
func (llrb *LLRB) Metadata() []byte {
	return nil
}

// Stats implement api.IndexMeta interface.
func (llrb *LLRB) Stats() (map[string]interface{}, error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.stats()
	}
	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	return llrb.stats(llrb)
}

// Fullstats implement api.IndexMeta interface.
func (llrb *LLRB) Fullstats() (map[string]interface{}, error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.fullstats()
	}

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	return llrb.fullstats(llrb)
}

// Validate implement api.IndexMeta interface. Will walk
// the full tree to confirm the sort order and check for
// memory leaks.
func (llrb *LLRB) Validate() {
	if llrb.mvcc.enabled {
		if err := llrb.mvcc.writer.validate(); err != nil {
			panic(fmt.Errorf("Validate(): %v", err))
		}
		return
	}
	llrb.rw.RLock()
	llrb.validate(llrb.getroot())
	llrb.rw.RUnlock()
}

// Log implement api.IndexMeta interface.
func (llrb *LLRB) Log(what string, humanize bool) {
	if llrb.mvcc.enabled {
		llrb.mvcc.writer.log(what, humanize)
		return
	}
	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	llrb.log(what, humanize)
}

// ---- api.Index interface

// RSnapshot implement api.Index interface. Snapshotting is allowed
// only if mvcc is enabled.
func (llrb *LLRB) RSnapshot(snapch chan api.IndexSnapshot, next bool) error {
	if llrb.mvcc.enabled {
		err := llrb.mvcc.writer.getSnapshot(snapch, next)
		if err != nil {
			return err
		}
		return nil
	}
	panic("RSnapshot(): mvcc is not enabled")
}

// Setclock implement api.Index interface.
func (llrb *LLRB) Setclock(clock api.Clock) {
	atomic.StorePointer(&llrb.clock, unsafe.Pointer(&clock))
}

// Clone implement api.Index interface. Avoid clone while there are
// incoming mutations.
func (llrb *LLRB) Clone(name string) (api.Index, error) {
	if llrb.mvcc.enabled {
		newllrb, err := llrb.mvcc.writer.clone(name)
		if err != nil {
			log.Errorf("%v Clone(): %v\n", llrb.logprefix, err)
		}
		return newllrb, err
	}

	// TODO: convert this into read lock ?
	llrb.rw.Lock()
	defer llrb.rw.Unlock()

	return llrb.doclone(name)
}

// Destroy implement api.Index interface.
func (llrb *LLRB) Destroy() error {
	if n_activeiter := atomic.LoadInt64(&llrb.n_activeiter); n_activeiter > 0 {
		log.Infof("%v n_activeiter: %v\n", llrb.logprefix, n_activeiter)
		return api.ErrorActiveIterators
	}
	if llrb.dead == false {
		if llrb.mvcc.enabled {
			llrb.mvcc.writer.destroy()
			llrb.mvcc.h_reclaims = nil
		}
		llrb.nodearena.Release()
		llrb.valarena.Release()
		llrb.setroot(nil)
		llrb.setts, llrb.strsl = nil, nil
		llrb.dead = true
		log.Infof("%v destroyed\n", llrb.logprefix)
		return nil
	}
	panic("Destroy(): already dead tree")
}

// ---- api.IndexSnapshot interface

// Refer implement api.IndexSnapshot interface. Call this method on
// llrb-snapshot, calling on this type will cause panic.
func (llrb *LLRB) Refer() {
	panic("Refer(): only allowed on snapshot")
}

// Release implement api.IndexSnapshot interface. Call this method on
// llrb-snapshot, calling on this type will cause panic.
func (llrb *LLRB) Release() {
	panic("Release(): only allowed on snapshot")
}

//---- api.IndexReader interface.

// Has implement api.IndexReader interface.
func (llrb *LLRB) Has(key []byte) bool {
	llrb.assertnomvcc()
	return llrb.Get(key, nil)
}

// Get implement api.IndexReader interface, acquires a read lock.
func (llrb *LLRB) Get(key []byte, callb api.NodeCallb) bool {
	llrb.assertnomvcc()

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_lookups, 1)
	_, ok := getkey(llrb, llrb.getroot(), key, callb)
	return ok
}

// Min implement api.IndexReader interface.
func (llrb *LLRB) Min(callb api.NodeCallb) bool {
	llrb.assertnomvcc()

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_lookups, 1)
	_, ok := getmin(llrb, llrb.getroot(), callb)
	return ok
}

// Max implement api.IndexReader interface.
func (llrb *LLRB) Max(callb api.NodeCallb) bool {
	llrb.assertnomvcc()

	llrb.rw.RLock()
	defer llrb.rw.RUnlock()
	atomic.AddInt64(&llrb.n_lookups, 1)
	_, ok := getmax(llrb, llrb.getroot(), callb)
	return ok
}

// Range from lkey to hkey, incl can be "both", "low", "high", "none"
func (llrb *LLRB) Range(
	lkey, hkey []byte, incl string, reverse bool, callb api.NodeCallb) {

	llrb.assertnomvcc()

	var skip bool
	lkey, hkey, incl, skip = fixrangeargs(lkey, hkey, incl)
	if skip {
		return
	}

	llrb.rw.RLock()
	dorange(llrb, llrb.getroot(), lkey, hkey, incl, reverse, callb)
	llrb.rw.RUnlock()

	atomic.AddInt64(&llrb.n_ranges, 1)
}

// Iterate implement api.IndexReader interface.
func (llrb *LLRB) Iterate(lk, hk []byte, incl string, r bool) api.IndexIterator {
	llrb.assertnomvcc()

	var skip bool
	lk, hk, incl, skip = fixrangeargs(lk, hk, incl)
	if skip {
		return nil
	}

	llrb.rw.RLock()
	iter := inititerator(llrb, llrb, lk, hk, incl, r)

	atomic.AddInt64(&llrb.n_ranges, 1)
	atomic.AddInt64(&llrb.n_activeiter, 1)
	return iter
}

//---- local functions

func (llrb *LLRB) initmvccstats() {
	llrb.mvcc.reclaim = make([]*Llrbnode, 0, 64)
	llrb.mvcc.h_bulkfree = lib.NewhistorgramInt64(100, 1000, 1000)
	llrb.mvcc.h_reclaims = map[string]*lib.HistogramInt64{
		"upsert":    lib.NewhistorgramInt64(10, 200, 20),
		"upsertcas": lib.NewhistorgramInt64(10, 200, 20),
		"mutations": lib.NewhistorgramInt64(10, 200, 20),
		"delmin":    lib.NewhistorgramInt64(10, 200, 20),
		"delmax":    lib.NewhistorgramInt64(10, 200, 20),
		"delete":    lib.NewhistorgramInt64(10, 200, 20),
	}
	llrb.mvcc.h_versions = lib.NewhistorgramInt64(1, 30, 10)
}

func (llrb *LLRB) enableMVCC() {
	llrb.spawnwriter()
}

func (llrb *LLRB) doclone(name string) (*LLRB, error) {
	if n_activeiter := atomic.LoadInt64(&llrb.n_activeiter); n_activeiter > 0 {
		fmsg := "Clone(): unexpected active-iterators %v"
		panic(fmt.Errorf(fmsg, n_activeiter))
	}

	newllrb := NewLLRB(llrb.name, llrb.setts)
	clock := llrb.Getclock()
	atomic.StorePointer(&newllrb.clock, unsafe.Pointer(&clock))
	newllrb.dead = llrb.dead

	newllrb.setroot(newllrb.clonetree(llrb.getroot()))

	return newllrb, nil
}

func (llrb *LLRB) assertnomvcc() {
	if llrb.mvcc.enabled {
		panic("mvcc enabled, use snapshots for reading")
	}
}

//---- api.IndexWriter interface

// Upsert implement api.IndexWriter interface.
func (llrb *LLRB) Upsert(key, value []byte, callb api.NodeCallb) error {
	if key == nil {
		panic("Upsert(): upserting nil key")
	}

	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wupsert(key, value, callb)
	}

	llrb.rw.Lock()

	root, newnd, oldnd := llrb.upsert(llrb.getroot(), 1 /*depth*/, key, value)
	root.metadata().setblack()
	llrb.setroot(root)
	llrb.upsertcounts(key, value, oldnd)

	if llrb.lsm && oldnd.IsDeleted() {
		newnd.metadata().cleardeleted()
		newnd.SetDeadseqno(0)
	}

	if callb != nil {
		callb(llrb, 0, llndornil(newnd), llndornil(oldnd), nil)
	}
	newnd.metadata().cleardirty()
	llrb.freenode(oldnd)

	llrb.rw.Unlock()
	return nil
}

// UpsertCas implement api.IndexWriter interface.
func (llrb *LLRB) UpsertCas(key, value []byte, cas uint64, callb api.NodeCallb) error {
	if key == nil {
		panic("Upsert(): upserting nil key")
	}

	if llrb.fmask.isBornSeqno() == false {
		if callb != nil {
			callb(llrb, 0, nil, nil, api.ErrorInvalidCAS)
		}
		return api.ErrorInvalidCAS
	}

	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wupsertcas(key, value, cas, callb)
	}

	llrb.rw.Lock()

	// Get to check for CAS
	if cas > 0 {
		var currcas uint64
		defer atomic.AddInt64(&llrb.n_casgets, 1)
		if nd, _ := getkey(llrb, llrb.getroot(), key, nil); nd != nil {
			currcas = nd.Bornseqno()
		}
		if currcas != cas {
			if callb != nil {
				callb(llrb, 0, nil, nil, api.ErrorInvalidCAS)
			}
			return api.ErrorInvalidCAS
		}
	}

	// if cas matches go ahead with upsert.
	root, newnd, oldnd := llrb.upsert(llrb.getroot(), 1 /*depth*/, key, value)
	root.metadata().setblack()
	llrb.setroot(root)
	llrb.upsertcounts(key, value, oldnd)

	if llrb.lsm && oldnd.IsDeleted() {
		newnd.metadata().cleardeleted()
		newnd.SetDeadseqno(0)
	}

	if callb != nil {
		callb(llrb, 0, llndornil(newnd), llndornil(oldnd), nil)
	}
	newnd.metadata().cleardirty()
	llrb.freenode(oldnd)

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
		llrb.h_upsertdepth.Add(depth)
		return newnd, newnd, nil
	}

	nd = llrb.walkdownrot23(nd)

	if nd.gtkey(llrb.mdsize, key, false) {
		nd.left, newnd, oldnd = llrb.upsert(nd.left, depth+1, key, value)
	} else if nd.ltkey(llrb.mdsize, key, false) {
		nd.right, newnd, oldnd = llrb.upsert(nd.right, depth+1, key, value)
	} else {
		oldnd, dirty = llrb.clonenode(nd), false
		if nd.metadata().ismvalue() {
			if nv := nd.nodevalue(); nv != nil { // free the value if present
				nv.pool.Free(unsafe.Pointer(nv))
				nd, dirty = nd.setnodevalue(nil), true
			}
		}
		if nd.metadata().ismvalue() && len(value) > 0 { // add new value if req.
			ptr, mpool := llrb.valarena.Alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.pool = mpool
			nd, dirty = nd.setnodevalue(nv.setvalue(value)), true
		}
		newnd = nd
		if dirty {
			nd.metadata().setdirty()
		}
		llrb.h_upsertdepth.Add(depth)
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd
}

// DeleteMin implement api.IndexWriter interface.
func (llrb *LLRB) DeleteMin(callb api.NodeCallb) (e error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdeleteMin(callb)
	}

	llrb.rw.Lock()

	if llrb.lsm {
		llrbnd, _ := getmin(llrb, llrb.getroot(), nil)
		if llrbnd != nil {
			llrbnd.metadata().setdeleted()
		}
		if callb != nil {
			callb(llrb, 0, llrbnd, llrbnd, nil)
		}

	} else {
		root, deleted := llrb.deletemin(llrb.getroot())
		if root != nil {
			root.metadata().setblack()
		}
		llrb.setroot(root)

		llrb.delcount(deleted)

		if callb != nil {
			nd := llndornil(deleted)
			callb(llrb, 0, nd, nd, nil)
		}
		llrb.freenode(deleted)
	}
	llrb.rw.Unlock()
	return
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

// DeleteMax implements api.IndexWriter interface.
func (llrb *LLRB) DeleteMax(callb api.NodeCallb) (e error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdeleteMax(callb)
	}

	llrb.rw.Lock()

	if llrb.lsm {
		llrbnd, _ := getmax(llrb, llrb.getroot(), nil)
		if llrbnd != nil {
			llrbnd.metadata().setdeleted()
		}
		if callb != nil {
			callb(llrb, 0, llrbnd, llrbnd, nil)
		}

	} else {
		root, deleted := llrb.deletemax(llrb.getroot())
		if root != nil {
			root.metadata().setblack()
		}
		llrb.setroot(root)

		llrb.delcount(deleted)

		if callb != nil {
			nd := llndornil(deleted)
			callb(llrb, 0, nd, nd, nil)
		}
		llrb.freenode(deleted)
	}
	llrb.rw.Unlock()
	return
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

// Delete implement api.IndexWriter interface.
func (llrb *LLRB) Delete(key []byte, callb api.NodeCallb) (e error) {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wdelete(key, callb)
	}

	llrb.rw.Lock()

	if llrb.lsm {
		llrbnd, _ := getkey(llrb, llrb.getroot(), key, nil)
		if llrbnd != nil {
			llrbnd.metadata().setdeleted()
			if callb != nil {
				callb(llrb, 0, llrbnd, llrbnd, nil)
			}

		} else {
			llrb.rw.Unlock()
			llrb.Upsert(
				key, nil,
				func(_ api.Index, _ int64, nnd, ond api.Node, err error) bool {
					llrbnd := nnd.(*Llrbnode)
					llrbnd.metadata().setdeleted()
					if callb != nil {
						callb(llrb, 0, nnd, ond, err)
					}
					return false
				})
			llrb.rw.Lock()
		}

	} else {
		root, deleted := llrb.delete(llrb.getroot(), key)
		if root != nil {
			root.metadata().setblack()
		}
		llrb.setroot(root)

		llrb.delcount(deleted)

		if deleted == nil { // handle key-missing
			if callb != nil {
				callb(llrb, 0, nil, nil, api.ErrorKeyMissing)
			}
			e = api.ErrorKeyMissing

		} else if callb != nil {
			nd := llndornil(deleted)
			callb(llrb, 0, nd, nd, nil)
		}
		llrb.freenode(deleted)
	}

	llrb.rw.Unlock()
	return
}

func (llrb *LLRB) delete(nd *Llrbnode, key []byte) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}

	if nd.gtkey(llrb.mdsize, key, false) {
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
		if !nd.ltkey(llrb.mdsize, key, false) && nd.right == nil {
			return nil, nd
		}
		if nd.right != nil && !isred(nd.right) && !isred(nd.right.left) {
			nd = llrb.moveredright(nd)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !nd.ltkey(llrb.mdsize, key, false) {
			var subdeleted *Llrbnode
			nd.right, subdeleted = llrb.deletemin(nd.right)
			if subdeleted == nil {
				panic("delete(): fatal logic, call the programmer")
			}
			newnd := llrb.clonenode(subdeleted)
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
			sdnv := subdeleted.nodevalue()
			if newnd.metadata().ismvalue() && sdnv != nil {
				newnd.nodevalue().setvalue(sdnv.value())
			}
			deleted, nd = nd, newnd
			llrb.freenode(subdeleted)
		} else { // Else, @key is bigger than @nd
			nd.right, deleted = llrb.delete(nd.right, key)
		}
	}
	return llrb.fixup(nd), deleted
}

// Mutations implement api.IndexWriter interface.
func (llrb *LLRB) Mutations(cmds []*api.MutationCmd, callb api.NodeCallb) error {
	if llrb.mvcc.enabled {
		return llrb.mvcc.writer.wmutations(cmds, callb)
	}

	var i int
	var mcmd *api.MutationCmd

	localfn := func(idx api.Index, _ int64, nnd, ond api.Node, err error) bool {
		if callb != nil {
			callb(idx, int64(i), nnd, ond, err)
		}
		return false
	}

	for i, mcmd = range cmds {
		switch mcmd.Cmd {
		case api.UpsertCmd:
			llrb.Upsert(mcmd.Key, mcmd.Value, localfn)
		case api.CasCmd:
			llrb.UpsertCas(mcmd.Key, mcmd.Value, mcmd.Cas, localfn)
		case api.DelminCmd:
			llrb.DeleteMin(localfn)
		case api.DelmaxCmd:
			llrb.DeleteMax(localfn)
		case api.DeleteCmd:
			llrb.Delete(mcmd.Key, localfn)
		default:
			panic(fmt.Errorf("invalid mutation command : {%v,%v}", i, mcmd.Cmd))
		}
	}
	return nil
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

//---- local functions

func (llrb *LLRB) newnode(k, v []byte) *Llrbnode {
	ptr, mpool := llrb.nodearena.Alloc(int64(nodesize + llrb.mdsize + len(k)))
	nd := (*Llrbnode)(ptr)
	nd.metadata().initMetadata(0, llrb.fmask).setdirty().setred()
	nd.setkey(llrb.mdsize, k)
	nd.pool, nd.left, nd.right = mpool, nil, nil

	if llrb.fmask.isDeadSeqno() {
		nd.SetDeadseqno(0)
	}

	if v != nil && nd.metadata().ismvalue() {
		ptr, mpool = llrb.valarena.Alloc(int64(nvaluesize + len(v)))
		nv := (*nodevalue)(ptr)
		nv.pool = mpool
		nvarg := (uintptr)(unsafe.Pointer(nv.setvalue(v)))
		nd.metadata().setmvalue((uint64)(nvarg))
	} else if v != nil {
		panic("newnode(): llrb tree not settings for accepting value")
	}

	llrb.n_nodes++
	return nd
}

func (llrb *LLRB) freenode(nd *Llrbnode) {
	if nd != nil {
		if nd.metadata().ismvalue() {
			nv := nd.nodevalue()
			if nv != nil {
				nv.pool.Free(unsafe.Pointer(nv))
				nd.setnodevalue(nil)
			}
		}
		nd.pool.Free(unsafe.Pointer(nd))
		llrb.n_frees++
	}
}

func (llrb *LLRB) clonetree(nd *Llrbnode) *Llrbnode {
	if nd == nil {
		return nil
	}

	newnd := llrb.clonenode(nd)
	llrb.n_clones--

	newnd.left = llrb.clonetree(nd.left)
	newnd.right = llrb.clonetree(nd.right)
	return newnd
}

func (llrb *LLRB) clonenode(nd *Llrbnode) (newnd *Llrbnode) {
	// clone Llrbnode.
	newndptr, mpool := llrb.nodearena.Alloc(nd.pool.Slabsize())
	newnd = (*Llrbnode)(newndptr)
	size := int(nd.pool.Slabsize())
	lib.Memcpy(unsafe.Pointer(newnd), unsafe.Pointer(nd), size)
	newnd.pool = mpool
	// clone value if value is present.
	if nd.metadata().ismvalue() {
		if mvalue := nd.metadata().mvalue(); mvalue != 0 {
			nv := (*nodevalue)(unsafe.Pointer((uintptr)(mvalue)))
			newnvptr, mpool := llrb.valarena.Alloc(nv.pool.Slabsize())
			lib.Memcpy(newnvptr, unsafe.Pointer(nv), int(nv.pool.Slabsize()))
			newnv := (*nodevalue)(newnvptr)
			newnv.pool = mpool
			newnd.setnodevalue(newnv)
		}
	}
	llrb.n_clones++
	return
}

func (llrb *LLRB) upsertcounts(key, value []byte, oldnd *Llrbnode) {
	if oldnd == nil {
		llrb.n_count++
		llrb.n_inserts++
	} else {
		llrb.keymemory -= int64(len(oldnd.key(llrb.mdsize)))
		if oldnd.metadata().ismvalue() {
			if nv := oldnd.nodevalue(); nv != nil {
				llrb.valmemory -= int64(len(nv.value()))
			}
		}
		llrb.n_updates++
	}
	llrb.keymemory += int64(len(key))
	llrb.valmemory += int64(len(value))
}

func (llrb *LLRB) delcount(nd *Llrbnode) {
	if nd != nil {
		llrb.keymemory -= int64(len(nd.key(llrb.mdsize)))
		if nd.metadata().ismvalue() {
			if nv := nd.nodevalue(); nv != nil {
				llrb.valmemory -= int64(len(nv.value()))
			}
		}
		llrb.n_count--
		llrb.n_deletes++
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

func (llrb *LLRB) logarenasettings() {
	// key arena
	stats, err := llrb.stats(llrb)
	if err != nil {
		panic(fmt.Errorf("logarenasettings(): %v", err))
	}
	kblocks := len(stats["node.blocks"].([]int64))
	min := humanize.Bytes(uint64(llrb.minkeysize))
	max := humanize.Bytes(uint64(llrb.maxkeysize))
	cp := humanize.Bytes(uint64(stats["node.capacity"].(int64)))
	fmsg := "%v key arena %v blocks over {%v %v} cap %v\n"
	log.Infof(fmsg, llrb.logprefix, kblocks, min, max, cp)

	// value arena
	vblocks := len(stats["value.blocks"].([]int64))
	min = humanize.Bytes(uint64(llrb.minvalsize))
	max = humanize.Bytes(uint64(llrb.maxvalsize))
	cp = humanize.Bytes(uint64(stats["value.capacity"].(int64)))
	fmsg = "%v val arena %v blocks over {%v %v} cap %v\n"
	log.Infof(fmsg, llrb.logprefix, vblocks, min, max, cp)
}

func (llrb *LLRB) getiterator() (iter *iterator) {
	select {
	case iter = <-llrb.iterpool:
	default:
		iter = &iterator{nodes: make([]api.Node, 0)}
	}
	return iter
}

func (llrb *LLRB) putiterator(iter *iterator) {
	select {
	case llrb.iterpool <- iter:
	default: // Let iter be collected by GC
	}
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
