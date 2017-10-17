package llrb

import "io"
import "fmt"
import "time"
import "sync"
import "bytes"
import "unsafe"
import "strings"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/malloc"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import humanize "github.com/dustin/go-humanize"

// LLRB to manage a single instance of in-memory sorted index using
// left-leaning-red-black tree.
type LLRB struct { // tree container
	llrbstats           // 64-bit aligned snapshot statistics.
	activetxns    int64 // there can be more than on ro-txns
	h_upsertdepth *lib.HistogramInt64
	// can be unaligned fields
	name      string
	nodearena api.Mallocer
	valarena  api.Mallocer
	root      unsafe.Pointer // *Llrbnode
	seqno     uint64
	rw        sync.RWMutex
	finch     chan struct{}
	txnsmeta

	// settings
	keycapacity int64
	valcapacity int64
	allocator   string
	setts       s.Settings
	logprefix   string
}

// NewLLRB a new instance of in-memory sorted index.
func NewLLRB(name string, setts s.Settings) *LLRB {
	llrb := &LLRB{name: name, finch: make(chan struct{})}
	llrb.logprefix = fmt.Sprintf("LLRB [%s]", name)
	llrb.inittxns()

	setts = make(s.Settings).Mixin(Defaultsettings(), setts)
	llrb.readsettings(setts)
	llrb.setts = setts

	llrb.nodearena = malloc.NewArena(llrb.keycapacity, llrb.allocator)
	llrb.valarena = malloc.NewArena(llrb.valcapacity, llrb.allocator)

	// statistics
	llrb.h_upsertdepth = lib.NewhistorgramInt64(10, 100, 10)

	log.Infof("%v started ...\n", llrb.logprefix)
	llrb.logarenasettings()
	return llrb
}

//---- local accessor methods.

func (llrb *LLRB) readsettings(setts s.Settings) *LLRB {
	llrb.keycapacity = setts.Int64("keycapacity")
	llrb.valcapacity = setts.Int64("valcapacity")
	llrb.allocator = setts.String("allocator")
	return llrb
}

func (llrb *LLRB) getroot() *Llrbnode {
	return (*Llrbnode)(llrb.root)
}

func (llrb *LLRB) setroot(root *Llrbnode) {
	llrb.root = unsafe.Pointer(root)
}

func (llrb *LLRB) newnode(k, v []byte) *Llrbnode {
	ptr := llrb.nodearena.Alloc(int64(nodesize + len(k)))
	nd := (*Llrbnode)(ptr)
	nd.left, nd.right, nd.value = nil, nil, nil
	nd.seqflags, nd.hdr = 0, 0
	nd.setdirty().setred().setkey(k)
	if len(v) > 0 {
		ptr = llrb.valarena.Alloc(int64(nvaluesize + len(v)))
		nv := (*nodevalue)(ptr)
		nd.setnodevalue(nv.setvalue(v))
	}
	llrb.n_nodes++
	return nd
}

func (llrb *LLRB) freenode(nd *Llrbnode) {
	if nd != nil {
		if nv := nd.nodevalue(); nv != nil {
			llrb.valarena.Free(unsafe.Pointer(nv))
		}
		llrb.nodearena.Free(unsafe.Pointer(nd))
		llrb.n_frees++
	}
}

func (llrb *LLRB) clonenode(nd *Llrbnode) (newnd *Llrbnode) {
	slabsize := llrb.nodearena.Slabsize(unsafe.Pointer(nd))
	newptr := llrb.nodearena.Allocslab(slabsize)
	size := llrb.nodearena.Chunklen(unsafe.Pointer(nd))
	newnd = (*Llrbnode)(newptr)
	lib.Memcpy(unsafe.Pointer(newnd), unsafe.Pointer(nd), int(size))
	// clone value if value is present.
	if nv := nd.nodevalue(); nv != nil {
		slabsize = llrb.valarena.Slabsize(unsafe.Pointer(nv))
		newnvptr := llrb.valarena.Allocslab(slabsize)
		size := llrb.valarena.Chunklen(unsafe.Pointer(nv))
		lib.Memcpy(newnvptr, unsafe.Pointer(nv), int(size))
		newnv := (*nodevalue)(newnvptr)
		newnd.setnodevalue(newnv)
	}
	llrb.n_clones++
	return
}

func (llrb *LLRB) upsertcounts(key, value []byte, oldnd *Llrbnode) {
	llrb.keymemory += int64(len(key))
	llrb.valmemory += int64(len(value))
	if oldnd == nil {
		atomic.AddInt64(&llrb.n_count, 1)
		llrb.n_inserts++
		return
	}
	llrb.n_updates++
	llrb.keymemory -= int64(len(oldnd.getkey()))
	if nv := oldnd.nodevalue(); nv != nil {
		llrb.valmemory -= int64(len(nv.value()))
	}
}

func (llrb *LLRB) delcounts(nd *Llrbnode) {
	if nd != nil {
		llrb.keymemory -= int64(len(nd.getkey()))
		if nv := nd.nodevalue(); nv != nil {
			llrb.valmemory -= int64(len(nv.value()))
		}
		atomic.AddInt64(&llrb.n_count, -1)
		llrb.n_deletes++
	}
}

func (llrb *LLRB) lock() bool {
	select {
	case <-llrb.finch:
		return false
	default:
	}
	llrb.rw.Lock()
	return true
}

func (llrb *LLRB) unlock() {
	llrb.rw.Unlock()
}

func (llrb *LLRB) rlock() bool {
	select {
	case <-llrb.finch:
		return false
	default:
	}
	llrb.rw.RLock()
	return true
}

func (llrb *LLRB) runlock() {
	llrb.rw.RUnlock()
}

//---- Exported Write methods

// Setseqno can be called immediately after creating the LLRB instance.
// All futher mutating APIs will start counting seqno from this value.
func (llrb *LLRB) Setseqno(seqno uint64) {
	llrb.seqno = seqno
}

// Getseqno return current seqno on this tree.
func (llrb *LLRB) Getseqno() uint64 {
	return llrb.seqno
}

// Set a key, value pair in the index, if key is already present,
// its value will be over-written. Make sure key is not nil.
// Return old value if oldvalue points to valid buffer.
func (llrb *LLRB) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	if !llrb.lock() {
		return
	}

	llrb.seqno++

	root, newnd, oldnd := llrb.upsert(llrb.getroot(), 1 /*depth*/, key, value)
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(llrb.seqno)
	seqno := llrb.seqno

	llrb.setroot(root)
	llrb.upsertcounts(key, value, oldnd)

	if oldvalue != nil {
		var val []byte
		if oldnd != nil && oldnd.isdeleted() == false {
			val = oldnd.Value()
		}
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
		copy(oldvalue, val)
	}

	llrb.freenode(oldnd)

	llrb.unlock()
	return oldvalue, seqno
}

// returns root, newnd, oldnd
func (llrb *LLRB) upsert(
	nd *Llrbnode, depth int64,
	key, value []byte) (root, oldnd, newnd *Llrbnode) {

	var dirty bool

	if nd == nil {
		newnd := llrb.newnode(key, value)
		llrb.h_upsertdepth.Add(depth)
		return newnd, newnd, nil
	}

	nd = llrb.walkdownrot23(nd)

	if nd.gtkey(key, false) {
		nd.left, newnd, oldnd = llrb.upsert(nd.left, depth+1, key, value)
	} else if nd.ltkey(key, false) {
		nd.right, newnd, oldnd = llrb.upsert(nd.right, depth+1, key, value)
	} else {
		oldnd, dirty = llrb.clonenode(nd), false
		if nv := nd.nodevalue(); nv != nil { // free the value if present
			llrb.valarena.Free(unsafe.Pointer(nv))
			nd, dirty = nd.setnodevalue(nil), true
		}
		if len(value) > 0 { // add new value if req.
			ptr := llrb.valarena.Alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nd, dirty = nd.setnodevalue(nv.setvalue(value)), true
		}
		newnd = nd
		if dirty {
			nd.setdirty()
		}
		llrb.h_upsertdepth.Add(depth)
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd
}

// SetCAS a key, value pair in the index, if CAS is ZERO then key
// should not be present in the index, otherwise existing CAS should
// match the supplied CAS. Value will be over-written. Make sure
// key is not nil. Return old value if oldvalue points to valid buffer.
func (llrb *LLRB) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	if !llrb.lock() {
		return nil, 0, fmt.Errorf("closed")
	}
	oldvalue, cas, err := llrb.setcas(key, value, oldvalue, cas)
	llrb.unlock()

	return oldvalue, cas, err
}

func (llrb *LLRB) setcas(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	// CAS matches, go ahead with upsert.
	root, depth := llrb.getroot(), int64(1)
	root, newnd, oldnd, err := llrb.upsertcas(root, depth, key, value, cas)
	if err != nil {
		if oldvalue != nil {
			oldvalue = lib.Fixbuffer(oldvalue, 0)
		}
		return oldvalue, 0, err
	}
	llrb.seqno++
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(llrb.seqno)
	seqno := llrb.seqno

	llrb.setroot(root)
	llrb.upsertcounts(key, value, oldnd)

	if oldvalue != nil {
		var val []byte
		if oldnd != nil && oldnd.isdeleted() == false {
			val = oldnd.Value()
		}
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
		copy(oldvalue, val)
	}

	llrb.freenode(oldnd)

	return oldvalue, seqno, nil
}

func (llrb *LLRB) upsertcas(
	nd *Llrbnode, depth int64,
	key, value []byte,
	cas uint64) (root, oldnd, newnd *Llrbnode, err error) {

	var dirty bool

	if nd == nil && cas > 0 { // Expected an update
		return nil, nil, nil, api.ErrorInvalidCAS

	} else if nd == nil { // Expected a create
		newnd := llrb.newnode(key, value)
		llrb.h_upsertdepth.Add(depth)
		return newnd, newnd, nil, nil
	}

	nd = llrb.walkdownrot23(nd)

	if nd.gtkey(key, false) {
		depth++
		nd.left, newnd, oldnd, err =
			llrb.upsertcas(nd.left, depth, key, value, cas)

	} else if nd.ltkey(key, false) {
		depth++
		nd.right, newnd, oldnd, err =
			llrb.upsertcas(nd.right, depth, key, value, cas)

	} else /*equal*/ {
		if nd.isdeleted() && (cas != 0 && cas != nd.getseqno()) {
			newnd = nd
			err = api.ErrorInvalidCAS

		} else if nd.isdeleted() == false && cas != nd.getseqno() {
			newnd = nd
			err = api.ErrorInvalidCAS

		} else {
			oldnd, dirty = llrb.clonenode(nd), false
			if nv := nd.nodevalue(); nv != nil { // free the value if present
				llrb.valarena.Free(unsafe.Pointer(nv))
				nd, dirty = nd.setnodevalue(nil), true
			}
			if len(value) > 0 { // add new value if req.
				ptr := llrb.valarena.Alloc(int64(nvaluesize + len(value)))
				nv := (*nodevalue)(ptr)
				nd, dirty = nd.setnodevalue(nv.setvalue(value)), true
			}
			newnd = nd
			if dirty {
				nd.setdirty()
			}
			llrb.h_upsertdepth.Add(depth)
		}
	}

	nd = llrb.walkuprot23(nd)
	return nd, newnd, oldnd, err
}

// Delete key from index. Key should not be nil, if is key found
// return its value. If lsm is true, then don't delete the node
// instead mark the node as deleted. Again, if lsm is true
// but key is not found in index a new entry will inserted.
func (llrb *LLRB) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	if !llrb.lock() {
		return nil, 0
	}
	oldvalue, cas := llrb.dodelete(key, oldvalue, lsm)
	llrb.unlock()

	return oldvalue, cas
}

func (llrb *LLRB) dodelete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	var val []byte
	root := llrb.getroot()
	llrb.seqno++

	if oldvalue != nil {
		oldvalue = lib.Fixbuffer(oldvalue, 0)
	}
	seqno := llrb.seqno
	if lsm {
		if nd, ok := llrb.getkey(llrb.getroot(), key); ok {
			nd.setdeleted()
			nd.setseqno(llrb.seqno)
			if oldvalue != nil {
				val = nd.Value()
				oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
				copy(oldvalue, val)
			}

		} else {
			root, newnd, oldnd := llrb.upsert(root, 1 /*depth*/, key, nil)
			root.setblack()
			newnd.setdeleted()
			newnd.cleardirty()
			newnd.setseqno(llrb.seqno)
			llrb.setroot(root)
			llrb.upsertcounts(key, nil, oldnd /*nil*/)
		}

	} else {
		root, deleted := llrb.delete(root, key)
		if root != nil {
			root.setblack()
		}
		llrb.setroot(root)
		llrb.delcounts(deleted)
		if deleted != nil && oldvalue != nil {
			val = deleted.Value()
			oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
			copy(oldvalue, val)
			llrb.freenode(deleted)
		}
	}

	return oldvalue, seqno
}

func (llrb *LLRB) delete(nd *Llrbnode, key []byte) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}

	if nd.gtkey(key, false) {
		if nd.left == nil { // key not present. Nothing to delete
			return nd, nil
		}
		if !nd.left.isred() && !nd.left.left.isred() {
			nd = llrb.moveredleft(nd)
		}
		nd.left, deleted = llrb.delete(nd.left, key)

	} else {
		if nd.left.isred() {
			nd = llrb.rotateright(nd)
		}
		// If @key equals @h.Item and no right children at @h
		if !nd.ltkey(key, false) && nd.right == nil {
			return nil, nd
		}
		if nd.right != nil && !nd.right.isred() && !nd.right.left.isred() {
			nd = llrb.moveredright(nd)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !nd.ltkey(key, false) {
			var subdeleted *Llrbnode
			nd.right, subdeleted = llrb.deletemin(nd.right)
			if subdeleted == nil {
				panic("delete(): fatal logic, call the programmer")
			}
			newnd := llrb.clonenode(subdeleted)
			newnd.left, newnd.right = nd.left, nd.right
			if nd.isdirty() {
				//newnd.setdirty()
				panic("delete(): unexpected dirty node, call the programmer")
			}
			if nd.isblack() {
				newnd.setblack()
			} else {
				newnd.setred()
			}
			sdnv := subdeleted.nodevalue()
			if sdnv != nil { // TODO: is this required ??
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

// using 2-3 trees
func (llrb *LLRB) deletemin(nd *Llrbnode) (newnd, deleted *Llrbnode) {
	if nd == nil {
		return nil, nil
	}
	if nd.left == nil {
		return nil, nd
	}
	if !nd.left.isred() && !nd.left.left.isred() {
		nd = llrb.moveredleft(nd)
	}
	nd.left, deleted = llrb.deletemin(nd.left)
	return llrb.fixup(nd), deleted
}

// BeginTxn starts a read-write transaction. All transactions should either
// be commited or aborted. Structure will be locked, no other read or write
// operation can be performed, until transaction is committed or aborted.
func (llrb *LLRB) BeginTxn(id uint64) *Txn {
	if !llrb.lock() {
		return nil
	}
	atomic.AddInt64(&llrb.activetxns, 1)
	atomic.AddInt64(&llrb.n_txns, 1)
	txn := llrb.gettxn(id, llrb /*db*/, llrb /*snap*/)
	return txn
}

// rollback will never happen B-)
func (llrb *LLRB) commit(txn *Txn) error {
	for _, head := range txn.writes {
		prevkey := []byte(nil)
		for head != nil {
			if prevkey == nil || bytes.Compare(head.key, prevkey) != 0 {
				llrb.commitrecord(head)
			}
			prevkey, head = head.key, head.next
		}
	}

	llrb.puttxn(txn)
	llrb.n_commits++
	atomic.AddInt64(&llrb.activetxns, -1)
	llrb.unlock()
	return nil
}

func (llrb *LLRB) commitrecord(rec *record) (err error) {
	switch rec.cmd {
	case cmdSet:
		_, _, err = llrb.setcas(rec.key, rec.value, nil, rec.seqno)
	case cmdDelete:
		llrb.dodelete(rec.key, nil, rec.lsm)
	}
	return
}

func (llrb *LLRB) aborttxn(txn *Txn) error {
	llrb.puttxn(txn)
	llrb.n_aborts++
	atomic.AddInt64(&llrb.activetxns, -1)
	llrb.unlock()
	return nil
}

// View start a read only transaction. Structure will be read-locked,
// no other write operations can be performed, until transaction is
// committed or aborted. Concurrent reads are still allowed.
func (llrb *LLRB) View(id uint64) *View {
	if !llrb.rlock() {
		return nil
	}
	atomic.AddInt64(&llrb.activetxns, 1)
	atomic.AddInt64(&llrb.n_txns, 1)
	view := llrb.getview(id, llrb /*db*/, llrb /*snap*/)
	return view
}

func (llrb *LLRB) abortview(view *View) error {
	llrb.putview(view)
	atomic.AddInt64(&llrb.activetxns, -1)
	llrb.runlock()
	return nil
}

//---- Exported Read methods

// Get value for key, if value argument points to valid buffer it will,
// be used to copy the entry's value. Also returns entry's cas, whether
// entry is marked as deleted by LSM. If ok is false, then key is not found.
func (llrb *LLRB) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	if !llrb.rlock() {
		return
	}
	value, cas, deleted, ok = llrb.get(key, value)
	llrb.runlock()
	return value, cas, deleted, ok
}

func (llrb *LLRB) get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	deleted, seqno := false, uint64(0)
	nd, ok := llrb.getkey(llrb.getroot(), key)
	if ok {
		if value != nil {
			val := nd.Value()
			value = lib.Fixbuffer(value, int64(len(val)))
			copy(value, val)
		}
		seqno, deleted = nd.getseqno(), nd.isdeleted()
	} else if value != nil {
		value = lib.Fixbuffer(value, 0)
	}
	return value, seqno, deleted, ok
}

func (llrb *LLRB) getkey(nd *Llrbnode, k []byte) (*Llrbnode, bool) {
	for nd != nil {
		if nd.gtkey(k, false) {
			nd = nd.left
		} else if nd.ltkey(k, false) {
			nd = nd.right
		} else {
			return nd, true
		}
	}
	//fmt.Printf("get %q %v\n", k, false)
	return nil, false
}

// Scan return a full table iterator.
func (llrb *LLRB) Scan() api.Iterator {
	currkey := make([]byte, 1024)
	sb := makescanbuf()

	leseqno := llrb.startscan(nil, sb, 0)
	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		key, value, seqno, deleted := sb.pop()
		if key == nil {
			llrb.startscan(currkey, sb, leseqno)
			key, value, seqno, deleted = sb.pop()
		}
		if cap(currkey) < len(key) {
			currkey = make([]byte, len(key))
		}
		currkey = currkey[:len(key)]
		copy(currkey, key)
		if key == nil {
			return key, value, seqno, deleted, io.EOF
		}
		return key, value, seqno, deleted, nil
	}
}

func (llrb *LLRB) startscan(key []byte, sb *scanbuf, leseqno uint64) uint64 {
	if !llrb.rlock() {
		return leseqno
	}
	if key == nil {
		leseqno = llrb.seqno
	}
	sb.startwrite()
	llrb.scan(llrb.getroot(), key, sb, leseqno)
	sb.startread()
	llrb.runlock()
	return leseqno
}

func (llrb *LLRB) scan(
	nd *Llrbnode, key []byte, sb *scanbuf, leseqno uint64) bool {

	if nd == nil {
		return true
	}
	if key != nil && nd.lekey(key, false) {
		return llrb.scan(nd.right, key, sb, leseqno)
	}
	if !llrb.scan(nd.left, key, sb, leseqno) {
		return false
	}
	seqno := nd.getseqno()
	if seqno <= leseqno {
		n := sb.append(nd.getkey(), nd.Value(), seqno, nd.isdeleted())
		if n >= scanlimit {
			return false
		}
	}
	return llrb.scan(nd.right, key, sb, leseqno)
}

//---- Exported Control methods

// ID is same as the name supplied while creating the LLRB instance.
func (llrb *LLRB) ID() string {
	return llrb.name
}

// Count return the number of items indexed.
func (llrb *LLRB) Count() int64 {
	return atomic.LoadInt64(&llrb.n_count)
}

// Dotdump to convert whole tree into dot script that can be
// visualized using graphviz. Until dotdump exits concurrent write
// operations will block.
func (llrb *LLRB) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	if !llrb.rlock() {
		return
	}
	llrb.getroot().dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
	llrb.runlock()
}

// Stats return a map of data-structure statistics and operational
// statistics.
func (llrb *LLRB) Stats() map[string]interface{} {
	if !llrb.rlock() {
		return nil
	}
	stats := llrb.stats()
	llrb.runlock()
	return stats
}

func (llrb *LLRB) stats() map[string]interface{} {
	m := make(map[string]interface{})
	m["n_count"] = atomic.LoadInt64(&llrb.n_count)
	m["n_inserts"] = llrb.n_inserts
	m["n_updates"] = llrb.n_updates
	m["n_deletes"] = llrb.n_deletes
	m["n_nodes"] = llrb.n_nodes
	m["n_frees"] = llrb.n_frees
	m["n_clones"] = llrb.n_clones
	m["n_txns"] = llrb.n_txns
	m["n_commits"] = llrb.n_commits
	m["n_aborts"] = llrb.n_aborts
	m["keymemory"] = llrb.keymemory
	m["valmemory"] = llrb.valmemory

	capacity, heap, alloc, overhead := llrb.nodearena.Info()
	m["keymemory"] = llrb.keymemory
	m["node.capacity"] = capacity
	m["node.heap"] = heap
	m["node.alloc"] = alloc
	m["node.overhead"] = overhead
	m["node.blocks"] = llrb.nodearena.Slabs()

	capacity, heap, alloc, overhead = llrb.valarena.Info()
	m["value.capacity"] = capacity
	m["value.heap"] = heap
	m["value.alloc"] = alloc
	m["value.overhead"] = overhead
	m["value.blocks"] = llrb.valarena.Slabs()

	m["h_upsertdepth"] = llrb.h_upsertdepth.Fullstats()
	return m
}

// Validate data structure. This is a costly operation, walks
// through the entire tree and holds a read lock while doing so.
func (llrb *LLRB) Validate() {
	if !llrb.rlock() {
		return
	}
	defer llrb.runlock()

	stats := llrb.Stats()

	n := stats["n_count"].(int64)
	kmem, vmem := stats["keymemory"].(int64), stats["valmemory"].(int64)

	validatetree(llrb.getroot(), llrb.logprefix, n, kmem, vmem)
	llrb.validatestats(stats)
}

func (llrb *LLRB) validatestats(stats map[string]interface{}) {
	// n_count should match (n_inserts - n_deletes)
	n_count := stats["n_count"].(int64)
	n_inserts := stats["n_inserts"].(int64)
	n_deletes := stats["n_deletes"].(int64)
	if n_count != (n_inserts - n_deletes) {
		fmsg := "validatestats(): n_count:%v != (n_inserts:%v - n_deletes:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_inserts, n_deletes))
	}
	// n_nodes should match n_inserts
	n_clones := stats["n_clones"].(int64)
	n_nodes, n_frees := stats["n_nodes"].(int64), stats["n_frees"].(int64)
	if n_inserts != n_nodes {
		fmsg := "validatestats(): n_inserts:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_inserts, n_nodes))
	}
	if (n_nodes - n_count) == n_frees {
	} else if n_clones+(n_nodes-n_count) == n_frees {
	} else {
		fmsg := "validatestats(): clones:%v+(nodes:%v-count:%v) != frees:%v"
		panic(fmt.Errorf(fmsg, n_clones, n_nodes, n_count, n_frees))
	}
}

// Log vital information.
func (llrb *LLRB) Log() {
	if !llrb.rlock() {
		return
	}

	stats := llrb.Stats()

	summary := func(args ...string) string {
		ss := []interface{}{}
		for _, arg := range args {
			ss = append(ss, humanize.Bytes(uint64(stats[arg].(int64))))
		}
		fmsg := "cap: %v {heap:%v,alloc:%v,overhd,%v}\n"
		return fmt.Sprintf(fmsg, ss...)
	}
	loguz := func(sizes []int, zs []float64, arena string) string {
		outs := []string{}
		fmsg := "  %4v chunk-size, utilz: %2.2f%%"
		for i, size := range sizes {
			outs = append(outs, fmt.Sprintf(fmsg, size, zs[i]))
		}
		out := strings.Join(outs, "\n")
		allock, heapk := "node.alloc", "node.heap"
		if arena == "value" {
			allock, heapk = "value.alloc", "value.heap"
		}
		alloc, heap := stats[allock], stats[heapk]
		uz := ((float64(alloc.(int64)) / float64(heap.(int64))) * 100)
		return fmt.Sprintf("utilization: %.2f%%\n%v", uz, out)
	}

	// log information about key memory arena
	kmem := humanize.Bytes(uint64(stats["keymemory"].(int64)))
	as := []string{"node.capacity", "node.heap", "node.alloc", "node.overhead"}
	log.Infof("%v keymem(%v): %v\n", llrb.logprefix, kmem, summary(as...))
	// log information about key memory utilization
	sizes, zs := llrb.nodearena.Utilization()
	log.Infof("%v key %v", llrb.logprefix, loguz(sizes, zs, "node"))
	// log information about value memory arena
	vmem := humanize.Bytes(uint64(stats["valmemory"].(int64)))
	as = []string{
		"value.capacity", "value.heap", "value.alloc", "value.overhead",
	}
	log.Infof("%v valmem(%v): %v\n", llrb.logprefix, vmem, summary(as...))
	// log information about key memory utilization
	sizes, zs = llrb.valarena.Utilization()
	log.Infof("%v val %v", llrb.logprefix, loguz(sizes, zs, "node"))

	llrb.runlock()
}

// Clone llrb instance and return the clone.
func (llrb *LLRB) Clone(name string) *LLRB {
	if !llrb.lock() {
		return nil
	}

	newllrb := NewLLRB(llrb.name, llrb.setts)
	newllrb.llrbstats = llrb.llrbstats
	newllrb.h_upsertdepth = llrb.h_upsertdepth.Clone()
	newllrb.seqno = llrb.seqno

	newllrb.setroot(newllrb.clonetree(llrb.getroot()))

	llrb.unlock()
	return newllrb
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

// Destroy releases all resources held by the tree. No other
// method call are allowed after Destroy.
func (llrb *LLRB) Destroy() {
	close(llrb.finch)
	for llrb.dodestory() == false {
		time.Sleep(100 * time.Millisecond)
	}
	log.Infof("%v destroyed\n", llrb.logprefix)
}

func (llrb *LLRB) dodestory() bool {
	if !llrb.lock() {
		return true
	}
	defer llrb.unlock()

	if atomic.LoadInt64(&llrb.activetxns) > 0 {
		return false
	}
	llrb.nodearena.Release()
	llrb.valarena.Release()
	llrb.setroot(nil)
	llrb.setts = nil
	return true
}

// rotation routines for 2-3 algorithm

func (llrb *LLRB) walkdownrot23(nd *Llrbnode) *Llrbnode {
	return nd
}

func (llrb *LLRB) walkuprot23(nd *Llrbnode) *Llrbnode {
	if nd.right.isred() && !nd.left.isred() {
		nd = llrb.rotateleft(nd)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd = llrb.rotateright(nd)
	}
	if nd.left.isred() && nd.right.isred() {
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) rotateleft(nd *Llrbnode) *Llrbnode {
	y := nd.right
	if y.isblack() {
		panic("rotateleft(): rotating a black link ? call the programmer")
	}
	nd.right = y.left
	y.left = nd
	if nd.isblack() {
		y.setblack()
	} else {
		y.setred()
	}
	nd.setred()
	return y
}

func (llrb *LLRB) rotateright(nd *Llrbnode) *Llrbnode {
	x := nd.left
	if x.isblack() {
		panic("rotateright(): rotating a black link ? call the programmer")
	}
	nd.left = x.right
	x.right = nd
	if nd.isblack() {
		x.setblack()
	} else {
		x.setred()
	}
	nd.setred()
	return x
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB) flip(nd *Llrbnode) {
	nd.left.togglelink()
	nd.right.togglelink()
	nd.togglelink()
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB) moveredleft(nd *Llrbnode) *Llrbnode {
	llrb.flip(nd)
	if nd.right.left.isred() {
		nd.right = llrb.rotateright(nd.right)
		nd = llrb.rotateleft(nd)
		llrb.flip(nd)
	}
	return nd
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB) moveredright(nd *Llrbnode) *Llrbnode {
	llrb.flip(nd)
	if nd.left.left.isred() {
		nd = llrb.rotateright(nd)
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) fixup(nd *Llrbnode) *Llrbnode {
	if nd.right.isred() {
		nd = llrb.rotateleft(nd)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd = llrb.rotateright(nd)
	}
	if nd.left.isred() && nd.right.isred() {
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB) logarenasettings() {
	stats := llrb.stats()

	// key arena
	kblocks := len(stats["node.blocks"].([]int64))
	cp := humanize.Bytes(uint64(stats["node.capacity"].(int64)))
	fmsg := "%v key arena %v blocks with capacity %v\n"
	log.Infof(fmsg, llrb.logprefix, kblocks, cp)

	// value arena
	vblocks := len(stats["value.blocks"].([]int64))
	cp = humanize.Bytes(uint64(stats["value.capacity"].(int64)))
	fmsg = "%v val arena %v blocks with capacity %v\n"
	log.Infof(fmsg, llrb.logprefix, vblocks, cp)
}
