package llrb

import "fmt"
import "unsafe"
import "io"
import "strings"
import "bytes"
import "sync"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/malloc"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import humanize "github.com/dustin/go-humanize"

type llrbstats1 struct {
	n_count   int64 // number of nodes in the tree
	n_inserts int64
	n_updates int64
	n_deletes int64
	n_txns    int64
	n_cursors int64
	n_nodes   int64 // TODO: is this field required ?
	n_frees   int64 // TODO: is this field required ?
	n_clones  int64 // TODO: is this field required ?
	n_reads   int64
	keymemory int64 // memory used by all keys
	valmemory int64 // memory used by all values
}

// LLRB1 manage a single instance of in-memory sorted index using
// left-leaning-red-black tree.
type LLRB1 struct { // tree container
	llrbstats1    // 64-bit aligned snapshot statistics.
	h_upsertdepth *lib.HistogramInt64
	// can be unaligned fields
	name      string
	nodearena api.Mallocer
	valarena  api.Mallocer
	root      unsafe.Pointer // *Llrbnode1
	seqno     uint64
	rw        sync.RWMutex
	// settings
	keycapacity int64
	valcapacity int64
	setts       s.Settings
	logprefix   string
}

// NewLLRB1 a new instance of in-memory sorted index.
func NewLLRB1(name string, setts s.Settings) *LLRB1 {
	llrb := &LLRB1{name: name}
	llrb.logprefix = fmt.Sprintf("LLRB1 [%s]", name)

	setts = make(s.Settings).Mixin(Defaultsettings(), setts)
	llrb.readsettings(setts)
	llrb.setts = setts

	// setup arena for nodes and node-values.
	memsetts := setts.Section("nodearena").Trim("nodearena.")
	llrb.nodearena = malloc.NewArena(llrb.keycapacity, memsetts)
	memsetts = setts.Section("valarena").Trim("valarena.")
	llrb.valarena = malloc.NewArena(llrb.valcapacity, memsetts)

	// statistics
	llrb.h_upsertdepth = lib.NewhistorgramInt64(10, 100, 10)

	log.Infof("%v started ...\n", llrb.logprefix)
	llrb.logarenasettings()
	return llrb
}

//---- local accessor methods.

func (llrb *LLRB1) readsettings(setts s.Settings) *LLRB1 {
	llrb.keycapacity = setts.Int64("keycapacity")
	llrb.valcapacity = setts.Int64("valcapacity")
	return llrb
}

func (llrb *LLRB1) getroot() *Llrbnode1 {
	return (*Llrbnode1)(atomic.LoadPointer(&llrb.root))
}

func (llrb *LLRB1) setroot(root *Llrbnode1) {
	atomic.StorePointer(&llrb.root, unsafe.Pointer(root))
}

func (llrb *LLRB1) newnode(k, v []byte) *Llrbnode1 {
	ptr := llrb.nodearena.Alloc(int64(nodesize1 + len(k)))
	nd := (*Llrbnode1)(ptr)
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

func (llrb *LLRB1) freenode(nd *Llrbnode1) {
	if nd != nil {
		if nv := nd.nodevalue(); nv != nil {
			llrb.valarena.Free(unsafe.Pointer(nv))
		}
		llrb.nodearena.Free(unsafe.Pointer(nd))
		llrb.n_frees++
	}
}

func (llrb *LLRB1) clonenode(nd *Llrbnode1) (newnd *Llrbnode1) {
	slabsize := llrb.nodearena.Slabsize(unsafe.Pointer(nd))
	newptr := llrb.nodearena.Allocslab(slabsize)
	size := llrb.nodearena.Chunklen(unsafe.Pointer(nd))
	newnd = (*Llrbnode1)(newptr)
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

func (llrb *LLRB1) upsertcounts(key, value []byte, oldnd *Llrbnode1) {
	llrb.keymemory += int64(len(key))
	llrb.valmemory += int64(len(value))
	if oldnd == nil {
		llrb.n_count++
		llrb.n_inserts++
		return
	}
	llrb.n_updates++
	llrb.keymemory -= int64(len(oldnd.getkey()))
	if nv := oldnd.nodevalue(); nv != nil {
		llrb.valmemory -= int64(len(nv.value()))
	}
}

func (llrb *LLRB1) delcounts(nd *Llrbnode1) {
	if nd != nil {
		llrb.keymemory -= int64(len(nd.getkey()))
		if nv := nd.nodevalue(); nv != nil {
			llrb.valmemory -= int64(len(nv.value()))
		}
		llrb.n_count--
		llrb.n_deletes++
	}
}

//---- Exported Write methods

// Set a key, value pair in the index, if key is already present,
// its value will be over-written. Make sure that key is not nil.
// Return old value.
func (llrb *LLRB1) Set(key, value, oldvalue []byte) []byte {
	llrb.rw.Lock()
	llrb.seqno++

	root, newnd, oldnd := llrb.upsert(llrb.getroot(), 1 /*depth*/, key, value)
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(llrb.seqno)

	llrb.setroot(root)
	llrb.upsertcounts(key, value, oldnd)

	var val []byte
	if oldnd != nil {
		val = oldnd.Value()
	}
	oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
	copy(oldvalue, val)

	llrb.freenode(oldnd)

	llrb.rw.Unlock()
	return oldvalue
}

// SetCAS a key, value pair in the index, if CAS is ZERO then key
// should be present in the index, otherwise existing CAS should
// match the supplied CAS. Value will be over-written. Make sure that
// key is not nil. Return old value.
func (llrb *LLRB1) SetCAS(key, value, oldv []byte, cas uint64) ([]byte, error) {
	llrb.rw.Lock()

	// Get to check for CAS
	currcas := uint64(0)
	nd, ok := llrb.getkey(key)
	if ok {
		currcas = nd.getseqno()
	}
	if currcas != cas {
		llrb.rw.Unlock()
		return oldv, api.ErrorInvalidCAS
	}

	llrb.seqno++

	// CAS matches, go ahead with upsert.
	root, newnd, oldnd := llrb.upsert(llrb.getroot(), 1 /*depth*/, key, value)
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(llrb.seqno)

	llrb.setroot(root)
	llrb.upsertcounts(key, value, oldnd)

	val := oldnd.Value()
	oldv = lib.Fixbuffer(oldv, int64(len(val)))
	copy(oldv, val)

	llrb.freenode(oldnd)

	llrb.rw.Unlock()
	return oldv, nil
}

// returns root, newnd, oldnd
func (llrb *LLRB1) upsert(
	nd *Llrbnode1, depth int64,
	key, value []byte) (*Llrbnode1, *Llrbnode1, *Llrbnode1) {

	var oldnd, newnd *Llrbnode1
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

// Delete key from index. Key should not be nil and if key found,
// return its value. If lsm is true, then don't delete the node
// instead simply mark the node as deleted. Again, if lsm is true
// but key is not found in index a new entry will inserted.
func (llrb *LLRB1) Delete(key, oldval []byte, lsm bool) []byte {
	llrb.rw.Lock()

	var val []byte
	root := llrb.getroot()
	llrb.seqno++

	if lsm {
		llrbnd, ok := llrb.getkey(key)
		if ok {
			llrbnd.setdeleted()
			val = llrbnd.Value()

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
		if deleted != nil {
			llrb.freenode(deleted)
			val = deleted.Value()
		}
	}

	oldval = lib.Fixbuffer(oldval, int64(len(val)))
	copy(oldval, val)

	llrb.rw.Unlock()
	return oldval
}

func (llrb *LLRB1) delete(nd *Llrbnode1, key []byte) (newnd, deleted *Llrbnode1) {
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
			var subdeleted *Llrbnode1
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
			if sdnv := subdeleted.nodevalue(); sdnv != nil { // TODO: is this required ??
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
func (llrb *LLRB1) deletemin(nd *Llrbnode1) (newnd, deleted *Llrbnode1) {
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

//---- Exported Read methods

func (llrb *LLRB1) Get(key []byte, value []byte) ([]byte, bool) {
	llrb.rw.RLock()

	nd, ok := llrb.getkey(key)
	if ok {
		val := nd.Value()
		value = lib.Fixbuffer(value, int64(len(val)))
		copy(value, val)
	}
	llrb.n_reads++

	llrb.rw.RUnlock()
	return value, ok
}

func (llrb *LLRB1) getkey(k []byte) (*Llrbnode1, bool) {
	nd := llrb.getroot()
	for nd != nil {
		if nd.gtkey(k, false) {
			nd = nd.left
		} else if nd.ltkey(k, false) {
			nd = nd.right
		} else {
			return nd, true
		}
	}
	return nil, false
}

// Range implement api.IndexReader interface. Acquires a read
// lock, can be called only when MVCC is disabled.
//func (llrb *LLRB1) Range(
//	lkey, hkey []byte, incl string, reverse bool, callb api.NodeCallb) {
//
//	llrb.assertnomvcc()
//
//	var skip bool
//	lkey, hkey, incl, skip = fixrangeargs(lkey, hkey, incl)
//	if skip {
//		return
//	}
//
//	llrb.rw.RLock()
//	dorange(llrb, llrb.getroot(), lkey, hkey, incl, reverse, callb)
//	llrb.rw.RUnlock()
//
//	atomic.AddInt64(&llrb.n_ranges, 1)
//}

// Iterate implement api.IndexReader interface. Acquires a read
// lock, can be called only when MVCC is disabled.
//func (llrb *LLRB1) Iterate(lk, hk []byte, incl string, r bool) api.IndexIterator {
//	llrb.assertnomvcc()
//
//	var skip bool
//	lk, hk, incl, skip = fixrangeargs(lk, hk, incl)
//	if skip {
//		return nil
//	}
//
//	llrb.rw.RLock()
//	iter := inititerator(llrb, llrb, lk, hk, incl, r)
//
//	atomic.AddInt64(&llrb.n_ranges, 1)
//	atomic.AddInt64(&llrb.n_activeiter, 1)
//	return iter
//}

//---- Exported Control methods

func (llrb *LLRB1) ID() string {
	return llrb.name
}

func (llrb *LLRB1) Count() int64 {
	return atomic.LoadInt64(&llrb.n_count)
}

// Dotdump to convert whole tree into dot script that can be
// visualized using graphviz.
func (llrb *LLRB1) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	llrb.getroot().dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

// Stats return a map of data-structure statistics and operational
// statistics.
func (llrb *LLRB1) Stats() map[string]interface{} {
	llrb.rw.RLock()

	m := make(map[string]interface{})
	m["n_count"] = llrb.n_count
	m["n_inserts"] = llrb.n_inserts
	m["n_updates"] = llrb.n_updates
	m["n_deletes"] = llrb.n_deletes
	m["n_txns"] = llrb.n_txns
	m["n_cursors"] = llrb.n_cursors
	m["n_nodes"] = llrb.n_nodes
	m["n_frees"] = llrb.n_frees
	m["n_clones"] = llrb.n_clones
	m["n_reads"] = llrb.n_reads
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

	llrb.rw.RUnlock()
	return m
}

// Validate data structure. This is a costly operation, walks
// through the entire tree and holds a read lock while doing so.
func (llrb *LLRB1) Validate() {
	llrb.rw.RLock()

	root := llrb.getroot()
	if root != nil {
		h := lib.NewhistorgramInt64(1, 256, 1)
		blacks, depth, fromred := int64(0), int64(1), root.isred()
		nblacks, km, vm := llrb.validatetree(root, fromred, blacks, depth, h)
		if km != llrb.keymemory {
			fmsg := "validate(): keymemory:%v != actual:%v"
			panic(fmt.Errorf(fmsg, llrb.keymemory, km))
		} else if vm != llrb.valmemory {
			fmsg := "validate(): valmemory:%v != actual:%v"
			panic(fmt.Errorf(fmsg, llrb.valmemory, vm))
		}
		if samples := h.Samples(); samples != llrb.Count() {
			fmsg := "expected h_height.samples:%v to be same as Count():%v"
			panic(fmt.Errorf(fmsg, samples, llrb.Count()))
		}
		log.Infof("%v found %v blacks on both sides\n", llrb.logprefix, nblacks)
		// `h_height`.max should not exceed certain limit, maxheight
		// gives some breathing room.
		if h.Samples() > 8 {
			entries := llrb.Count()
			if float64(h.Max()) > maxheight(entries) {
				fmsg := "validate(): max height %v exceeds <factor>*log2(%v)"
				panic(fmt.Errorf(fmsg, float64(h.Max()), entries))
			}
		}

	}

	// Validation check based on statistics accounting.

	// n_count should match (n_inserts - n_deletes)
	n_count := llrb.n_count
	n_inserts, n_deletes := llrb.n_inserts, llrb.n_deletes
	if n_count != (n_inserts - n_deletes) {
		fmsg := "validatestats(): n_count:%v != (n_inserts:%v - n_deletes:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_inserts, n_deletes))
	}
	// n_nodes should match n_inserts
	n_nodes := llrb.n_nodes
	if n_inserts != n_nodes {
		fmsg := "validatestats(): n_inserts:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_inserts, n_nodes))
	}

	llrb.rw.RUnlock()
}

// Log vital information.
func (llrb *LLRB1) Log() {
	llrb.rw.RLock()

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
	as = []string{"value.capacity", "value.heap", "value.alloc", "value.overhead"}
	log.Infof("%v valmem(%v): %v\n", llrb.logprefix, vmem, summary(as...))
	// log information about key memory utilization
	sizes, zs = llrb.valarena.Utilization()
	log.Infof("%v val %v", llrb.logprefix, loguz(sizes, zs, "node"))

	llrb.rw.RUnlock()
}

// Clone llrb instance and return the cloned instance.
func (llrb *LLRB1) Clone(name string) *LLRB1 {
	llrb.rw.Lock()

	newllrb := NewLLRB1(llrb.name, llrb.setts)
	newllrb.llrbstats1 = llrb.llrbstats1
	newllrb.h_upsertdepth = llrb.h_upsertdepth.Clone()
	newllrb.seqno = llrb.seqno

	newllrb.setroot(newllrb.clonetree(llrb.getroot()))

	llrb.rw.Unlock()
	return newllrb
}

func (llrb *LLRB1) clonetree(nd *Llrbnode1) *Llrbnode1 {
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
func (llrb *LLRB1) Destroy() {
	llrb.nodearena.Release()
	llrb.valarena.Release()
	llrb.setroot(nil)
	llrb.setts = nil
	log.Infof("%v destroyed\n", llrb.logprefix)
}

// rotation routines for 2-3 algorithm

func (llrb *LLRB1) walkdownrot23(nd *Llrbnode1) *Llrbnode1 {
	return nd
}

func (llrb *LLRB1) walkuprot23(nd *Llrbnode1) *Llrbnode1 {
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

func (llrb *LLRB1) rotateleft(nd *Llrbnode1) *Llrbnode1 {
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

func (llrb *LLRB1) rotateright(nd *Llrbnode1) *Llrbnode1 {
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
func (llrb *LLRB1) flip(nd *Llrbnode1) {
	nd.left.togglelink()
	nd.right.togglelink()
	nd.togglelink()
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB1) moveredleft(nd *Llrbnode1) *Llrbnode1 {
	llrb.flip(nd)
	if nd.right.left.isred() {
		nd.right = llrb.rotateright(nd.right)
		nd = llrb.rotateleft(nd)
		llrb.flip(nd)
	}
	return nd
}

// REQUIRE: Left and Right children must be present
func (llrb *LLRB1) moveredright(nd *Llrbnode1) *Llrbnode1 {
	llrb.flip(nd)
	if nd.left.left.isred() {
		nd = llrb.rotateright(nd)
		llrb.flip(nd)
	}
	return nd
}

func (llrb *LLRB1) fixup(nd *Llrbnode1) *Llrbnode1 {
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

// using 2-3 trees
func (llrb *LLRB1) deletemax(nd *Llrbnode1) (newnd, deleted *Llrbnode1) {
	if nd == nil {
		return nil, nil
	}
	if nd.left.isred() {
		nd = llrb.rotateright(nd)
	}
	if nd.right == nil {
		return nil, nd
	}
	if !nd.right.isred() && !nd.right.left.isred() {
		nd = llrb.moveredright(nd)
	}
	nd.right, deleted = llrb.deletemax(nd.right)
	return llrb.fixup(nd), deleted
}

func (llrb *LLRB1) logarenasettings() {
	stats := llrb.Stats()

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

/*
following expectations on the tree should be met.
* If current node is red, parent node should be black.
* At each level, number of black-links on the left subtree should be
  equal to number of black-links on the right subtree.
* Make sure that the tree is in sort order.
* Return number of blacks, cummulative memory consumed by keys,
  cummulative memory consumed by values.
*/
func (llrb *LLRB1) validatetree(
	nd *Llrbnode1, fromred bool, blacks, depth int64,
	h *lib.HistogramInt64) (nblacks, keymem, valmem int64) {

	if nd != nil {
		h.Add(depth)
		if fromred && nd.isred() {
			panic(redafterred)
		}
		if !nd.isred() {
			blacks++
		}

		lblacks, lkm, lvm := llrb.validatetree(
			nd.left, nd.isred(), blacks, depth+1, h)
		rblacks, rkm, rvm := llrb.validatetree(
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
	return blacks, 0, 0
}
