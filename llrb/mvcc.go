package llrb

import "io"
import "fmt"
import "sync"
import "time"
import "bytes"
import "unsafe"
import "strings"
import "math"
import "sync/atomic"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/malloc"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import humanize "github.com/dustin/go-humanize"

// MVCC manages a single instance of LLRB tree in MVCC mode.
type MVCC struct {
	llrbstats    // 64-bit aligned snapshot statistics.
	n_routines   int64
	n_maxverions int64
	// can be unaligned fields
	name      string
	nodearena api.Mallocer
	valarena  api.Mallocer
	seqno     uint64
	rw        sync.RWMutex
	rwhbf     sync.RWMutex
	finch     chan struct{}
	txnsmeta

	// mvcc fields
	snapshot   unsafe.Pointer // *mvccsnapshot
	h_bulkfree *lib.HistogramInt64
	h_reclaims *lib.HistogramInt64
	// cache
	snapcache chan *mvccsnapshot

	// settings
	memcapacity int64
	snaptick    time.Duration // mvcc settings
	allocator   string
	setts       s.Settings
	logprefix   string
}

// NewMVCC a new instance of in-memory sorted index.
func NewMVCC(name string, setts s.Settings) *MVCC {
	mvcc := &MVCC{
		name:      name,
		finch:     make(chan struct{}),
		logprefix: fmt.Sprintf("MVCC [%s]", name),
		snapcache: make(chan *mvccsnapshot, 1024),
	}
	mvcc.inittxns()

	setts = make(s.Settings).Mixin(Defaultsettings(), setts)
	mvcc.readsettings(setts)
	mvcc.setts = setts

	// setup arena for nodes and node-values.
	mvcc.nodearena = malloc.NewArena(mvcc.memcapacity, mvcc.allocator)
	mvcc.valarena = malloc.NewArena(mvcc.memcapacity, mvcc.allocator)

	// statistics
	mvcc.snapshot = nil
	mvcc.h_bulkfree = lib.NewhistorgramInt64(100, 1000, 1000)
	mvcc.h_reclaims = lib.NewhistorgramInt64(10, 200, 20)

	mvcc.logarenasettings()

	mvcc.makesnapshot()
	go housekeeper(mvcc, mvcc.snaptick, mvcc.finch)

	log.Infof("%v started ...\n", mvcc.logprefix)
	return mvcc
}

//---- local accessor methods.

func (mvcc *MVCC) readsettings(setts s.Settings) *MVCC {
	mvcc.memcapacity = setts.Int64("memcapacity")
	snaptick := setts.Int64("snapshottick")
	mvcc.snaptick = time.Duration(snaptick) * time.Millisecond
	mvcc.allocator = setts.String("allocator")
	return mvcc
}

func (mvcc *MVCC) getroot() *Llrbnode {
	return mvcc.currsnapshot().getroot()
}

// should be called with the write-lock.
func (mvcc *MVCC) setroot(root *Llrbnode) {
	mvcc.currsnapshot().setroot(root)
}

func (mvcc *MVCC) currsnapshot() *mvccsnapshot {
	return (*mvccsnapshot)(atomic.LoadPointer(&mvcc.snapshot))
}

func (mvcc *MVCC) setheadsnapshot(snapshot *mvccsnapshot) {
	atomic.StorePointer(&mvcc.snapshot, unsafe.Pointer(snapshot))
}

func (mvcc *MVCC) newnode(k, v []byte) *Llrbnode {
	ptr := mvcc.nodearena.Alloc(int64(nodesize + len(k)))
	nd := (*Llrbnode)(ptr)
	nd.left, nd.right, nd.value = nil, nil, nil
	nd.seqflags, nd.hdr = 0, 0
	nd.setdirty().setred().setkey(k).setreclaim()
	if len(v) > 0 {
		ptr = mvcc.valarena.Alloc(int64(nvaluesize + len(v)))
		nv := (*nodevalue)(ptr)
		nv.hdr = 0
		nd.setnodevalue(nv.setvalue(v))
	}
	mvcc.n_nodes++
	return nd
}

func (mvcc *MVCC) freenode(nd *Llrbnode) {
	if nd != nil {
		if nv := nd.nodevalue(); nv != nil && nd.isreclaim() {
			mvcc.valarena.Free(unsafe.Pointer(nv))
		}
		mvcc.nodearena.Free(unsafe.Pointer(nd))
		atomic.AddInt64(&mvcc.n_frees, 1)
	}
}

func (mvcc *MVCC) clonenode(nd *Llrbnode, copyval bool) (newnd *Llrbnode) {
	slabsize := mvcc.nodearena.Slabsize(unsafe.Pointer(nd))
	newptr := mvcc.nodearena.Allocslab(slabsize)
	size := mvcc.nodearena.Chunklen(unsafe.Pointer(nd))
	newnd = (*Llrbnode)(newptr)
	lib.Memcpy(unsafe.Pointer(newnd), unsafe.Pointer(nd), int(size))
	newnd.setreclaim()
	// clone value if value is present.
	if nv := nd.nodevalue(); copyval && nv != nil {
		slabsize = mvcc.valarena.Slabsize(unsafe.Pointer(nv))
		newnvptr := mvcc.valarena.Allocslab(slabsize)
		size := mvcc.valarena.Chunklen(unsafe.Pointer(nv))
		lib.Memcpy(newnvptr, unsafe.Pointer(nv), int(size))
		newnv := (*nodevalue)(newnvptr)
		newnd.setnodevalue(newnv)
	} else if copyval == false {
		nd.clearreclaim()
		newnd.setnodevalue(nv)
	}
	mvcc.n_clones++
	return
}

func (mvcc *MVCC) upsertcounts(key, value []byte, oldnd *Llrbnode) {
	mvcc.keymemory += int64(len(key))
	mvcc.valmemory += int64(len(value))
	if oldnd == nil {
		atomic.AddInt64(&mvcc.n_count, 1)
		mvcc.n_inserts++
		return
	}
	mvcc.n_updates++
	mvcc.keymemory -= int64(len(oldnd.getkey()))
	if nv := oldnd.nodevalue(); nv != nil {
		mvcc.valmemory -= int64(len(nv.value()))
	}
}

func (mvcc *MVCC) delcounts(nd *Llrbnode, lsm bool) {
	if lsm == false {
		if nd != nil {
			mvcc.keymemory -= int64(len(nd.getkey()))
			if nv := nd.nodevalue(); nv != nil {
				mvcc.valmemory -= int64(len(nv.value()))
			}
			atomic.AddInt64(&mvcc.n_count, -1)
			mvcc.n_deletes++
		}
	}
}

func (mvcc *MVCC) appendreclaim(reclaim []*Llrbnode) {
	if ln := int64(len(reclaim)); ln > 0 {
		mvcc.h_reclaims.Add(ln)
		mvcc.n_reclaims += ln
		snapshot := mvcc.currsnapshot()
		snapshot.reclaims = append(snapshot.reclaims, reclaim...)
	}
}

func (mvcc *MVCC) logarenasettings() {
	stats := mvcc.stats()

	// key arena
	kblocks := len(stats["node.blocks"].([]int64))
	cp := humanize.Bytes(uint64(stats["node.capacity"].(int64)))
	fmsg := "%v key arena %v blocks with capacity %v\n"
	log.Infof(fmsg, mvcc.logprefix, kblocks, cp)

	// value arena
	vblocks := len(stats["value.blocks"].([]int64))
	cp = humanize.Bytes(uint64(stats["value.capacity"].(int64)))
	fmsg = "%v val arena %v blocks with capacity %v\n"
	log.Infof(fmsg, mvcc.logprefix, vblocks, cp)
}

func (mvcc *MVCC) lock() bool {
	mvcc.rw.Lock()
	return true
}

func (mvcc *MVCC) unlock() {
	mvcc.rw.Unlock()
}

func (mvcc *MVCC) rlock() bool {
	mvcc.rw.RLock()
	return true
}

func (mvcc *MVCC) runlock() {
	mvcc.rw.RUnlock()
}

//---- Exported Control methods

// ID is same as the name supplied while creating the MVCC instance.
func (mvcc *MVCC) ID() string {
	return mvcc.name
}

// Count return the number of items indexed.
func (mvcc *MVCC) Count() int64 {
	return atomic.LoadInt64(&mvcc.n_count)
}

// Dotdump to convert whole tree into dot script that can be
// visualized using graphviz. Until dotdump exits concurrent write
// operations will block.
func (mvcc *MVCC) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))

	rsnap := mvcc.readsnapshot()
	rsnap.getroot().dotdump(buffer)
	rsnap.release()

	buffer.Write([]byte(lines[len(lines)-1]))
}

// Stats return a map of data-structure statistics and operational
// statistics.
func (mvcc *MVCC) Stats() map[string]interface{} {
	if !mvcc.rlock() {
		return nil
	}
	stats := mvcc.stats()
	mvcc.runlock()
	return stats
}

func (mvcc *MVCC) stats() map[string]interface{} {
	m := make(map[string]interface{})
	m["n_count"] = atomic.LoadInt64(&mvcc.n_count)
	m["n_inserts"] = atomic.LoadInt64(&mvcc.n_inserts)
	m["n_updates"] = atomic.LoadInt64(&mvcc.n_updates)
	m["n_deletes"] = atomic.LoadInt64(&mvcc.n_deletes)
	m["n_nodes"] = atomic.LoadInt64(&mvcc.n_nodes)
	m["n_frees"] = atomic.LoadInt64(&mvcc.n_frees)
	m["n_clones"] = atomic.LoadInt64(&mvcc.n_clones)
	m["n_txns"] = atomic.LoadInt64(&mvcc.n_txns)
	m["n_commits"] = atomic.LoadInt64(&mvcc.n_commits)
	m["n_aborts"] = atomic.LoadInt64(&mvcc.n_aborts)
	m["keymemory"] = atomic.LoadInt64(&mvcc.keymemory)
	m["valmemory"] = atomic.LoadInt64(&mvcc.valmemory)
	// mvcc
	m["n_reclaims"] = atomic.LoadInt64(&mvcc.n_reclaims)
	m["n_snapshots"] = atomic.LoadInt64(&mvcc.n_snapshots)
	m["n_purgedss"] = atomic.LoadInt64(&mvcc.n_purgedss)
	m["n_activess"] = atomic.LoadInt64(&mvcc.n_activess)
	m["n_maxverions"] = atomic.LoadInt64(&mvcc.n_maxverions)

	capacity, heap, alloc, overhead := mvcc.nodearena.Info()
	m["node.capacity"] = capacity
	m["node.heap"] = heap
	m["node.alloc"] = alloc
	m["node.overhead"] = overhead
	m["node.blocks"] = mvcc.nodearena.Slabs()

	capacity, heap, alloc, overhead = mvcc.valarena.Info()
	m["value.capacity"] = capacity
	m["value.heap"] = heap
	m["value.alloc"] = alloc
	m["value.overhead"] = overhead
	m["value.blocks"] = mvcc.valarena.Slabs()

	mvcc.rwhbf.RLock()
	m["h_bulkfree"] = mvcc.h_bulkfree.Fullstats()
	mvcc.rwhbf.RUnlock()

	m["h_reclaims"] = mvcc.h_reclaims.Fullstats()
	return m
}

// Validate data structure. This is a costly operation, walks
// through the entire tree and holds a read lock while doing so.
func (mvcc *MVCC) Validate() {
	if !mvcc.rlock() {
		return
	}
	defer mvcc.runlock()

	stats := mvcc.stats()

	n := stats["n_count"].(int64)
	kmem, vmem := stats["keymemory"].(int64), stats["valmemory"].(int64)

	wsnap := mvcc.writesnapshot()
	validatetree(wsnap.getroot(), mvcc.logprefix, n, kmem, vmem)
	mvcc.validatestats(stats)
	wsnap.release()
}

func (mvcc *MVCC) validatestats(stats map[string]interface{}) {
	// n_count should match (n_inserts - n_deletes)
	n_count := stats["n_count"].(int64)
	n_inserts := stats["n_inserts"].(int64)
	n_deletes := stats["n_deletes"].(int64)
	if n_count != (n_inserts - n_deletes) {
		fmsg := "validatestats(): n_count:%v != (n_inserts:%v - n_deletes:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_inserts, n_deletes))
	}
	// n_nodes should match n_inserts and n_count
	n_nodes := stats["n_nodes"].(int64)
	if n_inserts != n_nodes {
		fmsg := "validatestats(): n_inserts:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_inserts, n_nodes))
	} else if (n_nodes - n_deletes) != n_count {
		fmsg := "validatestats(): ncount:%v-ndels:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_count, n_deletes, n_nodes))
	}

	n_clones := stats["n_clones"].(int64)
	n_reclaims := stats["n_reclaims"].(int64)
	if n_reclaims != (n_clones + n_deletes) {
		fmsg := "validatestats(): n_clones:%v+n_dels:%v != n_reclaims:%v"
		panic(fmt.Errorf(fmsg, n_clones, n_deletes, n_reclaims))
	}
	//ok1 := (n_clones + n_deletes + n_updates) == n_frees
	//ok2 := (n_clones + n_updates + n_deletes) == (n_frees + n_reclaims)
	//if (ok1 == false) && (ok2 == false) {
	//	fmsg := "validatestats(): clones:%v+dels:%v+updts:%v != frees:%v,%v"
	//	panic(fmt.Errorf(
	//		fmsg, n_clones, n_deletes, n_updates, n_frees, n_reclaims))
	//}
	//x := lib.AbsInt64(n_deletes - n_reclaims)
	//y := lib.AbsInt64(n_clones - n_frees)
	//if x != y {
	//	fmsg := "validatestats(): abs(n_deletes:%v - n_reclaim:%v) != " +
	//		"abs(n_clones:%v - n_frees:%v)"
	//	panic(fmt.Errorf(fmsg, n_deletes, n_reclaims, n_clones, n_frees))
	//}

	entries := mvcc.Count()
	maxreclaim := 7 * math.Log2(float64(entries))  // 7x the height
	meanreclaim := 4 * math.Log2(float64(entries)) // 4x the height
	if max := float64(mvcc.h_reclaims.Max()); max > 0 {
		if entries > 1000 && max > maxreclaim {
			fmsg := "validatestats(): max %v reclaim exceeds 7*log2(%v)"
			panic(fmt.Errorf(fmsg, max, entries))
		}
	}
	if mean := float64(mvcc.h_reclaims.Mean()); mean > 0 {
		if entries > 1000 && mean > meanreclaim {
			fmsg := "validatestats(): mean %v reclaim exceeds 4*log2(%v)"
			panic(fmt.Errorf(fmsg, mean, entries))
		}
	}
}

// Log vital information.
func (mvcc *MVCC) Log() {
	if !mvcc.rlock() {
		return
	}
	defer mvcc.runlock()

	lprefix, stats := mvcc.logprefix, mvcc.stats()

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
	log.Infof("%v keymem(%v): %v\n", mvcc.logprefix, kmem, summary(as...))
	// log information about key memory utilization
	sizes, zs := mvcc.nodearena.Utilization()
	log.Infof("%v key %v", mvcc.logprefix, loguz(sizes, zs, "node"))
	// log information about value memory arena
	vmem := humanize.Bytes(uint64(stats["valmemory"].(int64)))
	as = []string{
		"value.capacity", "value.heap", "value.alloc", "value.overhead",
	}
	log.Infof("%v valmem(%v): %v\n", mvcc.logprefix, vmem, summary(as...))
	// log information about key memory utilization
	sizes, zs = mvcc.valarena.Utilization()
	log.Infof("%v val %v", mvcc.logprefix, loguz(sizes, zs, "node"))

	log.Infof("%v count: %10d\n", mvcc.logprefix, stats["n_count"])
	a, b, c := stats["n_inserts"], stats["n_updates"], stats["n_deletes"]
	log.Infof("%v write: %10d(ins) %10d(ups) %10d(del)\n", lprefix, a, b, c)
	a, b, c = stats["n_nodes"], stats["n_frees"], stats["n_clones"]
	log.Infof("%v nodes: %10d(nds) %10d(fre) %10d(cln)\n", lprefix, a, b, c)
	a, b, c = stats["n_txns"], stats["n_commits"], stats["n_aborts"]
	log.Infof("%v txns : %10d(txn) %10d(com) %10d(abr)\n", lprefix, a, b, c)
	a, b = stats["n_reclaims"], stats["n_maxverions"]
	log.Infof("%v rclms: %10d(rcm) %10d(ver)", lprefix, a, b)
	a, b, c = stats["n_snapshots"], stats["n_purgedss"], stats["n_activess"]
	log.Infof("%v snaps: %10d(tot) %10d(pur) %10d(act)", mvcc.logprefix, a, b, c)
	hstat := stats["h_bulkfree"].(map[string]interface{})
	a, b, c = hstat["samples"], hstat["max"], hstat["mean"]
	log.Infof("%v h_bulkfree: %10d(cnt) %10d(max) %10d(mea)", lprefix, a, b, c)
	hstat = stats["h_reclaims"].(map[string]interface{})
	a, b, c = hstat["samples"], hstat["max"], hstat["mean"]
	log.Infof("%v h_reclaims: %10d(cnt) %10d(max) %10d(mea)", lprefix, a, b, c)

	// log snapshots
	wsnap := mvcc.writesnapshot()
	snapshot, items := wsnap, []string{}
	for snapshot != nil {
		snapid := atomic.LoadInt64(&snapshot.id)
		refcount := atomic.LoadInt64(&snapshot.refcount)
		item := fmt.Sprintf("%v(%v)", snapid, refcount)
		items = append(items, item)
		snapshot = (*mvccsnapshot)(atomic.LoadPointer(&snapshot.next))
	}
	wsnap.release()
	log.Infof("%v snapshots %v", mvcc.logprefix, strings.Join(items, " -> "))
}

// Clone mvcc instance and return the clone. Clone walks the entire
// tree and concurrent reads and writes will block until call returns.
func (mvcc *MVCC) Clone(name string) *MVCC {
	if !mvcc.rlock() {
		return nil
	}
	defer mvcc.runlock()

	newmvcc := NewMVCC(mvcc.name, mvcc.setts)
	newmvcc.lock()
	defer newmvcc.unlock()

	wsnap := mvcc.writesnapshot()

	newmvcc.seqno = atomic.LoadUint64(&mvcc.seqno)
	newmvcc.setroot(newmvcc.clonetree(wsnap.getroot()))

	newmvcc.clonestats(mvcc.stats())
	newmvcc.h_reclaims = mvcc.h_reclaims.Clone()
	func() {
		mvcc.rwhbf.RLock()
		defer mvcc.rwhbf.RUnlock()
		newmvcc.h_bulkfree = mvcc.h_bulkfree.Clone()
	}()

	wsnap.release()

	return newmvcc
}

func (mvcc *MVCC) clonetree(nd *Llrbnode) *Llrbnode {
	if nd == nil {
		return nil
	}

	newnd := mvcc.clonenode(nd, true)
	atomic.AddInt64(&mvcc.n_clones, -1)

	newnd.left = mvcc.clonetree(nd.left)
	newnd.right = mvcc.clonetree(nd.right)
	return newnd
}

func (mvcc *MVCC) clonestats(stats map[string]interface{}) {
	atomic.StoreInt64(&mvcc.n_count, stats["n_count"].(int64))
	atomic.StoreInt64(&mvcc.n_inserts, stats["n_inserts"].(int64))
	atomic.StoreInt64(&mvcc.n_updates, stats["n_updates"].(int64))
	atomic.StoreInt64(&mvcc.n_deletes, stats["n_deletes"].(int64))
	atomic.StoreInt64(&mvcc.n_nodes, stats["n_nodes"].(int64))
	atomic.StoreInt64(&mvcc.n_frees, stats["n_frees"].(int64))
	atomic.StoreInt64(&mvcc.n_clones, stats["n_clones"].(int64))
	atomic.StoreInt64(&mvcc.n_txns, stats["n_txns"].(int64))
	atomic.StoreInt64(&mvcc.n_commits, stats["n_commits"].(int64))
	atomic.StoreInt64(&mvcc.n_aborts, stats["n_aborts"].(int64))
	atomic.StoreInt64(&mvcc.keymemory, stats["keymemory"].(int64))
	atomic.StoreInt64(&mvcc.valmemory, stats["valmemory"].(int64))
	// mvcc
	atomic.StoreInt64(&mvcc.n_reclaims, stats["n_reclaims"].(int64))
	atomic.StoreInt64(&mvcc.n_snapshots, stats["n_snapshots"].(int64))
	atomic.StoreInt64(&mvcc.n_purgedss, stats["n_purgedss"].(int64))
	atomic.StoreInt64(&mvcc.n_activess, stats["n_activess"].(int64))
	atomic.StoreInt64(&mvcc.n_maxverions, stats["n_maxverions"].(int64))
}

// Footprint return the heap footprint consumed by mvcc instance.
func (mvcc *MVCC) Footprint() int64 {
	stats := mvcc.Stats()
	return stats["node.heap"].(int64) + stats["value.heap"].(int64)
}

// Close does nothing
func (mvcc *MVCC) Close() {
	return
}

// Destroy releases all resources held by the tree. No other
// method call are allowed after Destroy.
func (mvcc *MVCC) Destroy() {
	close(mvcc.finch) // close housekeeping routine
	for atomic.LoadInt64(&mvcc.n_routines) > 0 {
		time.Sleep(mvcc.snaptick)
	}
	// n_snapshots should match (n_activess + n_purgedss)
	n_snapshots := atomic.LoadInt64(&mvcc.n_snapshots)
	n_purgedss := atomic.LoadInt64(&mvcc.n_purgedss)
	n_activess := atomic.LoadInt64(&mvcc.n_activess)
	if n_snapshots != (n_purgedss + n_activess) {
		fmsg := "n_snapshots:%v != (n_activess:%v + n_purgedss:%v)"
		panic(fmt.Errorf(fmsg, n_snapshots, n_activess, n_purgedss))
	}

	for mvcc.destroy() == false {
		time.Sleep(100 * time.Millisecond)
	}
	log.Infof("%v destroyed\n", mvcc.logprefix)
}

func (mvcc *MVCC) destroy() bool {
	if !mvcc.lock() {
		return true
	}
	defer mvcc.unlock()

	snapshot := mvcc.currsnapshot()
	if snapshot != nil {
		if snapshot.getref() > 0 {
			return false
		} else if mvcc.purgesnapshot(snapshot) == false {
			return false
		}
	}
	next := (*mvccsnapshot)(atomic.LoadPointer(&snapshot.next))
	if next != nil || snapshot.getref() > 0 {
		panic("impossible situation")
	}
	mvcc.setheadsnapshot(nil)
	mvcc.nodearena.Release()
	mvcc.valarena.Release()
	mvcc.setts, mvcc.snapcache = nil, nil
	return true
}

//---- Exported Write methods

// Setseqno can be called immediately after creating the MVCC instance.
// All futher mutating APIs will start counting seqno from this value.
func (mvcc *MVCC) Setseqno(seqno uint64) {
	atomic.StoreUint64(&mvcc.seqno, seqno)
}

// Getseqno return current seqno on this tree.
func (mvcc *MVCC) Getseqno() uint64 {
	return atomic.LoadUint64(&mvcc.seqno)
}

// Set a key, value pair in the index, if key is already present,
// its value will be over-written. Make sure key is not nil.
// Return old value if oldvalue points to a valid buffer.
func (mvcc *MVCC) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	if !mvcc.lock() {
		return
	}

	wsnap := mvcc.writesnapshot()
	ov, cas = mvcc.set(wsnap, key, value, oldvalue)
	wsnap.release()

	mvcc.unlock()
	return
}

func (mvcc *MVCC) set(
	wsnap *mvccsnapshot, key, value, oldvalue []byte) (ov []byte, cas uint64) {

	var newnd, oldnd *Llrbnode

	seqno := atomic.AddUint64(&mvcc.seqno, 1)
	reclaim := wsnap.reclaim[:0]

	root := wsnap.getroot()
	root, newnd, oldnd, reclaim = mvcc.upsert(root, 1, key, value, reclaim)
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(seqno)

	wsnap.setroot(root)
	mvcc.upsertcounts(key, value, oldnd)

	if oldvalue != nil {
		var val []byte
		if oldnd != nil {
			val = oldnd.Value()
		}
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
		copy(oldvalue, val)
	}

	mvcc.appendreclaim(reclaim)

	return oldvalue, seqno
}

func (mvcc *MVCC) upsert(
	nd *Llrbnode, depth int64,
	key, value []byte,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, *Llrbnode, []*Llrbnode) {

	var oldnd, newnd, ndmvcc *Llrbnode

	if nd == nil {
		newnd := mvcc.newnode(key, value)
		return newnd, newnd, nil, reclaim
	}
	reclaim = append(reclaim, nd)

	if nd.gtkey(key, false) {
		ndmvcc = mvcc.clonenode(nd, false)
		//ndmvcc = mvcc.walkdownrot23(ndmvcc)
		ndmvcc.left, newnd, oldnd, reclaim =
			mvcc.upsert(ndmvcc.left, depth+1, key, value, reclaim)
	} else if nd.ltkey(key, false) {
		ndmvcc = mvcc.clonenode(nd, false)
		//ndmvcc = mvcc.walkdownrot23(ndmvcc)
		ndmvcc.right, newnd, oldnd, reclaim =
			mvcc.upsert(ndmvcc.right, depth+1, key, value, reclaim)
	} else {
		ndmvcc = mvcc.clonenode(nd, true)
		//ndmvcc = mvcc.walkdownrot23(ndmvcc)
		oldnd = nd
		if nv := ndmvcc.nodevalue(); nv != nil { // free the value if pres.
			mvcc.valarena.Free(unsafe.Pointer(nv))
			ndmvcc = ndmvcc.setnodevalue(nil)
		}
		if len(value) > 0 { // add new value.
			ptr := mvcc.valarena.Alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			nv.hdr = 0
			ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
		}
		ndmvcc.setdirty()
		newnd = ndmvcc
	}

	ndmvcc, reclaim = mvcc.walkuprot23(ndmvcc, reclaim)
	return ndmvcc, newnd, oldnd, reclaim
}

// SetCAS a key, value pair in the index, if CAS is ZERO then key
// should not be present in the index, otherwise existing CAS should
// match the supplied CAS. Value will be over-written. Make sure
// key is not nil. Return old value if oldvalue points to valid buffer.
func (mvcc *MVCC) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	var err error

	if !mvcc.lock() {
		return nil, 0, fmt.Errorf("closed")
	}

	wsnap := mvcc.writesnapshot()
	oldvalue, cas, err = mvcc.setcas(wsnap, key, value, oldvalue, cas)
	wsnap.release()

	mvcc.unlock()
	return oldvalue, cas, err
}

func (mvcc *MVCC) setcas(
	wsnap *mvccsnapshot,
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	// check for cas match.
	// if cas > 0, key should be found and its seqno should match cas.
	// if cas == 0, key should be missing.
	nd, ok := mvcc.getkey(wsnap.getroot(), key)
	ok1 := (ok && nd.isdeleted() == false) && nd.getseqno() != cas
	ok2 := (ok == false || nd.isdeleted()) && cas != 0
	if ok1 || ok2 {
		if oldvalue != nil {
			oldvalue = lib.Fixbuffer(oldvalue, 0)
		}
		//fmt.Printf("SetCAS %q %v %v InvaldCAS 0\n", key, nd.getseqno(), cas)
		return oldvalue, 0, api.ErrorInvalidCAS
	}
	oldvalue, cas = mvcc.set(wsnap, key, value, oldvalue)
	return oldvalue, cas, nil
}

func (mvcc *MVCC) upsertcas(
	nd *Llrbnode, depth int64,
	key, value []byte, cas uint64,
	reclaim []*Llrbnode) (
	*Llrbnode, *Llrbnode, *Llrbnode, []*Llrbnode, error) {

	var oldnd, newnd, ndmvcc *Llrbnode
	var err error

	if nd == nil && cas > 0 { // Expected an update
		//fmt.Printf("SetCAS %q Invalid cas 1\n", key)
		return nil, nil, nil, reclaim, api.ErrorInvalidCAS

	} else if nd == nil { // Expected a create
		newnd := mvcc.newnode(key, value)
		return newnd, newnd, nil, reclaim, nil
	}
	reclaim = append(reclaim, nd)

	if nd.gtkey(key, false) {
		ndmvcc = mvcc.clonenode(nd, false)
		// ndmvcc = mvcc.walkdownrot23(ndmvcc)
		depth++
		ndmvcc.left, newnd, oldnd, reclaim, err =
			mvcc.upsertcas(ndmvcc.left, depth, key, value, cas, reclaim)

	} else if nd.ltkey(key, false) {
		ndmvcc = mvcc.clonenode(nd, false)
		// ndmvcc = mvcc.walkdownrot23(ndmvcc)
		depth++
		ndmvcc.right, newnd, oldnd, reclaim, err =
			mvcc.upsertcas(ndmvcc.right, depth, key, value, cas, reclaim)

	} else /*equal*/ {
		ndmvcc = mvcc.clonenode(nd, true)
		// ndmvcc = mvcc.walkdownrot23(ndmvcc)
		if ndmvcc.isdeleted() && (cas != 0 && cas != ndmvcc.getseqno()) {
			newnd = ndmvcc
			//fmt.Printf("SetCAS %q Invalid cas 2\n", key)
			err = api.ErrorInvalidCAS

		} else if ndmvcc.isdeleted() == false && cas != ndmvcc.getseqno() {
			newnd = ndmvcc
			//fmt.Printf("SetCAS %q Invalid cas 3\n", key)
			err = api.ErrorInvalidCAS

		} else {
			oldnd = nd
			if nv := ndmvcc.nodevalue(); nv != nil { // free the value if pres.
				mvcc.valarena.Free(unsafe.Pointer(nv))
				ndmvcc = ndmvcc.setnodevalue(nil)
			}
			if len(value) > 0 { // add new value.
				ptr := mvcc.valarena.Alloc(int64(nvaluesize + len(value)))
				nv := (*nodevalue)(ptr)
				nv.hdr = 0
				ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
			}
			ndmvcc.setdirty()
			newnd = ndmvcc
		}
	}

	ndmvcc, reclaim = mvcc.walkuprot23(ndmvcc, reclaim)
	return ndmvcc, newnd, oldnd, reclaim, err
}

// Delete key from index. Key should not be nil, if key found
// return its value. If lsm is true, then don't delete the node
// instead mark the node as deleted. Again, if lsm is true
// but key is not found in index, a new entry will be inserted.
func (mvcc *MVCC) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	if !mvcc.lock() {
		return nil, 0
	}

	wsnap := mvcc.writesnapshot()
	oldvalue, cas := mvcc.dodelete(wsnap, key, oldvalue, lsm)
	wsnap.release()

	mvcc.unlock()
	return oldvalue, cas
}

func (mvcc *MVCC) dodelete(
	wsnap *mvccsnapshot, key, oldvalue []byte, lsm bool) ([]byte, uint64) {

	var root, newnd, oldnd, deleted *Llrbnode

	seqno := atomic.AddUint64(&mvcc.seqno, 1)
	reclaim := wsnap.reclaim[:0]

	if oldvalue != nil {
		oldvalue = lib.Fixbuffer(oldvalue, 0)
	}
	if lsm {
		if nd, ok := mvcc.getkey(wsnap.getroot(), key); ok {
			// we don't do mvcc here.
			nd.setseqnodeleted(seqno) // set deleted and seqno atomically.
			if oldvalue != nil {
				val := nd.Value()
				oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
				copy(oldvalue, val)
			}

		} else {
			root, depth := wsnap.getroot(), int64(1)
			root, newnd, oldnd, reclaim =
				mvcc.upsert(root, depth, key, nil, reclaim)
			root.setblack()
			newnd.setdeleted()
			newnd.cleardirty()
			newnd.setseqno(seqno)

			wsnap.setroot(root)
			mvcc.upsertcounts(key, nil, oldnd /*nil*/)
		}

	} else {
		root, deleted, reclaim = mvcc.delete(wsnap.getroot(), key, reclaim)
		if root != nil {
			root.setblack()
		}
		wsnap.setroot(root)

		if deleted != nil && oldvalue != nil {
			val := deleted.Value()
			oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
			copy(oldvalue, val)
		}
	}

	mvcc.delcounts(deleted, lsm)
	mvcc.appendreclaim(reclaim)

	return oldvalue, seqno
}

func (mvcc *MVCC) delete(
	nd *Llrbnode, key []byte,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var newnd, deleted *Llrbnode

	if nd == nil {
		return nil, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := mvcc.clonenode(nd, true)

	if ndmvcc.gtkey(key, false) {
		if ndmvcc.left == nil { // key not present. Nothing to delete
			return ndmvcc, nil, reclaim
		}
		if !ndmvcc.left.isred() && !ndmvcc.left.left.isred() {
			ndmvcc, reclaim = mvcc.moveredleft(ndmvcc, reclaim)
		}
		ndmvcc.left, deleted, reclaim = mvcc.delete(ndmvcc.left, key, reclaim)

	} else {
		if ndmvcc.left.isred() {
			ndmvcc, reclaim = mvcc.rotateright(ndmvcc, reclaim)
		}

		// If @key equals @h.Item and no right children at @h
		if !ndmvcc.ltkey(key, false) && ndmvcc.right == nil {
			reclaim = append(reclaim, ndmvcc)
			return nil, ndmvcc, reclaim
		}
		if ndmvcc.right != nil &&
			!ndmvcc.right.isred() && !ndmvcc.right.left.isred() {
			ndmvcc, reclaim = mvcc.moveredright(ndmvcc, reclaim)
		}
		// If @key equals @h.Item, and (from above) 'h.Right != nil'
		if !ndmvcc.ltkey(key, false) {
			var subd *Llrbnode
			ndmvcc.right, subd, reclaim = mvcc.deletemin(ndmvcc.right, reclaim)
			if subd == nil {
				panic("delete(): fatal logic, call the programmer")
			}
			newnd = mvcc.clonenode(subd, true)
			newnd.left, newnd.right = ndmvcc.left, ndmvcc.right
			if ndmvcc.isdirty() {
				//newnd.metadata().setdirty()
				panic("delete(): unexpected dirty node, call the programmer")
			}
			if ndmvcc.isblack() {
				newnd.setblack()
			} else {
				newnd.setred()
			}
			sdnv := subd.nodevalue()
			if sdnv != nil {
				newnd.nodevalue().setvalue(sdnv.value())
			}
			deleted, ndmvcc = ndmvcc, newnd
			reclaim = append(reclaim, deleted)
		} else { // Else, @key is bigger than @ndmvcc
			ndmvcc.right, deleted, reclaim =
				mvcc.delete(ndmvcc.right, key, reclaim)
		}
	}
	ndmvcc, reclaim = mvcc.fixup(ndmvcc, reclaim)
	return ndmvcc, deleted, reclaim
}

// using 2-3 trees, returns root, deleted, reclaim
func (mvcc *MVCC) deletemin(
	nd *Llrbnode,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, []*Llrbnode) {

	var deleted *Llrbnode

	if nd == nil {
		return nil, nil, reclaim
	}

	reclaim = append(reclaim, nd)
	ndmvcc := mvcc.clonenode(nd, true)

	if ndmvcc.left == nil {
		reclaim = append(reclaim, ndmvcc)
		return nil, ndmvcc, reclaim
	}

	if !ndmvcc.left.isred() && !ndmvcc.left.left.isred() {
		ndmvcc, reclaim = mvcc.moveredleft(ndmvcc, reclaim)
	}

	ndmvcc.left, deleted, reclaim = mvcc.deletemin(ndmvcc.left, reclaim)
	ndmvcc, reclaim = mvcc.fixup(ndmvcc, reclaim)
	return ndmvcc, deleted, reclaim
}

// BeginTxn starts a read-write transaction. All transactions should either
// be committed or aborted. Every transaction holds on to a MVCC snapshot.
// If transactions are not released for long time accumulating too many
// background mutations, it will increase the memory pressure on the system.
// Concurrent transactions are allowed, and serialized internally.
func (mvcc *MVCC) BeginTxn(id uint64) api.Transactor {
	if snapshot := mvcc.readsnapshot(); snapshot != nil {
		atomic.AddInt64(&mvcc.n_txns, 1)
		txn := mvcc.gettxn(id, mvcc /*db*/, snapshot /*snap*/)
		return txn
	}
	return nil
}

func (mvcc *MVCC) commit(txn *Txn) error {
	if !mvcc.lock() { // no further mutations allowed until we are done.
		txn.snapshot.(*mvccsnapshot).release()
		return fmt.Errorf("close")
	}

	wsnap := mvcc.writesnapshot()

	err := mvcc.docommit(wsnap, txn)
	txn.snapshot.(*mvccsnapshot).release()
	mvcc.puttxn(txn)

	wsnap.release()

	mvcc.unlock()
	return err
}

func (mvcc *MVCC) docommit(wsnap *mvccsnapshot, txn *Txn) error {
	// Check whether writes operations match the key's CAS.
	for _, head := range txn.writes {
		prevkey := []byte(nil)
		for head != nil {
			if prevkey == nil || bytes.Compare(head.key, prevkey) != 0 {
				seqno := uint64(0)
				if nd, ok := mvcc.getkey(wsnap.getroot(), head.key); ok {
					seqno = nd.getseqno()
				}
				if seqno != head.seqno {
					return api.ErrorRollback // rollback
				}
			}
			prevkey, head = head.key, head.next
		}
	}

	// CAS matches, proceed to commit.
	for _, head := range txn.writes {
		prevkey := []byte(nil)
		for head != nil {
			if prevkey == nil || bytes.Compare(head.key, prevkey) != 0 {
				mvcc.commitrecord(wsnap, head)
			}
			prevkey, head = head.key, head.next
		}
	}

	mvcc.n_commits++
	return nil
}

func (mvcc *MVCC) commitrecord(wsnap *mvccsnapshot, rec *record) (err error) {
	switch rec.cmd {
	case cmdSet:
		mvcc.set(wsnap, rec.key, rec.value, nil)
	case cmdDelete:
		mvcc.dodelete(wsnap, rec.key, nil, rec.lsm)
	}
	return
}

func (mvcc *MVCC) aborttxn(txn *Txn) error {
	if !mvcc.lock() { // TODO: is this lock required ?
		return fmt.Errorf("closed")
	}

	snapshot := txn.snapshot.(*mvccsnapshot)
	snapshot.release()

	mvcc.puttxn(txn)
	mvcc.n_aborts++

	mvcc.unlock()
	return nil
}

// View starts a read-only transaction. Other than that it is similar
// to BeginTxn. All view transactions should be aborted.
func (mvcc *MVCC) View(id uint64) api.Transactor {
	if snapshot := mvcc.readsnapshot(); snapshot != nil {
		atomic.AddInt64(&mvcc.n_txns, 1)
		view := mvcc.getview(id, mvcc /*db*/, snapshot /*snap*/)
		return view
	}
	return nil
}

func (mvcc *MVCC) abortview(view *View) {
	snapshot := view.snapshot.(*mvccsnapshot)
	snapshot.release()

	mvcc.putview(view)
}

//---- Exported Read methods

// Get value for key, if value argument points to valid buffer, it will
// be used to copy the entry's value. Also returns entry's cas, whether
// entry is marked as deleted by LSM. If ok is false, then key is not found.
func (mvcc *MVCC) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	if snapshot := mvcc.writesnapshot(); snapshot != nil {
		v, cas, deleted, ok = snapshot.get(key, value)
		snapshot.release()
	}
	return
}

func (mvcc *MVCC) getkey(nd *Llrbnode, k []byte) (*Llrbnode, bool) {
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

func (mvcc *MVCC) Scan() api.Iterator {
	currkey := make([]byte, 1024)
	sb := makescanbuf()

	leseqno := mvcc.startscan(nil, sb, 0)
	return func(fin bool) ([]byte, []byte, uint64, bool, error) {
		key, value, seqno, deleted := sb.pop()
		if key == nil {
			mvcc.startscan(currkey, sb, leseqno)
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

// TODO: can we instead to the snapshot and avoid rlock ?
func (mvcc *MVCC) startscan(key []byte, sb *scanbuf, leseqno uint64) uint64 {
	if key == nil {
		leseqno = atomic.LoadUint64(&mvcc.seqno)
	}
	rsnap := mvcc.readsnapshot()

	sb.preparewrite()
	mvcc.scan(rsnap.getroot(), key, sb, leseqno)
	sb.prepareread()

	rsnap.release()
	return leseqno
}

func (mvcc *MVCC) scan(
	nd *Llrbnode, key []byte, sb *scanbuf, leseqno uint64) bool {

	if nd == nil {
		return true
	}
	if key != nil && nd.lekey(key, false) {
		return mvcc.scan(nd.right, key, sb, leseqno)
	}
	if !mvcc.scan(nd.left, key, sb, leseqno) {
		return false
	}
	seqno := nd.getseqno()
	if seqno <= leseqno {
		n := sb.append(nd.getkey(), nd.Value(), seqno, nd.isdeleted())
		if n >= scanlimit {
			return false
		}
	}
	return mvcc.scan(nd.right, key, sb, leseqno)
}

// llrb rotation routines for 2-3 algorithm

func (mvcc *MVCC) walkdownrot23(nd *Llrbnode) *Llrbnode {
	return nd
}

func (mvcc *MVCC) walkuprot23(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	if nd.right.isred() && !nd.left.isred() {
		nd, reclaim = mvcc.rotateleft(nd, reclaim)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd, reclaim = mvcc.rotateright(nd, reclaim)
	}
	if nd.left.isred() && nd.right.isred() {
		reclaim = mvcc.flip(nd, reclaim)
	}

	return nd, reclaim
}

func (mvcc *MVCC) rotateleft(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	y, ok := mvcc.cloneifdirty(nd.right)
	if ok {
		reclaim = append(reclaim, nd.right)
	}

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
	return y, reclaim
}

func (mvcc *MVCC) rotateright(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	x, ok := mvcc.cloneifdirty(nd.left)
	if ok {
		reclaim = append(reclaim, nd.left)
	}

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
	return x, reclaim
}

// REQUIRE: Left and Right children must be present
func (mvcc *MVCC) flip(nd *Llrbnode, reclaim []*Llrbnode) []*Llrbnode {
	x, ok := mvcc.cloneifdirty(nd.left)
	if ok {
		reclaim = append(reclaim, nd.left)
	}
	y, ok := mvcc.cloneifdirty(nd.right)
	if ok {
		reclaim = append(reclaim, nd.right)
	}

	x.togglelink()
	y.togglelink()
	nd.togglelink()
	nd.left, nd.right = x, y
	return reclaim
}

// REQUIRE: Left and Right children must be present
func (mvcc *MVCC) moveredleft(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	reclaim = mvcc.flip(nd, reclaim)
	if nd.right.left.isred() {
		nd.right, reclaim = mvcc.rotateright(nd.right, reclaim)
		nd, reclaim = mvcc.rotateleft(nd, reclaim)
		reclaim = mvcc.flip(nd, reclaim)
	}
	return nd, reclaim
}

// REQUIRE: Left and Right children must be present
func (mvcc *MVCC) moveredright(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	reclaim = mvcc.flip(nd, reclaim)
	if nd.left.left.isred() {
		nd, reclaim = mvcc.rotateright(nd, reclaim)
		reclaim = mvcc.flip(nd, reclaim)
	}
	return nd, reclaim
}

// REQUIRE: Left and Right children must be present
func (mvcc *MVCC) fixup(
	nd *Llrbnode, reclaim []*Llrbnode) (*Llrbnode, []*Llrbnode) {

	if nd.right.isred() {
		nd, reclaim = mvcc.rotateleft(nd, reclaim)
	}
	if nd.left.isred() && nd.left.left.isred() {
		nd, reclaim = mvcc.rotateright(nd, reclaim)
	}
	if nd.left.isred() && nd.right.isred() {
		reclaim = mvcc.flip(nd, reclaim)
	}
	return nd, reclaim
}

func (mvcc *MVCC) cloneifdirty(nd *Llrbnode) (*Llrbnode, bool) {
	if nd.isdirty() { // already cloned
		return nd, false
	}
	return mvcc.clonenode(nd, true), true
}

//---- snapshot routines.

func (mvcc *MVCC) makesnapshot() {
	mvcc.lock()

	nextsnap := mvcc.getsnapshot()
	n_snapshots := atomic.LoadInt64(&mvcc.n_snapshots)
	currsnap := mvcc.currsnapshot()
	nextsnap = nextsnap.initsnapshot(n_snapshots+1, mvcc, currsnap)
	mvcc.setheadsnapshot(nextsnap)

	mvcc.unlock()

	atomic.AddInt64(&mvcc.n_snapshots, 1)
	n_activess := atomic.AddInt64(&mvcc.n_activess, 1)

	wsnap := mvcc.currsnapshot()
	rsnap := (*mvccsnapshot)(atomic.LoadPointer(&wsnap.next))
	if rsnap != nil {
		next := (*mvccsnapshot)(atomic.LoadPointer(&rsnap.next))
		if mvcc.purgesnapshot(next) {
			atomic.StorePointer(&rsnap.next, nil)
		}
	}
	n_maxverions := atomic.LoadInt64(&mvcc.n_maxverions)
	if n_activess > n_maxverions {
		atomic.StoreInt64(&mvcc.n_maxverions, n_activess)
	}
}

func (mvcc *MVCC) writesnapshot() *mvccsnapshot {
loop:
	for {
		wsnap := mvcc.currsnapshot()
		if wsnap == nil {
			time.Sleep(mvcc.snaptick)
			continue
		}

		wsnap.refer()
		snap := mvcc.currsnapshot()
		snapid := atomic.LoadInt64(&snap.id)
		wsnapid := atomic.LoadInt64(&wsnap.id)
		if snapid != wsnapid {
			wsnap.release()
			continue loop
		}
		return wsnap
	}
	panic("unreachable code")
}

func (mvcc *MVCC) readsnapshot() *mvccsnapshot {
	for {
		wsnap := mvcc.currsnapshot()
		if wsnap == nil { // no snapshot available.
			time.Sleep(mvcc.snaptick)
			continue
		}
		rsnap := (*mvccsnapshot)(atomic.LoadPointer(&wsnap.next))
		if rsnap == nil {
			time.Sleep(mvcc.snaptick)
			continue
		}
		rsnap.refer()
		snap := mvcc.currsnapshot()
		snapid := atomic.LoadInt64(&snap.id)
		wsnapid := atomic.LoadInt64(&wsnap.id)
		if snapid != wsnapid {
			rsnap.release()
			continue
		}
		return rsnap
	}
	panic("unreachable code")
}

func (mvcc *MVCC) purgesnapshot(snapshot *mvccsnapshot) bool {
	if snapshot == nil {
		return true
	}
	next := (*mvccsnapshot)(atomic.LoadPointer(&snapshot.next))
	if mvcc.purgesnapshot(next) {
		atomic.StorePointer(&snapshot.next, nil)
		if snapshot.getref() == 0 {
			mvcc.rwhbf.Lock()
			mvcc.h_bulkfree.Add(int64(len(snapshot.reclaims)))
			mvcc.rwhbf.Unlock()

			// all older snapshots are purged, and this snapshot is not refered
			// by anyone.

			for _, nd := range snapshot.reclaims {
				mvcc.freenode(nd)
			}
			atomic.AddInt64(&mvcc.n_activess, -1)
			atomic.AddInt64(&mvcc.n_purgedss, 1)
			mvcc.putsnapshot(snapshot)
			snapid := atomic.LoadInt64(&snapshot.id)
			log.Debugf("%s snapshot %v PURGED...", mvcc.logprefix, snapid)
			return true
		}
	}
	return false
}

func (mvcc *MVCC) getsnapshot() (snapshot *mvccsnapshot) {
	select {
	case snapshot = <-mvcc.snapcache:
	default:
		snapshot = &mvccsnapshot{}
	}
	if snapshot.reclaims == nil {
		snapshot.reclaims = make([]*Llrbnode, 1024)
	}
	if snapshot.reclaim == nil {
		snapshot.reclaim = make([]*Llrbnode, 64)
	}
	return
}

func (mvcc *MVCC) putsnapshot(snapshot *mvccsnapshot) {
	snapshot.mvcc, snapshot.root = nil, nil
	atomic.StorePointer(&snapshot.next, nil)
	select {
	case mvcc.snapcache <- snapshot:
	default: // Leave it for GC.
	}
	return
}
