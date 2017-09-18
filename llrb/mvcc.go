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
	llrbstats            // 64-bit aligned snapshot statistics.
	activetxns    uint64 // there can be more than one txns.
	h_upsertdepth *lib.HistogramInt64
	// can be unaligned fields
	name      string
	nodearena api.Mallocer
	valarena  api.Mallocer
	root      unsafe.Pointer // *Llrbnode
	seqno     uint64
	rw        sync.RWMutex
	triggch   chan bool
	finch     chan struct{}
	txnsmeta

	// mvcc fields
	snapshot   unsafe.Pointer // *mvccsnapshot
	reclaims   []*Llrbnode
	h_bulkfree *lib.HistogramInt64
	h_reclaims *lib.HistogramInt64
	h_versions *lib.HistogramInt64
	// cache
	reclaim   []*Llrbnode
	snapcache chan *mvccsnapshot

	// settings
	keycapacity int64
	valcapacity int64
	snaptick    int64 // mvcc settings
	allocator   string
	setts       s.Settings
	logprefix   string
}

// NewMVCC a new instance of in-memory sorted index.
func NewMVCC(name string, setts s.Settings) *MVCC {
	mvcc := &MVCC{
		name:      name,
		triggch:   make(chan bool, 1),
		finch:     make(chan struct{}),
		logprefix: fmt.Sprintf("MVCC [%s]", name),
		snapcache: make(chan *mvccsnapshot, 32),
	}
	mvcc.inittxns()

	setts = make(s.Settings).Mixin(Defaultsettings(), setts)
	mvcc.readsettings(setts)
	mvcc.setts = setts

	// setup arena for nodes and node-values.
	mvcc.nodearena = malloc.NewArena(mvcc.keycapacity, mvcc.allocator)
	mvcc.valarena = malloc.NewArena(mvcc.valcapacity, mvcc.allocator)

	// statistics
	mvcc.snapshot = nil
	mvcc.h_upsertdepth = lib.NewhistorgramInt64(10, 100, 10)
	mvcc.reclaims = make([]*Llrbnode, 0, 64)
	mvcc.reclaim = make([]*Llrbnode, 0, 64)
	mvcc.h_bulkfree = lib.NewhistorgramInt64(100, 1000, 1000)
	mvcc.h_reclaims = lib.NewhistorgramInt64(10, 200, 20)
	mvcc.h_versions = lib.NewhistorgramInt64(1, 30, 10)

	mvcc.makesnapshot()
	go housekeeper(mvcc, mvcc.snaptick, mvcc.triggch, mvcc.finch)

	log.Infof("%v started ...\n", mvcc.logprefix)
	mvcc.logarenasettings()
	return mvcc
}

//---- local accessor methods.

func (mvcc *MVCC) readsettings(setts s.Settings) *MVCC {
	mvcc.keycapacity = setts.Int64("keycapacity")
	mvcc.valcapacity = setts.Int64("valcapacity")
	mvcc.snaptick = setts.Int64("snapshottick")
	mvcc.allocator = setts.String("allocator")
	return mvcc
}

func (mvcc *MVCC) getroot() *Llrbnode {
	return (*Llrbnode)(mvcc.root)
}

func (mvcc *MVCC) setroot(root *Llrbnode) {
	mvcc.root = unsafe.Pointer(root)
}

func (mvcc *MVCC) newnode(k, v []byte) *Llrbnode {
	ptr := mvcc.nodearena.Alloc(int64(nodesize + len(k)))
	nd := (*Llrbnode)(ptr)
	nd.left, nd.right, nd.value = nil, nil, nil
	nd.seqflags, nd.hdr = 0, 0
	nd.setdirty().setred().setkey(k)
	if len(v) > 0 {
		ptr = mvcc.valarena.Alloc(int64(nvaluesize + len(v)))
		nv := (*nodevalue)(ptr)
		nd.setnodevalue(nv.setvalue(v))
	}
	mvcc.n_nodes++
	return nd
}

func (mvcc *MVCC) freenode(nd *Llrbnode) {
	if nd != nil {
		if nv := nd.nodevalue(); nv != nil {
			mvcc.valarena.Free(unsafe.Pointer(nv))
		}
		mvcc.nodearena.Free(unsafe.Pointer(nd))
		mvcc.n_frees++
	}
}

func (mvcc *MVCC) clonenode(nd *Llrbnode) (newnd *Llrbnode) {
	slabsize := mvcc.nodearena.Slabsize(unsafe.Pointer(nd))
	newptr := mvcc.nodearena.Allocslab(slabsize)
	size := mvcc.nodearena.Chunklen(unsafe.Pointer(nd))
	newnd = (*Llrbnode)(newptr)
	lib.Memcpy(unsafe.Pointer(newnd), unsafe.Pointer(nd), int(size))
	// clone value if value is present.
	if nv := nd.nodevalue(); nv != nil {
		slabsize = mvcc.valarena.Slabsize(unsafe.Pointer(nv))
		newnvptr := mvcc.valarena.Allocslab(slabsize)
		size := mvcc.valarena.Chunklen(unsafe.Pointer(nv))
		lib.Memcpy(newnvptr, unsafe.Pointer(nv), int(size))
		newnv := (*nodevalue)(newnvptr)
		newnd.setnodevalue(newnv)
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

func (mvcc *MVCC) delcounts(nd *Llrbnode) {
	if nd != nil {
		mvcc.keymemory -= int64(len(nd.getkey()))
		if nv := nd.nodevalue(); nv != nil {
			mvcc.valmemory -= int64(len(nv.value()))
		}
		atomic.AddInt64(&mvcc.n_count, -1)
		mvcc.n_deletes++
	}
}

func (mvcc *MVCC) appendreclaim(reclaim []*Llrbnode) {
	if len(reclaim) > 0 {
		mvcc.h_reclaims.Add(int64(len(reclaim)))
		mvcc.n_reclaims += int64(len(reclaim))
		if mvcc.n_activess == 0 {
			// no snapshots are refering to these nodes, free them.
			for _, nd := range reclaim {
				mvcc.freenode(nd)
			}
		} else {
			// snapshots do _refer_ to these nodes. remember to free later.
			mvcc.reclaims = append(mvcc.reclaims, reclaim...)
		}
	}
}

func (mvcc *MVCC) logarenasettings() {
	stats := mvcc.Stats()

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
	mvcc.rw.RLock()
	mvcc.getroot().dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
	mvcc.rw.RUnlock()
}

// Stats return a map of data-structure statistics and operational
// statistics.
func (mvcc *MVCC) Stats() map[string]interface{} {
	mvcc.rw.RLock()

	m := make(map[string]interface{})
	m["n_count"] = atomic.LoadInt64(&mvcc.n_count)
	m["n_inserts"] = mvcc.n_inserts
	m["n_updates"] = mvcc.n_updates
	m["n_deletes"] = mvcc.n_deletes
	m["n_nodes"] = mvcc.n_nodes
	m["n_frees"] = mvcc.n_frees
	m["n_clones"] = mvcc.n_clones
	m["n_reads"] = mvcc.n_reads
	m["n_txns"] = mvcc.n_txns
	m["n_commits"] = mvcc.n_commits
	m["n_aborts"] = mvcc.n_aborts
	m["keymemory"] = mvcc.keymemory
	m["valmemory"] = mvcc.valmemory
	// mvcc
	m["n_reclaims"] = mvcc.n_reclaims
	m["n_snapshots"] = mvcc.n_snapshots
	m["n_purgedss"] = mvcc.n_purgedss
	m["n_activess"] = mvcc.n_activess

	capacity, heap, alloc, overhead := mvcc.nodearena.Info()
	m["keymemory"] = mvcc.keymemory
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

	m["h_upsertdepth"] = mvcc.h_upsertdepth.Fullstats()
	m["h_bulkfree"] = mvcc.h_bulkfree.Fullstats()
	m["h_reclaims"] = mvcc.h_reclaims.Fullstats()
	m["h_versions"] = mvcc.h_versions.Fullstats()

	mvcc.rw.RUnlock()
	return m
}

// Validate data structure. This is a costly operation, walks
// through the entire tree and holds a read lock while doing so.
func (mvcc *MVCC) Validate() {
	stats := mvcc.Stats()

	mvcc.rw.RLock()
	defer mvcc.rw.RUnlock()

	n := stats["n_count"].(int64)
	kmem, vmem := stats["keymemory"].(int64), stats["valmemory"].(int64)

	validatetree(mvcc.getroot(), mvcc.logprefix, n, kmem, vmem)
	mvcc.validatestats(stats)
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

	// n_deletes + reclaim should match (n_frees - n_clones)
	total_reclaim := int64(len(mvcc.reclaims)) + mvcc.countreclaimnodes()
	x := lib.AbsInt64(n_deletes - total_reclaim)
	y := lib.AbsInt64(n_clones - n_frees)
	if x != y {
		fmsg := "abs(n_deletes:%v - reclaim:%v) != " +
			"abs(n_clones:%v - n_frees:%v)"
		panic(fmt.Errorf(fmsg, n_deletes, total_reclaim, n_clones, n_frees))
	}
	// n_snapshots should match (n_activess + n_purgedss)
	n_snapshots := stats["n_snapshots"].(int64)
	n_purgedss := stats["n_purgedss"].(int64)
	n_activess := stats["n_activess"].(int64)
	if n_snapshots != (n_purgedss + n_activess) {
		fmsg := "n_snapshots:%v != (n_activess:%v + n_purgedss:%v)"
		panic(fmt.Errorf(fmsg, n_snapshots, n_activess, n_purgedss))
	}

	entries := mvcc.Count()
	maxreclaim := 7 * math.Log2(float64(entries))  // 7x the height
	meanreclaim := 3 * math.Log2(float64(entries)) // 3x the height
	if max := float64(mvcc.h_reclaims.Max()); max > 0 {
		if max > maxreclaim {
			fmsg := "validatestats(): max %v reclaim exceeds 7*log2(%v)"
			panic(fmt.Errorf(fmsg, max, entries))
		}
	}
	if mean := float64(mvcc.h_reclaims.Mean()); mean > 0 {
		if mean > meanreclaim {
			fmsg := "validatestats(): mean %v reclaim exceeds 3*log2(%v)"
			panic(fmt.Errorf(fmsg, mean, entries))
		}
	}
}

// return the sum of all nodes that needs to be reclaimed from snapshots.
func (mvcc *MVCC) countreclaimnodes() (total int64) {
	snapshot := (*mvccsnapshot)(mvcc.snapshot)
	for ; snapshot != nil; snapshot = snapshot.next {
		total += int64(len(snapshot.reclaims))
	}
	return total
}

// Log vital information.
func (mvcc *MVCC) Log() {
	mvcc.rw.RLock()

	stats := mvcc.Stats()

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

	mvcc.rw.RUnlock()
}

// Clone mvcc instance and return the clone. Clone walks the entire
// tree and concurrent reads and writes will block until call returns.
func (mvcc *MVCC) Clone(name string) *MVCC {
	mvcc.rw.Lock()

	newmvcc := NewMVCC(mvcc.name, mvcc.setts)
	newmvcc.llrbstats = mvcc.llrbstats
	newmvcc.h_upsertdepth = mvcc.h_upsertdepth.Clone()
	newmvcc.h_bulkfree = mvcc.h_bulkfree.Clone()
	newmvcc.h_reclaims = mvcc.h_reclaims.Clone()
	newmvcc.h_versions = mvcc.h_versions.Clone()
	newmvcc.seqno = mvcc.seqno

	newmvcc.setroot(newmvcc.clonetree(mvcc.getroot()))

	mvcc.rw.Unlock()
	return newmvcc
}

func (mvcc *MVCC) clonetree(nd *Llrbnode) *Llrbnode {
	if nd == nil {
		return nil
	}

	newnd := mvcc.clonenode(nd)
	mvcc.n_clones--

	newnd.left = mvcc.clonetree(nd.left)
	newnd.right = mvcc.clonetree(nd.right)
	return newnd
}

// Destroy releases all resources held by the tree. No other
// method call are allowed after Destroy.
func (mvcc *MVCC) Destroy() {
	close(mvcc.finch) // close housekeeping routine
	time.Sleep(time.Duration(mvcc.snaptick+10) * time.Millisecond)
	for mvcc.destroy() == false {
		time.Sleep(10 * time.Millisecond)
	}
	log.Infof("%v destroyed\n", mvcc.logprefix)
}

func (mvcc *MVCC) destroy() bool {
	mvcc.rw.Lock()
	defer mvcc.rw.Unlock()

	snapshot := (*mvccsnapshot)(mvcc.snapshot)
	if snapshot != nil {
		if snapshot.getref() > 0 {
			return false
		} else if mvcc.purgesnapshot(snapshot) == false {
			return false
		}
	}
	if snapshot.next != nil || snapshot.getref() > 0 {
		panic("impossible situation")
	}
	mvcc.snapshot = nil
	mvcc.nodearena.Release()
	mvcc.valarena.Release()
	mvcc.setroot(nil)
	close(mvcc.triggch)
	mvcc.setts, mvcc.reclaim, mvcc.snapcache = nil, nil, nil
	return true
}

//---- Exported Write methods

// Set a key, value pair in the index, if key is already present,
// its value will be over-written. Make sure key is not nil.
// Return old value if oldvalue is not nil.
func (mvcc *MVCC) Set(key, value, oldvalue []byte) (ov []byte, cas uint64) {
	var newnd, oldnd *Llrbnode

	mvcc.rw.Lock()

	mvcc.seqno++
	reclaim := mvcc.reclaim
	mvcc.h_versions.Add(mvcc.n_activess)

	root := mvcc.getroot()
	root, newnd, oldnd, reclaim = mvcc.upsert(root, 1, key, value, reclaim)
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(mvcc.seqno)
	seqno := mvcc.seqno

	mvcc.setroot(root)
	mvcc.upsertcounts(key, value, oldnd)

	if oldvalue != nil {
		var val []byte
		if oldnd != nil {
			val = oldnd.Value()
		}
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
		copy(oldvalue, val)
	}

	mvcc.freenode(oldnd)
	mvcc.appendreclaim(reclaim)
	mvcc.reclaim = reclaim[:0]

	mvcc.rw.Unlock()
	return oldvalue, seqno
}

func (mvcc *MVCC) upsert(
	nd *Llrbnode, depth int64,
	key, value []byte,
	reclaim []*Llrbnode) (*Llrbnode, *Llrbnode, *Llrbnode, []*Llrbnode) {

	var oldnd, newnd *Llrbnode

	if nd == nil {
		newnd := mvcc.newnode(key, value)
		mvcc.h_upsertdepth.Add(depth)
		return newnd, newnd, nil, reclaim
	}
	reclaim = append(reclaim, nd)
	ndmvcc := mvcc.clonenode(nd)

	ndmvcc = mvcc.walkdownrot23(ndmvcc)

	if ndmvcc.gtkey(key, false) {
		ndmvcc.left, newnd, oldnd, reclaim =
			mvcc.upsert(ndmvcc.left, depth+1, key, value, reclaim)
	} else if ndmvcc.ltkey(key, false) {
		ndmvcc.right, newnd, oldnd, reclaim =
			mvcc.upsert(ndmvcc.right, depth+1, key, value, reclaim)
	} else {
		oldnd = nd
		if nv := ndmvcc.nodevalue(); nv != nil { // free the value if pres.
			mvcc.valarena.Free(unsafe.Pointer(nv))
			ndmvcc = ndmvcc.setnodevalue(nil)
		}
		if len(value) > 0 { // add new value.
			ptr := mvcc.valarena.Alloc(int64(nvaluesize + len(value)))
			nv := (*nodevalue)(ptr)
			ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
		}
		ndmvcc.setdirty()
		newnd = ndmvcc
		mvcc.h_upsertdepth.Add(depth)
	}

	ndmvcc, reclaim = mvcc.walkuprot23(ndmvcc, reclaim)
	return ndmvcc, newnd, oldnd, reclaim
}

// SetCAS a key, value pair in the index, if CAS is ZERO then key
// should not be present in the index, otherwise existing CAS should
// match the supplied CAS. Value will be over-written. Make sure
// key is not nil. Return old value if oldvalue is not nil.
func (mvcc *MVCC) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	var newnd, oldnd *Llrbnode
	var err error

	mvcc.rw.Lock()

	mvcc.seqno++
	reclaim := mvcc.reclaim
	mvcc.h_versions.Add(mvcc.n_activess)

	root, depth := mvcc.getroot(), int64(1)
	root, newnd, oldnd, reclaim, err =
		mvcc.upsertcas(root, depth, key, value, cas, reclaim)
	if err != nil {
		if oldvalue != nil {
			oldvalue = lib.Fixbuffer(oldvalue, 0)
		}
		mvcc.rw.Unlock()
		return oldvalue, 0, err
	}
	root.setblack()
	newnd.cleardeleted()
	newnd.cleardirty()
	newnd.setseqno(mvcc.seqno)
	seqno := mvcc.seqno

	mvcc.setroot(root)
	mvcc.upsertcounts(key, value, oldnd)

	if oldvalue != nil {
		var val []byte
		if oldnd != nil {
			val = oldnd.Value()
		}
		oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
		copy(oldvalue, val)
	}

	mvcc.freenode(oldnd)
	mvcc.appendreclaim(reclaim)
	mvcc.reclaim = reclaim[:0]

	mvcc.rw.Unlock()
	return oldvalue, seqno, nil
}

func (mvcc *MVCC) upsertcas(
	nd *Llrbnode, depth int64,
	key, value []byte, cas uint64,
	reclaim []*Llrbnode) (
	*Llrbnode, *Llrbnode, *Llrbnode, []*Llrbnode, error) {

	var oldnd, newnd *Llrbnode
	var err error

	if nd == nil && cas > 0 { // Expected an update
		return nil, nil, nil, reclaim, api.ErrorInvalidCAS

	} else if nd == nil { // Expected a create
		newnd := mvcc.newnode(key, value)
		mvcc.h_upsertdepth.Add(depth)
		return newnd, newnd, nil, reclaim, nil
	}
	reclaim = append(reclaim, nd)
	ndmvcc := mvcc.clonenode(nd)

	ndmvcc = mvcc.walkdownrot23(ndmvcc)

	if ndmvcc.gtkey(key, false) {
		depth++
		ndmvcc.left, newnd, oldnd, reclaim, err =
			mvcc.upsertcas(ndmvcc.left, depth, key, value, cas, reclaim)

	} else if ndmvcc.ltkey(key, false) {
		depth++
		ndmvcc.right, newnd, oldnd, reclaim, err =
			mvcc.upsertcas(ndmvcc.right, depth, key, value, cas, reclaim)

	} else /*equal*/ {
		if ndmvcc.isdeleted() && (cas != 0 && cas != ndmvcc.getseqno()) {
			newnd = ndmvcc
			err = api.ErrorInvalidCAS

		} else if ndmvcc.isdeleted() == false && cas != ndmvcc.getseqno() {
			newnd = ndmvcc
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
				ndmvcc = ndmvcc.setnodevalue(nv.setvalue(value))
			}
			ndmvcc.setdirty()
			newnd = ndmvcc
			mvcc.h_upsertdepth.Add(depth)
		}
	}

	ndmvcc, reclaim = mvcc.walkuprot23(ndmvcc, reclaim)
	return ndmvcc, newnd, oldnd, reclaim, err
}

// Delete key from index. Key should not be nil, if key is found
// return its value. If lsm is true, then don't delete the node
// instead mark the node as deleted. Again, if lsm is true
// but key is not found in index a new entry will be inserted.
func (mvcc *MVCC) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	var root, newnd, oldnd, deleted *Llrbnode

	mvcc.rw.Lock()

	mvcc.seqno++
	reclaim := mvcc.reclaim
	mvcc.h_versions.Add(mvcc.n_activess)

	if oldvalue != nil {
		oldvalue = lib.Fixbuffer(oldvalue, 0)
	}
	seqno := mvcc.seqno
	if lsm {
		if nd, ok := mvcc.getkey(mvcc.getroot(), key); ok {
			nd.setdeleted()
			nd.setseqno(seqno)
			if oldvalue != nil {
				val := nd.Value()
				oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
				copy(oldvalue, val)
			}

		} else {
			root, depth := mvcc.getroot(), int64(1)
			root, newnd, oldnd, reclaim =
				mvcc.upsert(root, depth, key, nil, reclaim)
			root.setblack()
			newnd.setdeleted()
			newnd.cleardirty()
			newnd.setseqno(mvcc.seqno)

			mvcc.setroot(root)
			mvcc.upsertcounts(key, nil, oldnd /*nil*/)
		}

	} else {
		root, deleted, reclaim = mvcc.delete(mvcc.getroot(), key, reclaim)
		if root != nil {
			root.setblack()
		}
		mvcc.setroot(root)
		mvcc.delcounts(deleted)

		if deleted != nil && oldvalue != nil {
			val := deleted.Value()
			oldvalue = lib.Fixbuffer(oldvalue, int64(len(val)))
			copy(oldvalue, val)
			mvcc.freenode(deleted)
		}
	}

	mvcc.appendreclaim(reclaim)
	mvcc.reclaim = reclaim[:0]

	mvcc.rw.Unlock()
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
	ndmvcc := mvcc.clonenode(nd)

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
			newnd = mvcc.clonenode(subd)
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
	ndmvcc := mvcc.clonenode(nd)

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
// Concurrent transactions are allowed.
func (mvcc *MVCC) BeginTxn(id uint64) *Txn {
	snapshot := mvcc.latestsnapshot()
	mvcc.activetxns++
	mvcc.n_txns++
	txn := mvcc.gettxn(id, mvcc /*db*/, snapshot /*snap*/)
	return txn
}

func (mvcc *MVCC) commit(txn *Txn) error {
	mvcc.rw.Lock() // no further mutations allowed until we are done.

	// Check whether writes operations match the key's CAS.
	for _, head := range txn.writes {
		prevkey := []byte(nil)
		for head != nil {
			if prevkey == nil || bytes.Compare(head.key, prevkey) != 0 {
				seqno := uint64(0)
				if nd, ok := mvcc.getkey(mvcc.getroot(), head.key); ok {
					seqno = nd.getseqno()
				}
				if seqno != head.seqno {
					mvcc.rw.Unlock()
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
				mvcc.commitrecord(head)
			}
			prevkey, head = head.key, head.next
		}
	}

	mvcc.puttxn(txn)
	mvcc.n_commits++
	mvcc.activetxns--

	mvcc.rw.Unlock()
	return nil
}

func (mvcc *MVCC) commitrecord(rec *record) error {
	switch rec.cmd {
	case cmdSet:
		mvcc.SetCAS(rec.key, rec.value, nil, rec.seqno)
	case cmdDelete:
		mvcc.Delete(rec.key, nil, rec.lsm)
	}
	return nil
}

func (mvcc *MVCC) aborttxn(txn *Txn) error {
	mvcc.rw.Lock()

	mvcc.puttxn(txn)
	mvcc.n_aborts++
	mvcc.activetxns--

	mvcc.rw.Unlock()
	return nil
}

// View starts a read-only transaction. Other than that it is similar
// to BeginTxn. All view transactions should be aborted.
func (mvcc *MVCC) View(id uint64) *View {
	snapshot := mvcc.latestsnapshot()
	mvcc.activetxns++
	mvcc.n_txns++
	view := mvcc.getview(id, mvcc /*db*/, snapshot /*snap*/)
	return view
}

func (mvcc *MVCC) abortview(view *View) {
	mvcc.rw.Lock()

	mvcc.putview(view)
	mvcc.activetxns--

	mvcc.rw.Unlock()
}

//---- Exported Read methods

// Get value for key, if value argument is not nil it will be used to copy the
// entry's value. Also returns entry's cas, whether entry is marked as deleted
// by LSM. If ok is false, then key is not found.
func (mvcc *MVCC) Get(
	key, value []byte) (v []byte, cas uint64, deleted, ok bool) {

	snapshot := mvcc.latestsnapshot()
	v, cas, deleted, ok = snapshot.get(key, value)
	snapshot.release()
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
	return mvcc.clonenode(nd), true
}

//---- snapshot routines.

func (mvcc *MVCC) makesnapshot() {
	mvcc.rw.Lock()
	snapshot := mvcc.getsnapshot()
	snapshot.initsnapshot(mvcc)
	mvcc.purgesnapshot(snapshot.next)
	mvcc.rw.Unlock()
}

func (mvcc *MVCC) latestsnapshot() *mvccsnapshot {
	mvcc.rw.RLock()
	snapshot := (*mvccsnapshot)(mvcc.snapshot)
	if snapshot != nil {
		snapshot.refer()
	}
	mvcc.rw.RUnlock()
	return snapshot
}

func (mvcc *MVCC) releasesnapshot(snapshot *mvccsnapshot) {
	mvcc.rw.Lock()
	mvcc.purgesnapshot(snapshot)
	mvcc.rw.Unlock()
}

func (mvcc *MVCC) purgesnapshot(snapshot *mvccsnapshot) bool {
	if snapshot == nil {
		return true
	}
	if snapshot.getref() > 0 {
		return false
	}
	if mvcc.purgesnapshot(snapshot.next) == false {
		return false
	}
	// all older snapshots are purged.
	mvcc.h_bulkfree.Add(int64(len(snapshot.reclaims)))
	for _, nd := range snapshot.reclaims {
		snapshot.mvcc.freenode(nd)
	}
	mvcc.n_activess--
	mvcc.n_purgedss++
	mvcc.putsnapshot(snapshot)
	log.Debugf("%s snapshot %s PURGED...\n", mvcc.logprefix, snapshot.id())
	return true
}

func (mvcc *MVCC) getsnapshot() (snapshot *mvccsnapshot) {
	select {
	case snapshot = <-mvcc.snapcache:
	default:
		snapshot = &mvccsnapshot{}
	}
	return
}

func (mvcc *MVCC) putsnapshot(snapshot *mvccsnapshot) {
	select {
	case mvcc.snapcache <- snapshot:
	default: // Leave it for GC.
	}
	return
}
