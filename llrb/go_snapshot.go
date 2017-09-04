package llrb

import "sync/atomic"
import "time"
import "strings"
import "math"
import "io"
import "fmt"
import "strconv"
import "runtime/debug"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"

// go-routine to generate snapshots.
func (writer *LLRBWriter) snapshotticker(interval int64, finch chan bool) {
	llrb := writer.llrb
	tick := time.NewTicker(time.Duration(interval) * time.Millisecond)

	defer func() {
		if r := recover(); r != nil {
			log.Errorf("%v snapshotticker() crashed: %v\n", llrb.logprefix, r)
			log.Errorf("\n%s", lib.GetStacktrace(2, debug.Stack()))
			for err := llrb.Destroy(); err != nil; { // handle active iterators.
				err = llrb.Destroy()
				time.Sleep(writer.tryexitafter /* 100ms ? */)
			}
		}
		tick.Stop()
	}()

loop:
	for {
		<-tick.C
		id := strconv.Itoa(int(time.Now().UnixNano() >> 19))
		select { // break out if writer has exited
		case <-finch:
			break loop
		default:
		}
		log.Tracef("%v snapshot tick for $%v ...\n", llrb.logprefix, id)
		if err := writer.makeSnapshot(id); err != nil {
			if err.Error() != "closed" {
				fmsg := "%v make snapshot $%v failed: %v\n"
				log.Errorf(fmsg, llrb.logprefix, id, err)
			}
			break loop
		}
	}
}

// LLRBSnapshot holds on to a read-only version of the LLRB tree.
// An llrb snapshot gaurantees a point in time snapshot of the entire
// tree, as a side-effect this will put memory pressure when there
// is a continuous stream of background mutations. Hence applications
// are adviced to hold on to this snapshot not exceeding couple of
// milliseconds.
type LLRBSnapshot struct {
	refcount int64
	llrbstats

	// can be unaligned fields

	llrb  *LLRB
	id    string // snapshot id
	root  *Llrbnode
	seqno uint64
	dead  bool

	// snapshot specific fields
	reclaim []*Llrbnode
	next    *LLRBSnapshot

	// settings
	logprefix string
}

// newsnapshot mvcc version for LLRB tree.
func newsnapshot(llrb *LLRB, id string) *LLRBSnapshot {
	snapshot := &LLRBSnapshot{
		llrb:  llrb,
		id:    id,
		root:  llrb.getroot(),
		seqno: llrb.seqno,
		dead:  llrb.dead,
	}
	snapshot.copystats(llrb)
	snapshot.logprefix = fmt.Sprintf("[LLRBSnapshot-%s/%s]", llrb.name, id)

	snapshot.reclaim = make([]*Llrbnode, len(llrb.mvcc.reclaim))
	copy(snapshot.reclaim, llrb.mvcc.reclaim)
	llrb.mvcc.reclaim = llrb.mvcc.reclaim[:0] // reset writer reclaims

	// track to the tail of snapshot list.
	if llrb.mvcc.snapshot == nil {
		llrb.mvcc.snapshot = snapshot
	} else {
		parent := llrb.mvcc.snapshot
		for parent.next != nil {
			parent = parent.next
		}
		parent.next = snapshot
	}

	fmsg := "%v snapshot BORN %v nodes to reclaim...\n"
	log.Debugf(fmsg, snapshot.logprefix, len(snapshot.reclaim))
	atomic.AddInt64(&llrb.n_snapshots, 1)
	atomic.AddInt64(&llrb.n_activess, 1)
	return snapshot
}

// return the sum of all nodes that needs to be reclaimed from snapshots.
func countreclaimnodes(head *LLRBSnapshot) (total int64) {
	for snapshot := head; snapshot != nil; snapshot = snapshot.next {
		total += int64(len(snapshot.reclaim))
	}
	return total
}

func (snapshot *LLRBSnapshot) copystats(llrb *LLRB) *LLRBSnapshot {
	// writer statistics
	snapshot.llrbstats = llrb.llrbstats
	snapshot.n_lookups, snapshot.n_ranges = 0, 0
	snapshot.n_activeiter, snapshot.n_cclookups, snapshot.n_ccranges = 0, 0, 0
	return snapshot
}

//---- implement api.IndexMeta interface.

// ID implement api.IndexMeta interface.
func (snapshot *LLRBSnapshot) ID() string {
	return snapshot.id
}

// Count implement api.IndexMeta interface.
func (snapshot *LLRBSnapshot) Count() int64 {
	return snapshot.n_count
}

// Isactive implement api.IndexMeta interface.
func (snapshot *LLRBSnapshot) Isactive() bool {
	return snapshot.dead == false
}

// Metadata implement api.IndexMeta interface. Call method
// on index instance, this will return nil.
func (snapshot *LLRBSnapshot) Metadata() []byte {
	return nil
}

// Stats implement api.IndexMeta interface.
func (snapshot *LLRBSnapshot) Stats() (map[string]interface{}, error) {
	return snapshot.stats(snapshot)
}

// Fullstats implement api.IndexMeta interface.
func (snapshot *LLRBSnapshot) Fullstats() (map[string]interface{}, error) {
	return snapshot.fullstats(snapshot)
}

// Validate implement api.IndexMeta interface. Will walk
// the full tree to confirm the sort order and check for
// memory leaks.
func (snapshot *LLRBSnapshot) Validate() {
	snapshot.validate(snapshot.root)
}

// Log implement api.IndexMeta interface. Call Log on the index
// instance, calling it on snapshot will panic.
func (snapshot *LLRBSnapshot) Log(string, bool) {
	panic("call this method on the index, snapshot don't support it")
}

//---- api.IndexSnapshot{} interface.

// Refer implement api.IndexSnapshot interface.
func (snapshot *LLRBSnapshot) Refer() {
	log.Debugf("%v snapshot REF\n", snapshot.logprefix)
	atomic.AddInt64(&snapshot.refcount, 1)
}

// Release implement api.IndexSnapshot interface. Snapshot will be purged
// only when all references to this snapshot has Release-d it. That includes
// the iterators.
func (snapshot *LLRBSnapshot) Release() {
	log.Debugf("%v snapshot DEREF\n", snapshot.logprefix)
	refcount := atomic.AddInt64(&snapshot.refcount, -1)
	if refcount < 0 {
		panic("Release(): snapshot refcount gone negative")
	} else if refcount > 0 {
		return
	}
	// refcount is ZERO
	n_activeiter := atomic.LoadInt64(&snapshot.n_activeiter)
	if n_activeiter > 0 {
		fmsg := "%v Release(): refcount is ZERO but iters are %v\n"
		panic(fmt.Errorf(fmsg, snapshot.logprefix, n_activeiter))
	}
	if err := snapshot.llrb.mvcc.writer.purgeSnapshot(); err != nil {
		log.Errorf("%v purgeSnapshot(): %v\n", snapshot.logprefix, err)
	}
}

// Dotdump to convert whole tree into dot script that can be visualized using
// graphviz.
func (snapshot *LLRBSnapshot) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	nd := snapshot.root
	nd.dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

//---- api.IndexReader{} interface.

// Has implement api.IndexReader{} interface.
func (snapshot *LLRBSnapshot) Has(key []byte) bool {
	return snapshot.Get(key, nil)
}

// Get implement api.IndexReader{} interface.
func (snapshot *LLRBSnapshot) Get(key []byte, callb api.NodeCallb) bool {
	snapshot.countlookup(atomic.LoadInt64(&snapshot.llrb.mvcc.ismut))
	_, ok := getkey(snapshot.llrb, snapshot.root, key, callb)
	return ok
}

// Range implement api.IndexReader{} interface.
func (snapshot *LLRBSnapshot) Range(
	lkey, hkey []byte, incl string, reverse bool, callb api.NodeCallb) {

	var skip bool
	lkey, hkey, incl, skip = fixrangeargs(lkey, hkey, incl)
	if skip {
		return
	}
	dorange(snapshot.llrb, snapshot.root, lkey, hkey, incl, reverse, callb)
	snapshot.countrange(atomic.LoadInt64(&snapshot.llrb.mvcc.ismut))
}

// Iterate implement api.IndexReader{} interface.
func (snapshot *LLRBSnapshot) Iterate(
	lkey, hkey []byte, incl string, r bool) api.IndexIterator {

	var skip bool
	lkey, hkey, incl, skip = fixrangeargs(lkey, hkey, incl)
	if skip {
		return nil
	}

	llrb := snapshot.llrb
	iter := inititerator(llrb, snapshot, lkey, hkey, incl, r)

	snapshot.countrange(atomic.LoadInt64(&snapshot.llrb.mvcc.ismut))
	atomic.AddInt64(&snapshot.n_activeiter, 1)
	return iter
}

// Validate the following expectation on the snapshot.
//  * Walk the tree and calculate cummulative memory consumed by
//    all keys, confirm the same with keymemory accounting.
//  * Walk the tree and calculate cummulative memory consumed by
//    all values, confirm the same with valmemory accounting.
//  * Maximum height of the tree should not exceed 3*math.Log2(n),
//    where `n` is the number of entries on the snapshot.
//  * Validatestats.
func (snapshot *LLRBSnapshot) validate(root *Llrbnode) {
	llrb, h := snapshot.llrb, lib.NewhistorgramInt64(1, 256, 1)
	_, km, vm := llrb.validatetree(root, isred(root), 0 /*blcks*/, 1 /*dep*/, h)
	if km != snapshot.keymemory {
		fmsg := "validate(): keymemory:%v != actual:%v"
		panic(fmt.Errorf(fmsg, snapshot.keymemory, km))
	} else if vm != snapshot.valmemory {
		fmsg := "validate(): valmemory:%v != actual:%v"
		panic(fmt.Errorf(fmsg, snapshot.valmemory, vm))
	}
	// h.max should not exceed certain limit
	if h.Samples() > 8 {
		nf := float64(snapshot.Count())
		if float64(h.Max()) > (3 * math.Log2(nf)) {
			fmsg := "validate(): max height %v exceeds log2(snapshot.count) %v"
			panic(fmt.Errorf(fmsg, float64(h.Max()), nf))
		}
	}
	snapshot.validatestats()
}

// validatestats can be used for quick validation based on
// statistic accounting.
func (snapshot *LLRBSnapshot) validatestats() {
	// n_count should match (n_inserts - n_deletes)
	n_count := snapshot.n_count
	n_inserts, n_deletes := snapshot.n_inserts, snapshot.n_deletes
	if n_count != (n_inserts - n_deletes) {
		fmsg := "sstats(): n_count:%v != (n_inserts:%v - n_deletes:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_inserts, n_deletes))
	}
	// n_nodes should match n_inserts
	n_nodes := snapshot.n_nodes
	if n_inserts != n_nodes {
		fmsg := "sstats(): n_inserts:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_inserts, n_nodes))
	}
	// n_count should match ((n_nodes + n_clones - n_reclaims)
	n_clones, n_reclaims := snapshot.n_clones, snapshot.n_reclaims
	if n_count != (n_nodes + n_clones - n_reclaims) {
		fmsg := "sstats(): n_count:%v != (n_nodes:%v + n_clones:%v - reclaims:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_nodes, n_clones, n_reclaims))
	}
	// n_deletes should match (n_reclaims - n_clones)
	if n_deletes != (n_reclaims - n_clones) {
		fmsg := "sstats(): n_deletes:%v != (n_reclaims:%v - n_clones:%v)"
		panic(fmt.Errorf(fmsg, n_deletes, n_reclaims, n_clones))
	}
	return
}
