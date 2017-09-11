package llrb

import "io"
import "fmt"
import "time"
import "sync"
import "bytes"
import "sync/atomic"
import "unsafe"
import "strings"

import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/malloc"
import "github.com/prataprc/golog"
import s "github.com/prataprc/gosettings"
import humanize "github.com/dustin/go-humanize"

// MVCC manages a single instance of LLRB tree in MVCC mode.
type MVCC struct {
	llrbstats1    // 64-bit aligned snapshot statistics.
	h_upsertdepth *lib.HistogramInt64
	// can be unaligned fields
	name      string
	nodearena api.Mallocer
	valarena  api.Mallocer
	root      unsafe.Pointer // *Llrbnode1
	seqno     uint64
	rw        sync.RWMutex
	// mvcc fields
	snapshot   unsafe.Pointer // *MVCCSnapshot
	reclaim    []*Llrbnode1
	reclaims   []*Llrbnode1
	h_bulkfree *lib.HistogramInt64
	h_reclaims *lib.HistogramInt64
	h_versions *lib.HistogramInt64

	// settings
	keycapacity int64
	valcapacity int64
	snaptick    int64 // mvcc settings
	setts       s.Settings
	logprefix   string
}

// NewMVCC a new instance of in-memory sorted index.
func NewMVCC(name string, setts s.Settings) *MVCC {
	mvcc := &MVCC{name: name}
	mvcc.logprefix = fmt.Sprintf("MVCC [%s]", name)

	setts = make(s.Settings).Mixin(Defaultsettings(), setts)
	mvcc.readsettings(setts)
	mvcc.setts = setts

	// setup arena for nodes and node-values.
	memsetts := setts.Section("nodearena").Trim("nodearena.")
	mvcc.nodearena = malloc.NewArena(mvcc.keycapacity, memsetts)
	memsetts = setts.Section("valarena").Trim("valarena.")
	mvcc.valarena = malloc.NewArena(mvcc.valcapacity, memsetts)

	// statistics
	mvcc.snapshot = nil
	mvcc.h_upsertdepth = lib.NewhistorgramInt64(10, 100, 10)
	mvcc.reclaims = make([]*Llrbnode1, 0, 64)
	mvcc.reclaim = make([]*Llrbnode1, 0, 64)
	mvcc.h_bulkfree = lib.NewhistorgramInt64(100, 1000, 1000)
	mvcc.h_reclaims = lib.NewhistorgramInt64(10, 200, 20)
	mvcc.h_versions = lib.NewhistorgramInt64(1, 30, 10)

	log.Infof("%v started ...\n", mvcc.logprefix)
	mvcc.logarenasettings()
	return mvcc
}

//---- local accessor methods.

func (mvcc *MVCC) readsettings(setts s.Settings) *MVCC {
	mvcc.keycapacity = setts.Int64("keycapacity")
	mvcc.valcapacity = setts.Int64("valcapacity")
	mvcc.snaptick = setts.Int64("snapshottick")
	return mvcc
}

func (mvcc *MVCC) getroot() *Llrbnode1 {
	return (*Llrbnode1)(mvcc.root)
}

func (mvcc *MVCC) setroot(root *Llrbnode1) {
	mvcc.root = unsafe.Pointer(root)
}

func (mvcc *MVCC) newnode(k, v []byte) *Llrbnode1 {
	ptr := mvcc.nodearena.Alloc(int64(nodesize1 + len(k)))
	nd := (*Llrbnode1)(ptr)
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

func (mvcc *MVCC) freenode(nd *Llrbnode1) {
	if nd != nil {
		if nv := nd.nodevalue(); nv != nil {
			mvcc.valarena.Free(unsafe.Pointer(nv))
		}
		mvcc.nodearena.Free(unsafe.Pointer(nd))
		mvcc.n_frees++
	}
}

func (mvcc *MVCC) clonenode(nd *Llrbnode1) (newnd *Llrbnode1) {
	slabsize := mvcc.nodearena.Slabsize(unsafe.Pointer(nd))
	newptr := mvcc.nodearena.Allocslab(slabsize)
	size := mvcc.nodearena.Chunklen(unsafe.Pointer(nd))
	newnd = (*Llrbnode1)(newptr)
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

func (mvcc *MVCC) upsertcounts(key, value []byte, oldnd *Llrbnode1) {
	mvcc.keymemory += int64(len(key))
	mvcc.valmemory += int64(len(value))
	if oldnd == nil {
		mvcc.n_count++
		mvcc.n_inserts++
		return
	}
	mvcc.n_updates++
	mvcc.keymemory -= int64(len(oldnd.getkey()))
	if nv := oldnd.nodevalue(); nv != nil {
		mvcc.valmemory -= int64(len(nv.value()))
	}
}

func (mvcc *MVCC) delcounts(nd *Llrbnode1) {
	if nd != nil {
		mvcc.keymemory -= int64(len(nd.getkey()))
		if nv := nd.nodevalue(); nv != nil {
			mvcc.valmemory -= int64(len(nv.value()))
		}
		mvcc.n_count--
		mvcc.n_deletes++
	}
}

func (mvcc *MVCC) appendreclaim(reclaim []*Llrbnode1) {
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

//---- Exported Write methods

func (mvcc *MVCC) Set(key, value, oldvalue []byte) ([]byte, uint64) {
	var newnd, oldnd *Llrbnode1

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
	nd *Llrbnode1, depth int64,
	key, value []byte,
	reclaim []*Llrbnode1) (*Llrbnode1, *Llrbnode1, *Llrbnode1, []*Llrbnode1) {

	var oldnd, newnd *Llrbnode1

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

func (mvcc *MVCC) SetCAS(
	key, value, oldvalue []byte, cas uint64) ([]byte, uint64, error) {

	var newnd, oldnd *Llrbnode1
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
	nd *Llrbnode1, depth int64,
	key, value []byte, cas uint64,
	reclaim []*Llrbnode1) (
	*Llrbnode1, *Llrbnode1, *Llrbnode1, []*Llrbnode1, error) {

	var oldnd, newnd *Llrbnode1
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

func (mvcc *MVCC) Delete(key, oldvalue []byte, lsm bool) ([]byte, uint64) {
	var root, newnd, oldnd, deleted *Llrbnode1

	mvcc.rw.Lock()
	mvcc.seqno++
	reclaim := mvcc.reclaim
	mvcc.h_versions.Add(mvcc.n_activess)

	if oldvalue != nil {
		oldvalue = lib.Fixbuffer(oldvalue, 0)
	}
	seqno := mvcc.seqno
	if lsm {
		if nd, ok := mvcc.getkey(key); ok {
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

func (mvcc *MVCC) getkey(k []byte) (*Llrbnode1, bool) {
	nd := mvcc.getroot()
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

func (mvcc *MVCC) delete(
	nd *Llrbnode1, key []byte,
	reclaim []*Llrbnode1) (*Llrbnode1, *Llrbnode1, []*Llrbnode1) {

	var newnd, deleted *Llrbnode1

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
			var subd *Llrbnode1
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
	nd *Llrbnode1,
	reclaim []*Llrbnode1) (*Llrbnode1, *Llrbnode1, []*Llrbnode1) {

	var deleted *Llrbnode1

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

//---- Exported Control methods

func (mvcc *MVCC) Dotdump(buffer io.Writer) {
	lines := []string{
		"digraph llrb {",
		"  node[shape=record];\n",
		"}",
	}
	buffer.Write([]byte(strings.Join(lines[:len(lines)-1], "\n")))
	mvcc.getroot().dotdump(buffer)
	buffer.Write([]byte(lines[len(lines)-1]))
}

func (mvcc *MVCC) ID() string {
	return mvcc.name
}

func (mvcc *MVCC) Count() int64 {
	mvcc.rw.RLock()
	n_count := mvcc.n_count
	mvcc.rw.RUnlock()
	return n_count
}

func (mvcc *MVCC) Stats() map[string]interface{} {
	mvcc.rw.RLock()

	m := make(map[string]interface{})
	m["n_count"] = mvcc.n_count
	m["n_inserts"] = mvcc.n_inserts
	m["n_updates"] = mvcc.n_updates
	m["n_deletes"] = mvcc.n_deletes
	m["n_cursors"] = mvcc.n_cursors
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

func (mvcc *MVCC) Validate() {
	mvcc.rw.RLock()

	root := mvcc.getroot()
	if root != nil {
		h := lib.NewhistorgramInt64(1, 256, 1)
		blacks, depth, fromred := int64(0), int64(1), root.isred()
		nblacks, km, vm := mvcc.validatetree(root, fromred, blacks, depth, h)
		if km != mvcc.keymemory {
			fmsg := "validate(): keymemory:%v != actual:%v"
			panic(fmt.Errorf(fmsg, mvcc.keymemory, km))
		} else if vm != mvcc.valmemory {
			fmsg := "validate(): valmemory:%v != actual:%v"
			panic(fmt.Errorf(fmsg, mvcc.valmemory, vm))
		}
		if samples := h.Samples(); samples != mvcc.Count() {
			fmsg := "expected h_height.samples:%v to be same as Count():%v"
			panic(fmt.Errorf(fmsg, samples, mvcc.Count()))
		}
		log.Infof("%v found %v blacks on both sides\n", mvcc.logprefix, nblacks)
		// `h_height`.max should not exceed certain limit, maxheight
		// gives some breathing room.
		if h.Samples() > 8 {
			entries := mvcc.Count()
			if float64(h.Max()) > maxheight(entries) {
				fmsg := "validate(): max height %v exceeds <factor>*log2(%v)"
				panic(fmt.Errorf(fmsg, float64(h.Max()), entries))
			}
		}
	}

	// Validation check based on statistics accounting.

	// n_count should match (n_inserts - n_deletes)
	n_count := mvcc.n_count
	n_inserts, n_deletes := mvcc.n_inserts, mvcc.n_deletes
	if n_count != (n_inserts - n_deletes) {
		fmsg := "validatestats(): n_count:%v != (n_inserts:%v - n_deletes:%v)"
		panic(fmt.Errorf(fmsg, n_count, n_inserts, n_deletes))
	}
	// n_nodes should match n_inserts
	n_clones, n_nodes, n_frees := mvcc.n_clones, mvcc.n_nodes, mvcc.n_frees
	if n_inserts != n_nodes {
		fmsg := "validatestats(): n_inserts:%v != n_nodes:%v"
		panic(fmt.Errorf(fmsg, n_inserts, n_nodes))
	}
	if (mvcc.n_nodes - mvcc.n_count) == mvcc.n_frees {
	} else if mvcc.n_clones+(mvcc.n_nodes-mvcc.n_count) == mvcc.n_frees {
	} else {
		fmsg := "validatestats(): clones:%v+(nodes:%v-count:%v) != frees:%v"
		panic(fmt.Errorf(fmsg, n_clones, n_nodes, n_count, n_frees))
	}

	mvcc.rw.RUnlock()
}

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

func (mvcc *MVCC) Clone(name string) *MVCC {
	mvcc.rw.Lock()

	newmvcc := NewMVCC(mvcc.name, mvcc.setts)
	newmvcc.llrbstats1 = mvcc.llrbstats1
	newmvcc.h_upsertdepth = mvcc.h_upsertdepth.Clone()
	newmvcc.h_bulkfree = mvcc.h_bulkfree.Clone()
	newmvcc.h_reclaims = mvcc.h_reclaims.Clone()
	newmvcc.h_versions = mvcc.h_versions.Clone()
	newmvcc.seqno = mvcc.seqno

	newmvcc.setroot(newmvcc.clonetree(mvcc.getroot()))

	mvcc.rw.Unlock()
	return newmvcc
}

func (mvcc *MVCC) clonetree(nd *Llrbnode1) *Llrbnode1 {
	if nd == nil {
		return nil
	}

	newnd := mvcc.clonenode(nd)
	mvcc.n_clones--

	newnd.left = mvcc.clonetree(nd.left)
	newnd.right = mvcc.clonetree(nd.right)
	return newnd
}

func (mvcc *MVCC) Destroy() {
	for mvcc.destroy() == false { // snapshots are still there.
		for atomic.LoadPointer(&mvcc.snapshot) == nil {
			time.Sleep(100 * time.Millisecond)
		}
	}
	log.Infof("%v destroyed\n", mvcc.logprefix)
}

func (mvcc *MVCC) destroy() bool {
	mvcc.rw.Lock()
	defer mvcc.rw.Unlock()

	if atomic.LoadPointer(&mvcc.snapshot) != nil {
		return false
	}

	mvcc.nodearena.Release()
	mvcc.valarena.Release()
	mvcc.setroot(nil)
	mvcc.setts, mvcc.reclaim = nil, nil

	return true
}

// rotation routines for 2-3 algorithm

func (mvcc *MVCC) walkdownrot23(nd *Llrbnode1) *Llrbnode1 {
	return nd
}

func (mvcc *MVCC) walkuprot23(
	nd *Llrbnode1, reclaim []*Llrbnode1) (*Llrbnode1, []*Llrbnode1) {

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
	nd *Llrbnode1, reclaim []*Llrbnode1) (*Llrbnode1, []*Llrbnode1) {

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
	nd *Llrbnode1, reclaim []*Llrbnode1) (*Llrbnode1, []*Llrbnode1) {

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
func (mvcc *MVCC) flip(nd *Llrbnode1, reclaim []*Llrbnode1) []*Llrbnode1 {
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
	nd *Llrbnode1, reclaim []*Llrbnode1) (*Llrbnode1, []*Llrbnode1) {

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
	nd *Llrbnode1, reclaim []*Llrbnode1) (*Llrbnode1, []*Llrbnode1) {

	reclaim = mvcc.flip(nd, reclaim)
	if nd.left.left.isred() {
		nd, reclaim = mvcc.rotateright(nd, reclaim)
		reclaim = mvcc.flip(nd, reclaim)
	}
	return nd, reclaim
}

// REQUIRE: Left and Right children must be present
func (mvcc *MVCC) fixup(
	nd *Llrbnode1, reclaim []*Llrbnode1) (*Llrbnode1, []*Llrbnode1) {

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

func (mvcc *MVCC) cloneifdirty(nd *Llrbnode1) (*Llrbnode1, bool) {
	if nd.isdirty() { // already cloned
		return nd, false
	}
	return mvcc.clonenode(nd), true
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
func (mvcc *MVCC) validatetree(
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

		lblacks, lkm, lvm := mvcc.validatetree(
			nd.left, nd.isred(), blacks, depth+1, h)
		rblacks, rkm, rvm := mvcc.validatetree(
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
