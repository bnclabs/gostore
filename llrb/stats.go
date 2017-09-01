package llrb

import "fmt"
import "time"
import "strings"
import "sync/atomic"

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"
import "github.com/prataprc/golog"
import gohumanize "github.com/dustin/go-humanize"

type llrbstats struct {
	// 64-bit aligned reader statistics.
	n_lookups int64
	n_casgets int64
	n_ranges  int64

	// 64-bit aligned snapshot statistics.
	n_activeiter int64
	n_cclookups  int64
	n_ccranges   int64

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

	// 64-bit aligned mvcc stats.
	n_snapshots int64
	n_purgedss  int64
	n_activess  int64
	n_reclaims  int64
}

//---- write methods

// count lookup operation in mvcc mode, if there is on-going mutation, count
// as concurrent-lookup n_cclookups, else as n_lookups.
func (stats *llrbstats) countlookup(ismut int64) {
	atomic.AddInt64(&stats.n_lookups, 1)
	if ismut > 0 {
		atomic.AddInt64(&stats.n_cclookups, 1)
	}
}

// count range operation in mvcc mode, if there is on-going mutation, count
// as concurrent-range n_ccranges, else as n_ranges.
func (stats *llrbstats) countrange(ismut int64) {
	atomic.AddInt64(&stats.n_ranges, 1)
	if ismut > 0 {
		atomic.AddInt64(&stats.n_ccranges, 1)
	}
}

//---- read methods

// gether stats either on LLRB or LLRBSnapshot tree, this function is
// expected to return back quickly.
func (stats *llrbstats) stats(
	tree api.IndexMeta) (map[string]interface{}, error) {

	m := make(map[string]interface{})
	m = stats.statsrd(stats.statsmvcc(stats.statswt(m)))
	if llrb, ok := tree.(*LLRB); ok {
		m = llrb.statsval(llrb.statskey(m))
		m["h_upsertdepth"] = llrb.h_upsertdepth.Fullstats()
		if llrb.mvcc.enabled {
			m["mvcc.h_bulkfree"] = llrb.mvcc.h_bulkfree.Fullstats()
			m["mvcc.h_versions"] = llrb.mvcc.h_versions.Fullstats()
			for k, h := range llrb.mvcc.h_reclaims {
				m["mvcc.h_reclaims."+k] = h.Fullstats()
			}
		}
	}
	return m, nil
}

// along with basic tree statistics, walks the tree to gather:
// * histogram of tree-height
// * number blacks, also make sures that number of blacks
//   on the left path patches with number of blacks on the right
//   path. this is a basic llrb invariant.
func (stats *llrbstats) fullstats(
	tree api.IndexMeta) (map[string]interface{}, error) {

	m, err := stats.stats(tree)
	if err != nil {
		return nil, err
	}
	var root *Llrbnode
	switch v := tree.(type) {
	case *LLRB:
		root = v.getroot()
	case *LLRBSnapshot:
		root = v.root
	}

	h_heightav := lib.NewhistorgramInt64(1, 256, 1)
	n_blacks := treestats(root, 1 /*depth*/, h_heightav, 0)
	m["h_height"] = h_heightav.Fullstats()
	m["n_blacks"] = n_blacks

	h_height := m["h_height"].(map[string]interface{})
	if x := h_height["samples"].(int64); x != tree.Count() {
		fmsg := "expected h_height.samples:%v to be same as tree.Count():%v"
		panic(fmt.Errorf(fmsg, x, tree.Count()))
	}
	return m, nil
}

// read op statistics.
func (stats *llrbstats) statsrd(m map[string]interface{}) map[string]interface{} {
	m["n_lookups"] = atomic.LoadInt64(&stats.n_lookups)
	m["n_casgets"] = atomic.LoadInt64(&stats.n_casgets)
	m["n_ranges"] = atomic.LoadInt64(&stats.n_ranges)
	m["n_activeiter"] = atomic.LoadInt64(&stats.n_activeiter)
	return m
}

// write op statistics.
func (stats *llrbstats) statswt(m map[string]interface{}) map[string]interface{} {
	m["n_count"] = stats.n_count
	m["n_inserts"] = stats.n_inserts
	m["n_updates"] = stats.n_updates
	m["n_deletes"] = stats.n_deletes
	m["n_nodes"] = stats.n_nodes
	m["n_frees"] = stats.n_frees
	m["n_clones"] = stats.n_clones
	return m
}

// mvcc statistics.
func (stats *llrbstats) statsmvcc(
	m map[string]interface{}) map[string]interface{} {

	m["mvcc.n_cclookups"] = stats.n_cclookups
	m["mvcc.n_ccranges"] = stats.n_ccranges
	m["mvcc.n_snapshots"] = atomic.LoadInt64(&stats.n_snapshots)
	m["mvcc.n_purgedss"] = atomic.LoadInt64(&stats.n_purgedss)
	m["mvcc.n_activess"] = atomic.LoadInt64(&stats.n_activess)
	m["mvcc.n_reclaims"] = stats.n_reclaims
	return m
}

//---- statistics relevant only on LLRB tree, not for snapshots.

// memory statistics for keys.
func (llrb *LLRB) statskey(m map[string]interface{}) map[string]interface{} {
	capacity, heap, alloc, overhead := llrb.nodearena.Info()
	m["keymemory"] = llrb.keymemory
	m["node.capacity"] = capacity
	m["node.heap"] = heap
	m["node.alloc"] = alloc
	m["node.overhead"] = overhead
	m["node.blocks"] = llrb.nodearena.Slabs()
	return m
}

// memory statistics for values.
func (llrb *LLRB) statsval(stats map[string]interface{}) map[string]interface{} {
	capacity, heap, alloc, overhead := llrb.valarena.Info()
	stats["valmemory"] = llrb.valmemory
	stats["value.capacity"] = capacity
	stats["value.heap"] = heap
	stats["value.alloc"] = alloc
	stats["value.overhead"] = overhead
	stats["value.blocks"] = llrb.valarena.Slabs()
	return stats
}

func (llrb *LLRB) log(what string, humanize bool) {
	var stats map[string]interface{}
	var err error

	switch what {
	case "full":
		startts := time.Now()
		stats, err = llrb.fullstats(llrb)
		if err != nil {
			panic(fmt.Errorf("log(): %v", err))
		}
		log.Infof("%v fullstats() took %v\n", llrb.logprefix, time.Since(startts))

	default:
		stats, err = llrb.stats(llrb)
		if err != nil {
			panic(fmt.Errorf("log(): %v", err))
		}
	}

	dohumanize := func(val interface{}) interface{} {
		if humanize {
			return gohumanize.Bytes(uint64(val.(int64)))
		}
		return val.(int64)
	}
	fullutilization := func(alloc, heap interface{}) int64 {
		uz := float64(alloc.(int64)) / float64(heap.(int64))
		return int64(uz * 100)
	}

	if humanize {
		// log information about key memory arena
		capac := dohumanize(stats["node.capacity"])
		overh := dohumanize(stats["node.overhead"])
		heap := dohumanize(stats["node.heap"])
		alloc := dohumanize(stats["node.alloc"])
		kmem := dohumanize(stats["keymemory"])
		fmsg := "%v keymem(%v): cap: %v {heap:%v,alloc:%v,overhd,%v}\n"
		log.Infof(fmsg, llrb.logprefix, kmem, capac, heap, alloc, overh)

		// log information about key memory utilization
		outs := []string{}
		fmsg = "  %4v chunk-size, utilz: %2.2f%%"
		sizes, zs := llrb.nodearena.Utilization()
		for i, size := range sizes {
			outs = append(outs, fmt.Sprintf(fmsg, size, zs[i]))
		}
		out := strings.Join(outs, "\n")
		uz := fullutilization(stats["node.alloc"], stats["node.heap"])
		log.Infof("%v key utilization: %v%%\n%v\n", llrb.logprefix, uz, out)

		// log information about value memory arena
		capac = dohumanize(stats["value.capacity"])
		overh = dohumanize(stats["value.overhead"])
		heap = dohumanize(stats["value.heap"])
		alloc = dohumanize(stats["value.alloc"])
		vmem := dohumanize(stats["valmemory"])
		fmsg = "%v valmem(%v): cap: %v {heap:%v,alloc:%v,overhd:%v}\n"
		log.Infof(fmsg, llrb.logprefix, vmem, capac, heap, alloc, overh)

		// log information about value utilization
		outs = []string{}
		fmsg = "  %4v chunk-size, utilz: %2.2f%%"
		sizes, zs = llrb.valarena.Utilization()
		for i, size := range sizes {
			outs = append(outs, fmt.Sprintf(fmsg, size, zs[i]))
		}
		out = strings.Join(outs, "\n")
		uz = fullutilization(stats["value.alloc"], stats["value.heap"])
		log.Infof("%v value utilization: %v%%\n%v\n", llrb.logprefix, uz, out)
	}

	// log statistics
	m := llrb.statskey(map[string]interface{}{})
	delete(m, "node.blocks")
	text := lib.Prettystats(m, false)
	log.Infof("%v keystats %v\n", llrb.logprefix, string(text))
	m = llrb.statsval(map[string]interface{}{})
	delete(m, "value.blocks")
	text = lib.Prettystats(m, false)
	log.Infof("%v valstats %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statswt(map[string]interface{}{}), false)
	log.Infof("%v writestats %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statsmvcc(map[string]interface{}{}), false)
	log.Infof("%v mvcc %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statsrd(map[string]interface{}{}), false)
	log.Infof("%v readstats %v\n", llrb.logprefix, string(text))
	// log histograms
	text = llrb.h_upsertdepth.Logstring()
	log.Infof("%v h_upsertdepth %v\n", llrb.logprefix, text)
	if llrb.mvcc.enabled {
		text = llrb.mvcc.h_bulkfree.Logstring()
		log.Infof("%v h_bulkfree %v\n", llrb.logprefix, text)
		text = llrb.mvcc.h_versions.Logstring()
		log.Infof("%v h_versions %v\n", llrb.logprefix, text)
		// log reclaim histograms
		keys := []string{
			"upsert", "upsertcas", "delmin", "delmax", "delete", "mutations",
		}
		for _, key := range keys {
			if h := llrb.mvcc.h_reclaims[key]; h.Samples() > 0 {
				text = h.Logstring()
				log.Infof("%v h_reclaims.%v %v\n", llrb.logprefix, key, text)
			}
		}
	}

	// log snapshot chain
	if llrb.mvcc.enabled {
		chain := []string{"root"}
		snapshot := llrb.mvcc.snapshot
		if snapshot != nil {
			chain = append(chain, snapshot.ID())
			snapshot = snapshot.next
		}
		fmsg := "%v snapshot chain %v\n"
		log.Infof(fmsg, llrb.logprefix, strings.Join(chain, "->"))
	}
}

// costly operation, don't call this on active trees.
func treestats(nd *Llrbnode, d int64, h *lib.HistogramInt64, c int64) int64 {
	if nd != nil {
		h.Add(d)
		if !isred(nd) {
			c++
		}
		x := treestats(nd.left, d+1, h, c)
		y := treestats(nd.right, d+1, h, c)
		if x != y {
			fmsg := "invariant failed, no. of blacks : {%v,%v}"
			panic(fmt.Errorf(fmsg, x, y))
		}
		return x
	}
	return c
}
