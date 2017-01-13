package llrb

import "fmt"
import "math"
import "time"
import "strings"
import "sync/atomic"

import gohumanize "github.com/dustin/go-humanize"
import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/log"

type llrbstats struct {
	// 64-bit aligned reader statistics
	n_lookups int64
	n_casgets int64
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
}

type mvccstats struct {
	n_snapshots int64
	n_purgedss  int64
	n_activess  int64
	n_cclookups int64
	n_ccranges  int64
	n_reclaims  int64
}

func (llrb *LLRB) stats() (map[string]interface{}, error) {
	stats := llrb.statsval(llrb.statskey(map[string]interface{}{}))
	stats = llrb.statsrd(llrb.statsmvcc(llrb.statswt(stats)))
	stats["h_upsertdepth"] = llrb.h_upsertdepth.Fullstats()
	if llrb.mvcc.enabled {
		stats["mvcc.h_bulkfree"] = llrb.mvcc.h_bulkfree.Fullstats()
		stats["mvcc.h_versions"] = llrb.mvcc.h_versions.Fullstats()
		for k, h := range llrb.mvcc.h_reclaims {
			stats["mvcc.h_reclaims."+k] = h.Fullstats()
		}
	}
	return stats, nil
}

func (llrb *LLRB) fullstats() (map[string]interface{}, error) {
	stats, err := llrb.stats()
	if err != nil {
		return nil, err
	}

	h_heightav := lib.NewhistorgramInt64(1, 256, 1)
	llrb.heightStats(llrb.root, 1 /*depth*/, h_heightav)
	stats["h_height"] = h_heightav.Fullstats()
	stats["n_blacks"] = llrb.countblacks(llrb.root, 0)

	h_height := stats["h_height"].(map[string]interface{})
	if x := h_height["samples"].(int64); x != llrb.Count() {
		fmsg := "expected h_height.samples:%v to be same as llrb.Count():%v"
		panic(fmt.Errorf(fmsg, x, llrb.Count()))
	}
	return stats, nil
}

// memory statistics for keys: node-arena, total-keysize
func (llrb *LLRB) statskey(stats map[string]interface{}) map[string]interface{} {
	overhead, useful := llrb.nodearena.Memory()
	stats["keymemory"] = llrb.keymemory
	stats["node.overhead"] = overhead
	stats["node.useful"] = useful
	stats["node.allocated"] = llrb.nodearena.Allocated()
	stats["node.available"] = llrb.nodearena.Available()
	stats["node.blocks"] = llrb.nodearena.Chunksizes()
	return stats
}

// memory statistics for keys: value.arena, total-valuesize
func (llrb *LLRB) statsval(stats map[string]interface{}) map[string]interface{} {
	overhead, useful := llrb.valarena.Memory()
	stats["valmemory"] = llrb.valmemory
	stats["value.overhead"] = overhead
	stats["value.useful"] = useful
	stats["value.allocated"] = llrb.valarena.Allocated()
	stats["value.available"] = llrb.valarena.Available()
	stats["value.blocks"] = llrb.valarena.Chunksizes()
	return stats
}

func (llrb *LLRB) statsrd(stats map[string]interface{}) map[string]interface{} {
	stats["n_lookups"] = llrb.n_lookups
	stats["n_casgets"] = llrb.n_casgets
	stats["n_ranges"] = llrb.n_ranges
	stats["n_activeiter"] = llrb.n_activeiter
	return stats
}

func (llrb *LLRB) statswt(stats map[string]interface{}) map[string]interface{} {
	stats["n_count"] = llrb.n_count
	stats["n_inserts"] = llrb.n_inserts
	stats["n_updates"] = llrb.n_updates
	stats["n_deletes"] = llrb.n_deletes
	stats["n_nodes"] = llrb.n_nodes
	stats["n_frees"] = llrb.n_frees
	stats["n_clones"] = llrb.n_clones
	return stats
}

func (llrb *LLRB) statsmvcc(stats map[string]interface{}) map[string]interface{} {
	stats["mvcc.n_snapshots"] = llrb.mvcc.n_snapshots
	stats["mvcc.n_purgedss"] = llrb.mvcc.n_purgedss
	stats["mvcc.n_activess"] = atomic.LoadInt64(&llrb.mvcc.n_activess)
	stats["mvcc.n_cclookups"] = llrb.mvcc.n_cclookups
	stats["mvcc.n_ccranges"] = llrb.mvcc.n_ccranges
	stats["mvcc.n_reclaims"] = llrb.mvcc.n_reclaims
	return stats
}

func (llrb *LLRB) validatestats() error {
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
	// n_deletes should match (n_frees - n_clones)
	n_frees, n_clones := llrb.n_frees, llrb.n_clones
	if n_deletes != (n_frees - n_clones) {
		fmsg := "validatestats(): n_deletes:%v != (n_frees:%v + n_clones:%v)"
		panic(fmt.Errorf(fmsg, n_deletes, n_frees, n_clones))
	}
	// mvcc.n_snapshots should match (mvcc.n_activess + mvcc.n_purgedss)
	n_snapshots := llrb.mvcc.n_snapshots
	n_purgedss := llrb.mvcc.n_purgedss
	n_activess := atomic.LoadInt64(&llrb.mvcc.n_activess)
	if n_snapshots != (n_purgedss + n_activess) {
		fmsg := "validatestats(): " +
			"n_snapshots:%v != (n_activess:%v + n_purgedss:%v)"
		panic(fmt.Errorf(fmsg, n_snapshots, n_activess, n_purgedss))
	}

	for k, h_reclaim := range llrb.mvcc.h_reclaims {
		if max := h_reclaim.Max(); max > 0 {
			nf := float64(llrb.Count())
			if float64(max) > (4 * math.Log2(nf)) {
				fmsg := "validatestats(): max %v reclaim %v exceeds log2(%v)"
				panic(fmt.Errorf(fmsg, k, float64(max), nf))
			}
		}
	}

	return nil
}

func (llrb *LLRB) log(involved string, humanize bool) {
	var stats map[string]interface{}
	var err error

	switch involved {
	case "full":
		startts := time.Now()
		stats, err = llrb.fullstats()
		if err != nil {
			panic(fmt.Errorf("log(): %v", err))
		}
		log.Infof("%v fullstats() took %v\n", llrb.logprefix, time.Since(startts))

	default:
		stats, err = llrb.stats()
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

	if humanize {
		overh := dohumanize(stats["node.overhead"])
		use := dohumanize(stats["node.useful"])
		alloc := dohumanize(stats["node.allocated"])
		avail := dohumanize(stats["node.available"])
		kmem := dohumanize(stats["keymemory"])
		fmsg := "%v keymem(%v): avail %v {allocated:%v,useful:%v,overhd,%v}\n"
		log.Infof(fmsg, llrb.logprefix, kmem, avail, alloc, use, overh)

		// node utilization
		outs := []string{}
		fmsg = "  %4v chunk-size, utilz: %2.2f%%"
		sizes, zs := llrb.nodearena.Utilization()
		for i, size := range sizes {
			outs = append(outs, fmt.Sprintf(fmsg, size, zs[i]))
		}
		out := strings.Join(outs, "\n")
		log.Infof("%v key utilization:\n%v\n", llrb.logprefix, out)

		// value memory
		overh = dohumanize(stats["value.overhead"])
		use = dohumanize(stats["value.useful"])
		alloc = dohumanize(stats["value.allocated"])
		avail = dohumanize(stats["value.available"])
		vmem := dohumanize(stats["valmemory"])
		fmsg = "%v valmem(%v): avail %v {allocated:%v,useful:%v,overhd:%v}\n"
		log.Infof(fmsg, llrb.logprefix, vmem, avail, alloc, use, overh)

		// value utilization
		outs = []string{}
		fmsg = "  %4v chunk-size, utilz: %2.2f%%"
		sizes, zs = llrb.valarena.Utilization()
		for i, size := range sizes {
			outs = append(outs, fmt.Sprintf(fmsg, size, zs[i]))
		}
		out = strings.Join(outs, "\n")
		log.Infof("%v value utilization:\n%v\n", llrb.logprefix, out)
	}

	// log statistics
	text := lib.Prettystats(llrb.statskey(map[string]interface{}{}), false)
	log.Infof("%v keystats %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statsval(map[string]interface{}{}), false)
	log.Infof("%v valstats %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statswt(map[string]interface{}{}), false)
	log.Infof("%v writestats %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statsmvcc(map[string]interface{}{}), false)
	log.Infof("%v mvcc %v\n", llrb.logprefix, string(text))
	text = lib.Prettystats(llrb.statsrd(map[string]interface{}{}), false)
	log.Infof("%v readstats %v\n", llrb.logprefix, string(text))
	// log histograms
	h_upsertdepth := stats["h_upsertdepth"].(map[string]interface{})
	text = lib.Prettystats(h_upsertdepth, false)
	log.Infof("%v h_upsertdepth %v\n", llrb.logprefix, string(text))
	h_bulkfree := stats["mvcc.h_bulkfree"].(map[string]interface{})
	text = lib.Prettystats(h_bulkfree, false)
	log.Infof("%v mvcc.h_bulkfree %v\n", llrb.logprefix, string(text))
	h_versions := stats["mvcc.h_versions"].(map[string]interface{})
	text = lib.Prettystats(h_versions, false)
	log.Infof("%v mvcc.h_versions %v\n", llrb.logprefix, string(text))
	if h_height, ok := stats["h_height"].(map[string]interface{}); ok {
		text = lib.Prettystats(h_height, false)
		log.Infof("%v h_versions %v\n", llrb.logprefix, string(text))
	}
	if n_blacks, ok := stats["n_blacks"].(map[string]interface{}); ok {
		text = lib.Prettystats(n_blacks, false)
		log.Infof("%v n_blacks %v\n", llrb.logprefix, string(text))
	}
	// log reclaim histograms
	h_reclaim_upsert, ok :=
		stats["mvcc.h_reclaims.upsert"].(map[string]interface{})
	if ok {
		text = lib.Prettystats(h_reclaim_upsert, false)
		log.Infof("%v mvcc.h_reclaims.upsert %v\n", llrb.logprefix, string(text))
	}
	h_reclaim_upsertcas, ok :=
		stats["mvcc.h_reclaims.upsertcase"].(map[string]interface{})
	if ok {
		text = lib.Prettystats(h_reclaim_upsertcas, false)
		fmsg := "%v mvcc.h_reclaims.upsertcas %v\n"
		log.Infof(fmsg, llrb.logprefix, string(text))
	}
	h_reclaim_delmin, ok :=
		stats["mvcc.h_reclaims.delmin"].(map[string]interface{})
	if ok {
		text = lib.Prettystats(h_reclaim_delmin, false)
		log.Infof("%v mvcc.h_reclaims.delmin %v\n", llrb.logprefix, string(text))
	}
	h_reclaim_delmax, ok :=
		stats["mvcc.h_reclaims.delmax"].(map[string]interface{})
	if ok {
		text = lib.Prettystats(h_reclaim_delmax, false)
		log.Infof("%v mvcc.h_reclaims.delmax %v\n", llrb.logprefix, string(text))
	}
	h_reclaim_delete, ok :=
		stats["mvcc.h_reclaims.delete"].(map[string]interface{})
	if ok {
		text = lib.Prettystats(h_reclaim_delete, false)
		log.Infof("%v mvcc.h_reclaims.delete %v\n", llrb.logprefix, string(text))
	}
	h_reclaim_muts, ok :=
		stats["mvcc.h_reclaims.mutations"].(map[string]interface{})
	if ok {
		text = lib.Prettystats(h_reclaim_muts, false)
		fmsg := "%v mvcc.h_reclaims.mutations %v\n"
		log.Infof(fmsg, llrb.logprefix, string(text))
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
