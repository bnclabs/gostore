package llrb

import "fmt"
import "math"
import "strings"
import "encoding/json"

import gohumanize "github.com/dustin/go-humanize"
import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/log"

func (llrb *LLRB) stats() (map[string]interface{}, error) {
	stats := llrb.statsmem(map[string]interface{}{})
	stats = llrb.stattree(stats)
	stats["h_upsertdepth"] = llrb.h_upsertdepth.Fullstats()
	if llrb.mvcc.enabled {
		stats["mvcc.h_bulkfree"] = llrb.mvcc.h_bulkfree.Fullstats()
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

// memory statistics -
//	   node-arena, value.arena, total-keysize, total-valuesize
func (llrb *LLRB) statsmem(stats map[string]interface{}) map[string]interface{} {
	overhead, useful := llrb.nodearena.Memory()
	stats["node.overhead"] = overhead
	stats["node.useful"] = useful
	stats["node.allocated"] = llrb.nodearena.Allocated()
	stats["node.available"] = llrb.nodearena.Available()
	stats["node.blocks"] = llrb.nodearena.Chunksizes()
	overhead, useful = llrb.valarena.Memory()
	stats["value.overhead"] = overhead
	stats["value.useful"] = useful
	stats["value.allocated"] = llrb.valarena.Allocated()
	stats["value.available"] = llrb.valarena.Available()
	stats["value.blocks"] = llrb.valarena.Chunksizes()
	stats["keymemory"] = llrb.keymemory
	stats["valmemory"] = llrb.valmemory

	return stats
}

// tree statistics -
func (llrb *LLRB) stattree(stats map[string]interface{}) map[string]interface{} {
	stats["n_count"] = llrb.n_count
	stats["n_lookups"] = llrb.n_lookups
	stats["n_casgets"] = llrb.n_casgets
	stats["n_ranges"] = llrb.n_ranges
	stats["n_inserts"] = llrb.n_inserts
	stats["n_updates"] = llrb.n_updates
	stats["n_deletes"] = llrb.n_deletes
	stats["n_nodes"] = llrb.n_nodes
	stats["n_frees"] = llrb.n_frees
	stats["n_clones"] = llrb.n_clones
	stats["keymemory"] = llrb.keymemory
	stats["valmemory"] = llrb.valmemory
	stats["mvcc.n_snapshots"] = llrb.mvcc.n_snapshots
	stats["mvcc.n_purgedss"] = llrb.mvcc.n_purgedss
	stats["mvcc.n_activess"] = llrb.mvcc.n_activess
	stats["mvcc.n_cclookups"] = llrb.mvcc.n_cclookups
	stats["mvcc.n_ccranges"] = llrb.mvcc.n_ccranges
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
	n_purgedss, n_activess := llrb.mvcc.n_purgedss, llrb.mvcc.n_activess
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

func (llrb *LLRB) log(involved int, humanize bool) {
	stats, err := llrb.fullstats() // llrb.stats(involved)
	if err != nil {
		panic(fmt.Errorf("log(): %v", err))
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
		fmsg := "%v keymem(%v): %v useful, overhd %v allocated %v avail %v\n"
		log.Infof(fmsg, llrb.logprefix, kmem, use, overh, alloc, avail)

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
		fmsg = "%v valmem(%v): %v useful, overhd %v allocated %v avail %v\n"
		log.Infof(fmsg, llrb.logprefix, vmem, use, overh, alloc, avail)

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
	text, err := json.Marshal(stats)
	if err != nil {
		panic(fmt.Errorf("log(): %v", err))
	}
	log.Infof("%v stats %v\n", llrb.logprefix, string(text))

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
