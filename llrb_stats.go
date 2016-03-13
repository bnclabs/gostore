package storage

import "sort"
import "fmt"
import "strings"
import "encoding/json"

import gohumanize "github.com/dustin/go-humanize"

func (llrb *LLRB) stats(involved int) (map[string]interface{}, error) {
	stats := llrb.statsmem(map[string]interface{}{})
	stats = llrb.stattree(stats)
	stats = llrb.h_upsertdepth.fullstats(stats, "upsertdepth.")
	if llrb.mvcc.enabled {
		stats = llrb.mvcc.h_bulkfree.fullstats(stats, "mvcc.h_bulkfree.")
		for k, h := range llrb.mvcc.h_reclaims {
			stats = h.fullstats(stats, "mvcc.h_reclaims."+k+".")
		}
	}
	if involved >= 9 {
		h_heightav := newhistorgramInt64(1, 256, 1)
		llrb.heightStats(llrb.root, 1 /*depth*/, h_heightav)
		stats = h_heightav.fullstats(stats, "h_height.")

		stats["n_blacks"] = llrb.countblacks(llrb.root, 0)
	}
	return stats, nil
}

// memory statistics -
//	   node-arena, value.arena, total-keysize, total-valuesize
func (llrb *LLRB) statsmem(stats map[string]interface{}) map[string]interface{} {
	overhead, useful := llrb.nodearena.memory()
	stats["node.overhead"] = overhead
	stats["node.useful"] = useful
	stats["node.allocated"] = llrb.nodearena.allocated()
	stats["node.available"] = llrb.nodearena.available()
	stats["node.blocks"] = llrb.nodearena.blocksizes
	overhead, useful = llrb.valarena.memory()
	stats["value.overhead"] = overhead
	stats["value.useful"] = useful
	stats["value.allocated"] = llrb.valarena.allocated()
	stats["value.available"] = llrb.valarena.available()
	stats["value.blocks"] = llrb.valarena.blocksizes
	stats["keymemory"] = llrb.keymemory
	stats["valmemory"] = llrb.valmemory

	return stats
}

// tree statistics -
func (llrb *LLRB) stattree(stats map[string]interface{}) map[string]interface{} {
	stats["n_count"] = llrb.n_count
	stats["n_lookups"] = llrb.n_lookups
	stats["n_ranges"] = llrb.n_ranges
	stats["n_inserts"] = llrb.n_inserts
	stats["n_updates"] = llrb.n_updates
	stats["n_deletes"] = llrb.n_deletes
	stats["n_allocs"] = llrb.n_allocs
	stats["n_frees"] = llrb.n_frees
	stats["n_clones"] = llrb.n_clones
	stats["keymemory"] = llrb.keymemory
	stats["valmemory"] = llrb.valmemory
	stats["mvcc.n_snapshots"] = llrb.mvcc.n_snapshots
	stats["mvcc.n_purgedss"] = llrb.mvcc.n_purgedss
	return stats
}

func (llrb *LLRB) log(involved int, humanize bool) {
	// node memory
	stats, err := llrb.stats(involved)
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
		log.Infof(fmsg, llrb.logPrefix, kmem, use, overh, alloc, avail)

		// node utilization
		arenapools := llrb.nodearena.mpools
		sizes := []int{}
		for size := range arenapools {
			sizes = append(sizes, int(size))
		}
		sort.Ints(sizes)
		outs := []string{}
		fmsg = "  %4v blocks %3v pools of %v each, utilz: %2.2f%%"
		for _, size := range sizes {
			mpools := arenapools[int64(size)]
			allocated, capct := int64(0), int64(0)
			if len(mpools) > 0 {
				for _, mpool := range mpools {
					allocated += mpool.allocated()
					capct += mpool.capacity
				}
				z := (float64(allocated) / float64(capct)) * 100
				outs = append(outs, fmt.Sprintf(fmsg, size, len(mpools), capct, z))
			}
		}
		out := strings.Join(outs, "\n")
		log.Infof("%v key utilization:\n%v\n", llrb.logPrefix, out)

		// value memory
		overh = dohumanize(stats["value.overhead"])
		use = dohumanize(stats["value.useful"])
		alloc = dohumanize(stats["value.allocated"])
		avail = dohumanize(stats["value.available"])
		vmem := dohumanize(stats["valmemory"])
		fmsg = "%v valmem(%v): %v useful, overhd %v allocated %v avail %v\n"
		log.Infof(fmsg, llrb.logPrefix, vmem, use, overh, alloc, avail)

		// value utilization
		arenapools = llrb.valarena.mpools
		sizes = []int{}
		for size := range arenapools {
			sizes = append(sizes, int(size))
		}
		sort.Ints(sizes)
		outs = []string{}
		fmsg = "  %4v blocks %3v pools of %v each, utilz: %2.2f%%"
		for _, size := range sizes {
			mpools := arenapools[int64(size)]
			allocated, capct := int64(0), int64(0)
			if len(mpools) > 0 {
				for _, mpool := range mpools {
					allocated += mpool.allocated()
					capct += mpool.capacity
				}
				z := (float64(allocated) / float64(capct)) * 100
				outs = append(outs, fmt.Sprintf(fmsg, size, len(mpools), capct, z))
			}
		}
		out = strings.Join(outs, "\n")
		log.Infof("%v value utilization:\n%v\n", llrb.logPrefix, out)
	}

	text, err := json.Marshal(stats)
	if err != nil {
		panic(fmt.Errorf("log(): %v", err))
	}
	log.Infof("%v stats %v\n", llrb.logPrefix, string(text))
}
