package storage

import "sort"
import "fmt"
import "strings"

import humanize "github.com/dustin/go-humanize"

func (llrb *LLRB) stats(involved int) (map[string]interface{}, error) {
	stats := llrb.statsmem(map[string]interface{}{})
	stats = llrb.statsupsertdepth(stats)
	stats = llrb.statsheight(involved, stats)
	if involved >= 9 {
		stats["llrb.n_blacks"] = llrb.countblacks(llrb.root, 0)
	}
	return stats, nil
}

// memory statistics -
//	   node-arena, value.arena, total-keysize, total-valuesize
func (llrb *LLRB) statsmem(stats map[string]interface{}) map[string]interface{} {
	overhead, useful := llrb.nodearena.memory()
	stats["llrb.node.overhead"] = overhead
	stats["llrb.node.useful"] = useful
	stats["llrb.node.allocated"] = llrb.nodearena.allocated()
	stats["llrb.node.available"] = llrb.nodearena.available()
	stats["llrb.node.blocks"] = llrb.nodearena.blocksizes
	overhead, useful = llrb.valarena.memory()
	stats["llrb.value.overhead"] = overhead
	stats["llrb.value.useful"] = useful
	stats["llrb.value.allocated"] = llrb.valarena.allocated()
	stats["llrb.value.available"] = llrb.valarena.available()
	stats["llrb.value.blocks"] = llrb.valarena.blocksizes
	stats["llrb.keymemory"] = llrb.keymemory
	stats["llrb.valmemory"] = llrb.valmemory

	return stats
}

func (llrb *LLRB) statsupsertdepth(
	stats map[string]interface{}) map[string]interface{} {

	stats["llrb.upsertdepth.samples"] = llrb.upsertdepth.samples()
	stats["llrb.upsertdepth.min"] = llrb.upsertdepth.min()
	stats["llrb.upsertdepth.max"] = llrb.upsertdepth.max()
	stats["llrb.upsertdepth.mean"] = llrb.upsertdepth.mean()
	stats["llrb.upsertdepth.variance"] = llrb.upsertdepth.variance()
	stats["llrb.upsertdepth.stddeviance"] = llrb.upsertdepth.sd()
	return stats
}

func (llrb *LLRB) statsheight(
	involved int, stats map[string]interface{}) map[string]interface{} {

	// walk through the entire tree to compute the average height of the tree
	stats["llrb.height.samples"] = -1
	stats["llrb.height.min"] = -1
	stats["llrb.height.max"] = -1
	stats["llrb.height.mean"] = -1
	stats["llrb.height.variance"] = -1
	stats["llrb.height.stddeviance"] = -1
	if involved >= 9 {
		heightav := &averageInt{}
		llrb.heightStats(llrb.root, 0, heightav)
		stats["llrb.height.samples"] = heightav.samples()
		stats["llrb.height.min"] = heightav.min()
		stats["llrb.height.max"] = heightav.max()
		stats["llrb.height.mean"] = heightav.mean()
		stats["llrb.height.variance"] = heightav.variance()
		stats["llrb.height.stddeviance"] = heightav.sd()
	}
	return stats
}

func (llrb *LLRB) log(involved int) {
	// node memory
	stats, err := llrb.stats(involved)
	if err != nil {
		panic(err)
	}
	overh := humanize.Bytes(uint64(stats["llrb.node.overhead"].(int64)))
	use := humanize.Bytes(uint64(stats["llrb.node.useful"].(int64)))
	alloc := humanize.Bytes(uint64(stats["llrb.node.allocated"].(int64)))
	avail := humanize.Bytes(uint64(stats["llrb.node.available"].(int64)))
	kmem := stats["llrb.keymemory"]
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
	overh = humanize.Bytes(uint64(stats["llrb.value.overhead"].(int64)))
	use = humanize.Bytes(uint64(stats["llrb.value.useful"].(int64)))
	alloc = humanize.Bytes(uint64(stats["llrb.value.allocated"].(int64)))
	avail = humanize.Bytes(uint64(stats["llrb.value.available"].(int64)))
	vmem := humanize.Bytes(uint64(stats["llrb.valmemory"].(int64)))
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

	// log upsert depth
	samples := stats["llrb.upsertdepth.samples"].(int64)
	min := stats["llrb.upsertdepth.min"].(int64)
	max := stats["llrb.upsertdepth.max"].(int64)
	mean := stats["llrb.upsertdepth.mean"]
	varn := stats["llrb.upsertdepth.variance"]
	sd := stats["llrb.upsertdepth.stddeviance"]
	fmsg = "%v average upsertdepth\n" +
		"  samples %v : <%v to %v> mean %v  varn %2.2f  sd %2.2f\n"
	log.Infof(fmsg, llrb.logPrefix, samples, min, max, mean, varn, sd)

	// log height
	samples = stats["llrb.height.samples"].(int64)
	min = stats["llrb.height.min"].(int64)
	max = stats["llrb.height.max"].(int64)
	mean = stats["llrb.height.mean"]
	varn = stats["llrb.height.variance"]
	sd = stats["llrb.height.stddeviance"]
	fmsg = "%v average heightstats\n" +
		"  samples %v : <%v to %v> mean %v  varn %2.2f  sd %2.2f\n"
	log.Infof(fmsg, llrb.logPrefix, samples, min, max, mean, varn, sd)
}
