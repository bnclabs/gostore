package llrb

import "github.com/prataprc/gostore/malloc"
import s "github.com/prataprc/gosettings"
import "github.com/cloudfoundry/gosigar"

// Defaultsettings for llrb instance along with node arena and value arena.
//
// "iterpool.size": int64(100)
//      Maximum number of active iterators. Each Iterate call will acquire
//      an instance of iterator.
//
// "lsm": false
//      Enable Log-Structured-Merge feature.
//
// "keycapacity" (int64)
//		Memory capacity required for keys. Default will be
//		(avgkeysize / (avgkeysize+avgvalsize)) * freeRAM
//
// "valcapacity" (int64)
//		Memory capacity required for values. Default will be
//		(avgvalsize / (avgkeysize+avgvalsize)) * freeRAM
//
// "maxlimit" (int64, default: 100)
//      Applicable for Iterate API. Iterate uses Range to fetch
//      a batch of entries to iterate upon. Limit the batch size
//      if number iterations are known apriori.
//
// "metadata.bornseqno" (bool, default: false)
//		If true, use metadata field to book-keep node's born
//		sequence number.
//
// "metadata.deadseqno": (bool,  default: false)
//		If true, use metadata field to book-keep node's dead
//		sequence number.
//
// "metadata.mvalue": (bool, default: true)
//		Store value pointer, as metadata, for each entry. If
//		entries don't have value, mark this as false.
//
// "metadata.vbuuid": (bool, default: false)
//		If true, use metadata field to book-keep node's vbuuid.
//
// "metadata.fpos": (bool, default: false)
//		If true, use file position in disk files where value is
//		stored.
//
// "mvcc.enable" (bool, default: false)
//      Manage LLRB as Multi-Version-Concurrency-Control tree.
//      Write intensive applications can leave this as false,
//      while read intensive applications can set this as true.
//
// "mvcc.snapshot.tick" (int64, default: 5)
//		Time period, in millisecond, for generating read-snapshots.
//
// "mvcc.writer.chansize" (int64, default: 1000)
//      Buffered channel's size, if "mvcc.enable" is true, to batch
//      write operations.
//
func Defaultsettings() s.Settings {
	ramsize, _, _ := getsysmem()
	setts := s.Settings{
		"keycapacity":         ramsize,
		"valcapacity":         ramsize,
		"nodearena.allocator": "flist", // just a place holder
	}

	nodesetts := malloc.Defaultsettings()
	nodesetts = nodesetts.AddPrefix("nodearena.")
	valsetts := malloc.Defaultsettings()
	valsetts = valsetts.AddPrefix("valarena.")
	setts = setts.Mixin(nodesetts, valsetts)
	return setts
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
