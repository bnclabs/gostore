package llrb

import "github.com/prataprc/gostore/api"
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
// "minkeysize" (int64, default: <api.MinKeysize>),
//		Minimum size allowed for key.
//
// "maxkeysize" (int64, default: <api.MaxKeysize>),
//		Maximum size allowed for key.
//
// "minvalsize" (int64, default: <api.MinValsize>),
//		Minimum size allowed for value, valid only if
//		"metadata.mvalue" is true.
//
// "maxvalsize" (int64, default: <api.MaxValsize>),
//		Maximum size allowed for value, valid only if
//		"metadata.mvalue" is true.
//
// "keycapacity" (int64)
//		Memory capacity required for keys. Default will be
//		(avgkeysize / (avgkeysize+avgvalsize)) * freeRAM
//
// "valcapacity" (int64)
//		Memory capacity required for values. Default will be
//		(avgvalsize / (avgkeysize+avgvalsize)) * freeRAM
//
// "maxlimit" (int64, default: 100),
//		Applicable for Iterate() API. Iterate uses Range to fetch
//      a batch of entries to iterate upon. Limit the batch size
//		if number iterations are known apriori.
//
// "metadata.bornseqno" (bool, default: false),
//		If true, use metadata field to book-keep node's born
//		sequence number.
//
// "metadata.deadseqno": (bool,  default: false),
//		If true, use metadata field to book-keep node's dead
//		sequence number.
//
// "metadata.mvalue": (bool, default: true),
//		Store value pointer, as metadata, for each entry. If
//		entries don't have value, mark this as false.
//
// "metadata.vbuuid": (bool, default: false),
//		If true, use metadata field to book-keep node's vbuuid.
//
// "metadata.fpos": (bool, default: false),
//		If true, use file position in disk files where value is
//		stored.
//
// "mvcc.enable" (bool, default: false),
//		Manage LLRB as Multi-Version-Concurrency-Control tree.
//		Write intensive applications can leave this as false,
//      while read intensive applications can set this as true.
//
// "mvcc.snapshot.tick" (int64, default: 5),
//		Time period, in millisecond, for generating read-snapshots.
//
// "mvcc.writer.chansize" (int64, default: 1000),
//		Buffered channel's size, if "mvcc.enable" is true, to batch
//      write operations.
//
func Defaultsettings() s.Settings {
	_, _, free := getsysmem()
	avgksize := float64(api.MinKeysize + (api.MaxKeysize-api.MinKeysize)/2)
	avgvsize := float64(api.MinValsize + (api.MaxValsize-api.MinValsize)/2)
	keycapacity := (avgksize / (avgksize + avgvsize)) * float64(free)
	valcapacity := (avgvsize / (avgksize + avgvsize)) * float64(free)
	setts := s.Settings{
		"iterpool.size":        int64(100),
		"lsm":                  false,
		"minkeysize":           api.MinKeysize,
		"maxkeysize":           api.MaxKeysize,
		"minvalsize":           api.MinValsize,
		"maxvalsize":           api.MaxValsize,
		"keycapacity":          int64(keycapacity),
		"valcapacity":          int64(valcapacity),
		"maxlimit":             int64(100),
		"metadata.bornseqno":   false,
		"metadata.deadseqno":   false,
		"metadata.mvalue":      true,
		"metadata.vbuuid":      false,
		"metadata.fpos":        false,
		"mvcc.enable":          false,
		"mvcc.snapshot.tick":   int64(5), // 5 millisecond
		"mvcc.writer.chansize": int64(1000),
		"nodearena.allocator":  "flist", // just a place holder
	}

	minkeysize := adjustkeysize(api.MinKeysize, setts)
	maxkeysize := adjustkeysize(api.MaxKeysize, setts)
	minvalsize := adjustvalsize(api.MinValsize)
	maxvalsize := adjustvalsize(api.MaxValsize)

	nodesetts := malloc.Defaultsettings(minkeysize, maxkeysize)
	nodesetts = nodesetts.AddPrefix("nodearena.")
	valsetts := malloc.Defaultsettings(minvalsize, maxvalsize)
	valsetts = valsetts.AddPrefix("valarena.")
	setts = setts.Mixin(nodesetts, valsetts)
	return setts
}

func getsysmem() (total, used, free uint64) {
	mem := sigar.Mem{}
	mem.Get()
	return mem.Total, mem.Used, mem.Free
}
