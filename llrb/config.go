package llrb

import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/malloc"
import s "github.com/prataprc/gosettings"

// Defaultsettings for llrb instance along with node arena and value arena.
//
// Configuration parameters:
//
// "iterpool.size":        int64(100),
//		maximum number of active iterators.
//
// "lsm":                  false,
//		handle deletes using log-structured-merge.
//
// "memutilization":       float64(0.4),
//		allowable memory utilization.
//
// "minkeysize" (int64, default: <api.MinKeysize>),
//		minimum size allowed for key.
//
// "maxkeysize" (int64, default: <api.MaxKeysize>),
//		maximum size allowed for key.
//
// "minvalsize" (int64, default: <api.MinValsize>),
//		minimum size allowed for value, valid only if
//		"metadata.mvalue" is true.
//
// "maxvalsize" (int64, default: <api.MaxValsize>),
//		maximum size allowed for value, valid only if
//		"metadata.mvalue" is true.
//
// "maxlimit" (int64, default: 100),
//		limit number of entries to batch read during iteration.
//
// "metadata.bornseqno" (bool, default: false),
//		if true, use metadata field to book-keep node's born
//		sequence number.
//
// "metadata.deadseqno": (bool,  default: false),
//		if true, use metadata field to book-keep node's dead
//		sequence number.
//
// "metadata.mvalue": (bool, default: true),
//		store value pointer, as metadata, for each entry, passing this as
//		false.
//
// "metadata.vbuuid": (bool, default: false),
//		if true, use metadata field to book-keep node's vbuuid.
//
// "metadata.fpos": (bool, default: false),
//		if true, use file position in disk files where value is
//		stored.
//
// "mvcc.enable" (bool, default: false),
//		manage LLRB as Multi-Version-Concurrency-Control tree.
//
// "mvcc.snapshot.tick" (int64, default: 5),
//		time period, in millisecond, for generating read-snapshots.
//
// "mvcc.writer.chansize" (int64, default: 1000),
//		buffered channel's size, if "mvcc.enable" is true, to batch write
//		operations.
//
func Defaultsettings() s.Settings {
	setts := s.Settings{
		"iterpool.size":        int64(100),
		"lsm":                  false,
		"memutilization":       float64(0.4),
		"minkeysize":           api.MinKeysize,
		"maxkeysize":           api.MaxKeysize,
		"minvalsize":           api.MinValsize,
		"maxvalsize":           api.MaxValsize,
		"maxlimit":             int64(100),
		"metadata.bornseqno":   false,
		"metadata.deadseqno":   false,
		"metadata.mvalue":      true,
		"metadata.vbuuid":      false,
		"metadata.fpos":        false,
		"mvcc.enable":          false,
		"mvcc.snapshot.tick":   int64(5), // 5 millisecond
		"mvcc.writer.chansize": int64(1000),
	}
	nodesetts := malloc.Defaultsettings(api.MinKeysize, api.MaxKeysize)
	nodesetts = nodesetts.AddPrefix("nodearena.")
	valsetts := malloc.Defaultsettings(api.MinValsize, api.MaxValsize)
	valsetts = valsetts.AddPrefix("valarena.")
	setts = setts.Mixin(nodesetts, valsetts)
	return setts
}
