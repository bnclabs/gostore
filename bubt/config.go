package bubt

import s "github.com/prataprc/gosettings"

// Defaultsettings for bubt instance.
//
// "level" (int64, default: 1),
//		Disk level, if applicable, can be used for implementing
//		multi level log-structured-merge index.
//
// "zblocksize" (int64, default: 4096)
//		Disk block size for leaf nodes (z-nodes).
//
// "mblocksize" (int64, default: 4096),
//		Disk block size for intermediate nodes (m-nodes).
//
// "mreduce" (bool, default: false),
//		Reduce entries at block level.
//
// "datafile" (bool, default: false),
//      If true, store values in a separate datafile. This
//      can give better density in leaf nodes, favourable for
//      buffer caching.
//
// "metadata.bornseqno" (bool, default: true),
//		If true, use metadata field to book-keep entry's born
//		sequence number.
//
// "metadata.deadseqno" (bool, default: true),
//		If true, use metadata field to book-keep entry's dead
//		sequence number.
//
// "metadata.vbuuid" (bool, default: true),
//		If true, use metadata field to book-keep entry's vbuuid.
//
// "iterpool.size" (int64, default: 100),
//		Maximum number of iterators that can be active.
func Defaultsettings() s.Settings {
	// when changing this also change in other places,
	// Bubt{}, Snapshot{} strucutres
	// readsettings()
	// setts2json()
	// json2setts()
	return s.Settings{
		"level":              1,
		"zblocksize":         4096,
		"mblocksize":         4096,
		"mreduce":            false,
		"datafile":           false,
		"metadata.vbuuid":    true,
		"metadata.bornseqno": true,
		"metadata.deadseqno": true,
		"iterpool.size":      100,
	}
}
