package bubt

import s "github.com/prataprc/gosettings"

// Defaultsettings for bubt instance, tuned for SSD without map-reduce.
//
// Configurable parameters:
//
// "zblocksize" (int64, default: 4096),
//		disk block size for leaf nodes (z-nodes)
//
// "mblocksize" (int64, default: 4096),
//		disk block size for intermediate nodes (m-nodes)
//
// "mreduce" (bool, default: false),
//		reduce entries at block level.
//
// "iterpool.size" (int64, default: 8),
//		maximum number of iterators that can be active.
//
// "level" (int64, default: 1),
//		disk level if applicable.
//
// "datafile" (bool, default: false),
//		store values in a separate datafile.
//
// "metadata.vbuuid" (bool, default: true),
//		store vbuuid, as metadata, for each entry.
//
// "metadata.bornseqno" (bool, default: true),
//		store bornseqno, as metadata, for each entry.
//
// "metadata.deadseqno" (bool, default: true),
//		store deadseqno, as metadata, for each entry.
//
func Defaultsettings() s.Settings {
	// when changing this also change in other places,
	// Bubt{}, Snapshot{} strucutres
	// readsettings()
	// setts2json()
	// json2setts()
	return s.Settings{
		"zblocksize":         4096,
		"mblocksize":         4096,
		"mreduce":            false,
		"iterpool.size":      8,
		"level":              1,
		"datafile":           false,
		"metadata.vbuuid":    true,
		"metadata.bornseqno": true,
		"metadata.deadseqno": true,
	}
}
