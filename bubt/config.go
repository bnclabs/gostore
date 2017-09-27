package bubt

import s "github.com/prataprc/gosettings"

// Defaultsettings for bubt instance.
//
// "zblocksize" (int64, default: 4096)
//		Disk block size for leaf nodes (z-nodes).
//
// "mblocksize" (int64, default: 4096),
//		Disk block size for intermediate nodes (m-nodes).
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
		"zblocksize": 4096,
		"mblocksize": 4096,
	}
}
