package bubt

import s "github.com/prataprc/gosettings"

// Defaultsettings provides an initial set of settings, tuned for
// SSD without map-reduce.
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
