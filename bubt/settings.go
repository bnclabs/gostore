package bubt

import "github.com/prataprc/storage.go/lib"

// DefaultSettings provides an initial set of settings, tuned for
// SSD without map-reduce.
func DefaultSettings() lib.Settings {
	return lib.Settings{
		"zblocksize":    4096,
		"mblocksize":    4096,
		"mreduce":       false,
		"iterpool.size": 8,
		"level":         1,
	}
}
