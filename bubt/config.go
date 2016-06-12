package bubt

import "github.com/prataprc/storage.go/lib"

// Defaultconfig provides an initial set of configuration, tuned for
// SSD without map-reduce.
func Defaultconfig() lib.Config {
	return lib.Config{
		"zblocksize":   4096,
		"mblocksize":   4086,
		"mreduce":      false,
		"iterpoolsize": 8,
		"level":        1,
	}
}
