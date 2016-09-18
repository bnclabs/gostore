package llrb

import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/malloc"

// DefaultSettings provides an initial set of settings tuned for in-memory
// key,value of 1TB storage.
func DefaultSettings() lib.Settings {
	setts := lib.Settings{
		"maxvb":                   int64(1024),
		"iterpool.size":           int64(100),
		"markdelete":              false,
		"metadata.bornseqno":      false,
		"metadata.deadseqno":      false,
		"metadata.mvalue":         true,
		"metadata.vbuuid":         false,
		"metadata.fpos":           false,
		"mvcc.enable":             false,
		"mvcc.snapshot.tick":      int64(5), // 5 millisecond
		"mvcc.writer.chansize":    int64(1000),
		"nodearena.capacity":      int64(1024 * 1024 * 1024),
		"nodearena.minblock":      int64(96),
		"nodearena.maxblock":      int64(1024),
		"nodearena.pool.capacity": int64(2 * 1024 * 1024),
		"nodearena.maxpools":      malloc.Maxpools,
		"nodearena.maxchunks":     malloc.Maxchunks,
		"nodearena.allocator":     "flist",
		"valarena.minblock":       int64(96),
		"valarena.maxblock":       int64(1024 * 1024),
		"valarena.capacity":       int64(1024 * 1024 * 1024 * 1024),
		"valarena.pool.capacity":  int64(10 * 1024 * 1024),
		"valarena.maxpools":       malloc.Maxpools,
		"valarena.maxchunks":      malloc.Maxchunks,
		"valarena.allocator":      "flist",
	}
	return setts
}
