package llrb

import "github.com/prataprc/storage.go/lib"
import "github.com/prataprc/storage.go/malloc"

func defaultconfig() lib.Config {
	config := lib.Config{
		"maxvb":                   int64(1024),
		"log.level":               "info",
		"log.file":                "",
		"iterpool.size":           int64(100),
		"metadata.bornseqno":      true,
		"metadata.deadseqno":      true,
		"metadata.mvalue":         true,
		"metadata.vbuuid":         false,
		"metadata.fpos":           true,
		"nodearena.minblock":      int64(96),
		"nodearena.maxblock":      int64(1024),
		"nodearena.capacity":      int64(1024 * 1024 * 1024),
		"nodearena.pool.capacity": int64(2 * 1024 * 1024),
		"nodearena.maxpools":      malloc.Maxpools,
		"nodearena.maxchunks":     malloc.Maxchunks,
		"nodearena.allocator":     "flist",
		"valarena.minblock":       int64(96),
		"valarena.maxblock":       int64(1024 * 1024),
		"valarena.capacity":       int64(10 * 1024 * 1024 * 1024),
		"valarena.pool.capacity":  int64(10 * 2 * 1024 * 1024),
		"valarena.maxpools":       malloc.Maxpools,
		"valarena.maxchunks":      malloc.Maxchunks,
		"valarena.allocator":      "flist",
		"mvcc.enable":             false,
		"mvcc.snapshot.tick":      int64(5), // 5 millisecond
		"mvcc.writer.chanbuffer":  int64(1000),
	}
	return config
}
