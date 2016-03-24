package storage

func llrbConfig() map[string]interface{} {
	config := map[string]interface{}{
		"maxvb": 1024,
		// TODO: tune `defrag.period` after measuring full table scan,
		// continuation, and piecewise range.
		"log.level":               "info",
		"log.file":                "",
		"defrag.period":           2 * 3600,
		"metadata.bornseqno":      true,
		"metadata.deadseqno":      true,
		"metadata.mvalue":         true,
		"metadata.vbuuid":         false,
		"nodearena.minblock":      96,
		"nodearena.maxblock":      1024,
		"nodearena.capacity":      1024 * 1024 * 1024,
		"nodearena.pool.capacity": 2 * 1024 * 1024,
		"valarena.minblock":       96,
		"valarena.maxblock":       1024 * 1024,
		"valarena.capacity":       10 * 1024 * 1024 * 1024,
		"valarena.pool.capacity":  10 * 2 * 1024 * 1024,
		"mvcc.enable":             false,
		"mvcc.snapshot.tick":      5, // 5 millisecond
		"mvcc.writer.chanbuffer":  1000,
	}
	return config
}
