package main

func newllrbconfig() map[string]interface{} {
	config := map[string]interface{}{
		"maxvb":                   1024,
		"mvcc.enabled":            validateopts.mvcc,
		"mvcc.snapshot.tick":      0,
		"mvcc.writer.chanbuffer":  1000,
		"nodearena.minblock":      loadopts.nodearena[0],
		"nodearena.maxblock":      loadopts.nodearena[1],
		"nodearena.capacity":      loadopts.nodearena[2],
		"nodearena.pool.capacity": loadopts.nodearena[3],
		"valarena.minblock":       loadopts.valarena[0],
		"valarena.maxblock":       loadopts.valarena[1],
		"valarena.capacity":       loadopts.valarena[2],
		"valarena.pool.capacity":  loadopts.valarena[3],
		"metadata.mvalue":         true,
		"metadata.vbuuid":         true,
		"metadata.bornseqno":      true,
		"metadata.deadseqno":      true,
	}
	return config
}
