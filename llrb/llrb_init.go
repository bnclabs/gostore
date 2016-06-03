package llrb

import "github.com/prataprc/storage.go/malloc"
import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

func (llrb *LLRB) validateConfig(config lib.Config) {
	minblock := config.Int64("nodearena.minblock")
	maxblock := config.Int64("nodearena.maxblock")
	capacity := config.Int64("nodearena.capacity")
	if minblock < api.MinKeymem {
		panicerr("nodearena.minblock < %v configuration", api.MinKeymem)
	} else if maxblock > api.MaxKeymem {
		panicerr("nodearena.maxblock > %v configuration", api.MaxKeymem)
	} else if capacity == 0 {
		panicerr("nodearena.capacity cannot be ZERO")
	}

	minblock = config.Int64("valarena.minblock")
	maxblock = config.Int64("valarena.maxblock")
	capacity = config.Int64("valarena.capacity")
	if minblock < api.MinValmem {
		panicerr("valarena.minblock < %v configuration", api.MinValmem)
	} else if maxblock > api.MaxValmem {
		panicerr("valarena.maxblock > %v configuration", api.MaxValmem)
	} else if capacity == 0 {
		panicerr("valarena.capacity cannot be ZERO")
	}
}

func (llrb *LLRB) newnodearena(config lib.Config) *malloc.Arena {
	return malloc.NewArena(config.Section("nodearena").Trim("nodearena."))
}

func (llrb *LLRB) newvaluearena(config lib.Config) *malloc.Arena {
	return malloc.NewArena(config.Section("valarena").Trim("valarena."))
}

func (llrb *LLRB) setupfmask(config lib.Config) metadataMask {
	fmask := metadataMask(0)
	if config.Bool("metadata.bornseqno") {
		fmask = fmask.enableBornSeqno()
	}
	if config.Bool("metadata.deadseqno") {
		fmask = fmask.enableDeadSeqno()
	}
	if config.Bool("metadata.mvalue") {
		fmask = fmask.enableMvalue()
	}
	if config.Bool("metadata.vbuuid") {
		fmask = fmask.enableVbuuid()
	}
	if config.Bool("metadata.fpos") {
		fmask = fmask.enableFpos()
	}
	return fmask
}
