package llrb

import "github.com/prataprc/storage.go/malloc"
import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

func (llrb *LLRB) validateSettings(setts lib.Settings) {
	minblock := setts.Int64("nodearena.minblock")
	maxblock := setts.Int64("nodearena.maxblock")
	capacity := setts.Int64("nodearena.capacity")
	if minblock < api.MinKeymem {
		panicerr("nodearena.minblock < %v settings", api.MinKeymem)
	} else if maxblock > api.MaxKeymem {
		panicerr("nodearena.maxblock > %v settings", api.MaxKeymem)
	} else if capacity == 0 {
		panicerr("nodearena.capacity cannot be ZERO")
	}

	minblock = setts.Int64("valarena.minblock")
	maxblock = setts.Int64("valarena.maxblock")
	capacity = setts.Int64("valarena.capacity")
	if minblock < api.MinValmem {
		panicerr("valarena.minblock < %v settings", api.MinValmem)
	} else if maxblock > api.MaxValmem {
		panicerr("valarena.maxblock > %v settings", api.MaxValmem)
	} else if capacity == 0 {
		panicerr("valarena.capacity cannot be ZERO")
	}
}

func (llrb *LLRB) newnodearena(setts lib.Settings) *malloc.Arena {
	return malloc.NewArena(setts.Section("nodearena").Trim("nodearena."))
}

func (llrb *LLRB) newvaluearena(setts lib.Settings) *malloc.Arena {
	return malloc.NewArena(setts.Section("valarena").Trim("valarena."))
}

func (llrb *LLRB) setupfmask(setts lib.Settings) metadataMask {
	fmask := metadataMask(0)
	if setts.Bool("metadata.bornseqno") {
		fmask = fmask.enableBornSeqno()
	}
	if setts.Bool("metadata.deadseqno") {
		fmask = fmask.enableDeadSeqno()
	}
	if setts.Bool("metadata.mvalue") {
		fmask = fmask.enableMvalue()
	}
	if setts.Bool("metadata.vbuuid") {
		fmask = fmask.enableVbuuid()
	}
	if setts.Bool("metadata.fpos") {
		fmask = fmask.enableFpos()
	}
	return fmask
}
