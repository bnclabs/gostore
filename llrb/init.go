package llrb

import "github.com/prataprc/storage.go/malloc"
import "github.com/prataprc/storage.go/api"
import "github.com/prataprc/storage.go/lib"

func (llrb *LLRB) getsettings(setts lib.Settings) {
	llrb.maxvb = setts.Int64("maxvb")
	llrb.iterpoolsize = setts.Int64("iterpool.size")
	llrb.markdelete = setts.Bool("markdelete")
	llrb.naminblock = setts.Int64("nodearena.minblock")
	llrb.namaxblock = setts.Int64("nodearena.maxblock")
	llrb.nacapacity = setts.Int64("nodearena.capacity")
	llrb.napcapacity = setts.Int64("nodearena.pool.capacity")
	llrb.namaxpools = setts.Int64("nodearena.maxpools")
	llrb.namaxchunks = setts.Int64("nodearena.maxchunks")
	llrb.naallocator = setts.String("nodearena.allocator")
	if llrb.naminblock < api.MinKeymem {
		panicerr("nodearena.minblock < %v settings", api.MinKeymem)
	} else if llrb.namaxblock > api.MaxKeymem {
		panicerr("nodearena.maxblock > %v settings", api.MaxKeymem)
	} else if llrb.nacapacity == 0 {
		panicerr("nodearena.capacity cannot be ZERO")
	}
	llrb.vaminblock = setts.Int64("valarena.minblock")
	llrb.vamaxblock = setts.Int64("valarena.maxblock")
	llrb.vacapacity = setts.Int64("valarena.capacity")
	llrb.vapcapacity = setts.Int64("valarena.pool.capacity")
	llrb.vamaxpools = setts.Int64("valarena.maxpools")
	llrb.vamaxchunks = setts.Int64("valarena.maxchunks")
	llrb.vaallocator = setts.String("valarena.allocator")
	if llrb.vaminblock < api.MinValmem {
		panicerr("valarena.minblock < %v settings", api.MinValmem)
	} else if llrb.vamaxblock > api.MaxValmem {
		panicerr("valarena.maxblock > %v settings", api.MaxValmem)
	} else if llrb.vacapacity == 0 {
		panicerr("valarena.capacity cannot be ZERO")
	}
	llrb.mvcc.enabled = setts.Bool("mvcc.enable")
	llrb.writechansz = setts.Int64("mvcc.writer.chansize")
	llrb.snaptick = setts.Int64("mvcc.snapshot.tick")
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
