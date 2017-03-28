package llrb

import "github.com/prataprc/gostore/malloc"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/lib"

func (llrb *LLRB) readsettings(setts lib.Settings) {
	fmask := llrb.setupfmask(setts)
	mdsize := int64((&metadata{}).initMetadata(0, fmask).sizeof())

	llrb.iterpoolsize = setts.Int64("iterpool.size")
	llrb.lsm = setts.Bool("lsm")
	llrb.minkeysize = setts.Int64("minkeysize")
	llrb.maxkeysize = setts.Int64("maxkeysize")
	llrb.naminblock = (llrb.minkeysize / 32) * 32 // floor minsize
	llrb.namaxblock = llrb.maxkeysize + int64((&Llrbnode{}).sizeof()) + mdsize
	if (llrb.namaxblock % 32) != 0 { // ceil maxsize
		llrb.namaxblock = ((llrb.namaxblock / 32) + 1) * 32
	}
	llrb.minvalsize = setts.Int64("minvalsize")
	llrb.maxvalsize = setts.Int64("maxvalsize")
	llrb.vaminblock = (llrb.minvalsize / 32) * 32 // floor minsize
	llrb.vamaxblock = llrb.maxvalsize + int64((&nodevalue{}).sizeof())
	if (llrb.vamaxblock % 32) != 0 { // ceil maxsize
		llrb.vamaxblock = ((llrb.vamaxblock / 32) + 1) * 32
	}

	llrb.maxlimit = setts.Int64("maxlimit")
	llrb.nacapacity = setts.Int64("nodearena.capacity")
	llrb.napcapacity = setts.Int64("nodearena.pool.capacity")
	llrb.namaxpools = setts.Int64("nodearena.maxpools")
	llrb.namaxchunks = setts.Int64("nodearena.maxchunks")
	llrb.naallocator = setts.String("nodearena.allocator")
	if llrb.minkeysize < api.MinKeymem {
		panicerr("nodearena.minblock < %v settings", api.MinKeymem)
	} else if llrb.maxkeysize > api.MaxKeymem {
		panicerr("nodearena.maxblock > %v settings", api.MaxKeymem)
	} else if llrb.nacapacity == 0 {
		panicerr("nodearena.capacity cannot be ZERO")
	}
	llrb.vacapacity = setts.Int64("valarena.capacity")
	llrb.vapcapacity = setts.Int64("valarena.pool.capacity")
	llrb.vamaxpools = setts.Int64("valarena.maxpools")
	llrb.vamaxchunks = setts.Int64("valarena.maxchunks")
	llrb.vaallocator = setts.String("valarena.allocator")
	if llrb.minvalsize < api.MinValmem {
		panicerr("valarena.minblock < %v settings", api.MinValmem)
	} else if llrb.maxvalsize > api.MaxValmem {
		panicerr("valarena.maxblock > %v settings", api.MaxValmem)
	} else if llrb.vacapacity == 0 {
		panicerr("valarena.capacity cannot be ZERO")
	}
	llrb.mvcc.enabled = setts.Bool("mvcc.enable")
	llrb.writechansz = setts.Int64("mvcc.writer.chansize")
	llrb.snaptick = setts.Int64("mvcc.snapshot.tick")
	llrb.memutilization = setts.Float64("memutilization")
}

func (llrb *LLRB) newnodearena(setts lib.Settings) *malloc.Arena {
	memsetts := setts.Section("nodearena").Trim("nodearena.")
	memsetts["minblock"] = llrb.naminblock
	memsetts["maxblock"] = llrb.namaxblock
	return malloc.NewArena(memsetts)
}

func (llrb *LLRB) newvaluearena(setts lib.Settings) *malloc.Arena {
	memsetts := setts.Section("valarena").Trim("valarena.")
	memsetts["minblock"] = llrb.vaminblock
	memsetts["maxblock"] = llrb.vamaxblock
	return malloc.NewArena(memsetts)
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
