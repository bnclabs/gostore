package llrb

import "github.com/prataprc/gostore/malloc"
import s "github.com/prataprc/gosettings"

func (llrb *LLRB) readsettings(setts s.Settings) {
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

	llrb.keycapacity = setts.Int64("keycapacity")
	llrb.valcapacity = setts.Int64("valcapacity")
	llrb.maxlimit = setts.Int64("maxlimit")
	llrb.naallocator = setts.String("nodearena.allocator")
	if llrb.minkeysize <= 0 {
		panicerr("invalid nodearena.minblock settings %v", llrb.minkeysize)
	}
	llrb.vaallocator = setts.String("valarena.allocator")
	llrb.mvcc.enabled = setts.Bool("mvcc.enable")
	llrb.writechansz = setts.Int64("mvcc.writer.chansize")
	llrb.snaptick = setts.Int64("mvcc.snapshot.tick")
	llrb.memutilization = setts.Float64("memutilization")
}

func (llrb *LLRB) newnodearena(capacity int64, setts s.Settings) *malloc.Arena {
	memsetts := setts.Section("nodearena").Trim("nodearena.")
	memsetts["minblock"] = llrb.naminblock
	memsetts["maxblock"] = llrb.namaxblock
	return malloc.NewArena(capacity, memsetts)
}

func (llrb *LLRB) newvaluearena(capacity int64, setts s.Settings) *malloc.Arena {
	memsetts := setts.Section("valarena").Trim("valarena.")
	memsetts["minblock"] = llrb.vaminblock
	memsetts["maxblock"] = llrb.vamaxblock
	return malloc.NewArena(capacity, memsetts)
}

func (llrb *LLRB) setupfmask(setts s.Settings) metadataMask {
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
