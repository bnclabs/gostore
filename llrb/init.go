package llrb

import "fmt"

import "github.com/prataprc/golog"
import "github.com/prataprc/gostore/api"
import "github.com/prataprc/gostore/malloc"
import s "github.com/prataprc/gosettings"

// TODO: replace panicerr with panic.

func (llrb *LLRB) readsettings(setts s.Settings) {
	fmask := llrb.setupfmask(setts)
	mdsize := int64((&metadata{}).initMetadata(0, fmask).sizeof())

	llrb.iterpoolsize = setts.Int64("iterpool.size")
	llrb.lsm = setts.Bool("lsm")
	llrb.minkeysize = setts.Int64("minkeysize")
	llrb.maxkeysize = setts.Int64("maxkeysize")
	llrb.minvalsize = setts.Int64("minvalsize")
	llrb.maxvalsize = setts.Int64("maxvalsize")
	llrb.keycapacity = setts.Int64("keycapacity")
	llrb.valcapacity = setts.Int64("valcapacity")
	llrb.maxlimit = setts.Int64("maxlimit")
	llrb.mvcc.enabled = setts.Bool("mvcc.enable")
	llrb.snaptick = setts.Int64("mvcc.snapshot.tick")
	llrb.writechansz = setts.Int64("mvcc.writer.chansize")

	// adjust minkeysize to include overhead
	if llrb.minkeysize < 0 {
		panicerr("invalid minkeysize settings %v", llrb.minkeysize)
	}
	minkeysize := llrb.minkeysize + int64((&Llrbnode{}).sizeof()) + mdsize
	minkeysize = minkeysize / 32 * 32 // floor minsize
	fmsg := "%v adjusting minkeysize %v->%v with overhead"
	log.Infof(fmsg, llrb.logprefix, llrb.minkeysize, minkeysize)
	llrb.minkeysize = minkeysize
	// adjust maxkeysize to include overhead
	maxkeysize := llrb.maxkeysize + int64((&Llrbnode{}).sizeof()) + mdsize
	if (maxkeysize % 32) != 0 { // ceil maxsize
		maxkeysize = ((maxkeysize / 32) + 1) * 32
	}
	fmsg = "%v adjusting maxkeysize %v->%v with overhead"
	log.Infof(fmsg, llrb.logprefix, llrb.maxkeysize, maxkeysize)
	llrb.maxkeysize = maxkeysize
	// validate minkeysize maxkeysize
	if llrb.minkeysize > api.MaxKeysize {
		fmsg := "%v minkey %v > %v"
		panic(fmt.Errorf(fmsg, llrb.logprefix, llrb.minkeysize, api.MaxKeysize))
	} else if llrb.maxkeysize > api.MaxKeysize {
		fmsg := "%v maxkey %v > %v"
		panic(fmt.Errorf(fmsg, llrb.logprefix, llrb.maxkeysize, api.MaxKeysize))
	}

	// adjust minvalsize to include overhead
	if llrb.minvalsize < 0 {
		panicerr("invalid minvalsize.maxblock settings %v", llrb.minvalsize)
	}
	minvalsize := llrb.minvalsize + int64((&nodevalue{}).sizeof())
	minvalsize = minvalsize / 32 * 32 // floor minsize
	fmsg = "%v adjusting minvalsize %v->%v with overhead"
	log.Infof(fmsg, llrb.logprefix, llrb.minvalsize, minvalsize)
	llrb.minvalsize = minvalsize
	// adjust maxvalsize to include overhead
	maxvalsize := llrb.maxvalsize + int64((&nodevalue{}).sizeof())
	if (maxvalsize % 32) != 0 { // ceil maxsize
		maxvalsize = ((maxvalsize / 32) + 1) * 32
	}
	fmsg = "%v adjusting maxvalsize %v->%v with overhead"
	log.Infof(fmsg, llrb.logprefix, llrb.maxvalsize, maxvalsize)
	llrb.maxvalsize = maxvalsize
}

func (llrb *LLRB) newnodearena(setts s.Settings) *malloc.Arena {
	memsetts := setts.Section("nodearena").Trim("nodearena.")
	memsetts["minblock"] = llrb.minkeysize
	memsetts["maxblock"] = llrb.maxkeysize
	return malloc.NewArena(llrb.keycapacity, memsetts)
}

func (llrb *LLRB) newvaluearena(setts s.Settings) *malloc.Arena {
	memsetts := setts.Section("valarena").Trim("valarena.")
	memsetts["minblock"] = llrb.minvalsize
	memsetts["maxblock"] = llrb.maxvalsize
	return malloc.NewArena(llrb.valcapacity, memsetts)
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
