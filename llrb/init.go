package llrb

import "github.com/prataprc/gostore/malloc"
import s "github.com/prataprc/gosettings"

func (llrb *LLRB) readsettings(setts s.Settings) {
	llrb.iterpoolsize = setts.Int64("iterpool.size")
	llrb.lsm = setts.Bool("lsm")
	llrb.keycapacity = setts.Int64("keycapacity")
	llrb.valcapacity = setts.Int64("valcapacity")
	llrb.maxlimit = setts.Int64("maxlimit")
	llrb.mvcc.enabled = setts.Bool("mvcc.enable")
	llrb.snaptick = setts.Int64("mvcc.snapshot.tick")
	llrb.writechansz = setts.Int64("mvcc.writer.chansize")
}

func (llrb *LLRB) newnodearena(setts s.Settings) *malloc.Arena {
	memsetts := setts.Section("nodearena").Trim("nodearena.")
	return malloc.NewArena(llrb.keycapacity, memsetts)
}

func (llrb *LLRB) newvaluearena(setts s.Settings) *malloc.Arena {
	memsetts := setts.Section("valarena").Trim("valarena.")
	return malloc.NewArena(llrb.valcapacity, memsetts)
}

func setupfmask(setts s.Settings) metadataMask {
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

// adjust key size to include metadata and overhead.
func adjustkeysize(keysize int64, setts s.Settings) int64 {
	fmask := setupfmask(setts)
	mdsize := int64((&metadata{}).initMetadata(0, fmask).sizeof())
	return keysize + int64((&Llrbnode{}).sizeof()) + mdsize
}

// adjust value size to include metadata and overhead.
func adjustvalsize(valsize int64) int64 {
	return valsize + int64((&nodevalue{}).sizeof())
}
