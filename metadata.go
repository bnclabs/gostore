package llrb

var mdlookup = [16]int{
	0, // 0x0
	0, // 0x1 bnseq
	0, // 0x2 ddseq
	1, // 0x3 ddseq bnseq
	0, // 0x4 mvalue
	1, // 0x5 mvalue bnseq
	1, // 0x6 mvalue ddseq
	2, // 0x7 mvalue ddseq bnseq
	0, // 0x8 vbuuid
	1, // 0x9 vbuuid bnseq
	1, // 0xa vbuuid ddseq
	2, // 0xb vbuuid ddseq bnseq
	1, // 0xc vbuuid mvalue
	2, // 0xd vbuuid mvalue bnseq
	2, // 0xe vbuuid mvalue ddseq
	3, // 0xf vbuuid mvalue ddseq bnseq
}

var mdOffsetmaskBnseq = uint64(0x1)
var mdOffsetmaskDdseq = uint64(0x3)
var mdOffsetmaskMvalue = uint64(0x7)
var mdOffsetmaskVbuuid = uint64(0xf)
var mdOffsetmaskFull = 0xffff

type metadataMask uint16

const (
	mdFlagBornseqno metadataMask = 0x1
	mdFlagDeadseqno              = 0x2
	mdFlagMvalue                 = 0x4
	mdFlagVbuuid                 = 0x8
)

func (mask metadataMask) enableBornSeqno() metadataMask {
	return mask | mdFlagBornseqno
}

func (mask metadataMask) enableDeadSeqno() metadataMask {
	return mask | mdFlagDeadseqno
}

func (mask metadataMask) enableMvalue() metadataMask {
	return mask | mdFlagMvalue
}

func (mask metadataMask) enableVbuuid() metadataMask {
	return mask | mdFlagVbuuid
}

type metadata struct {
	// access(32:52) vbno(16:32) (vbuuid mvalue bnseq)[:16]
	hdr uint64
	// vbuuid mvalue ddseq bnseq
	fields [16]uint64 // maximum 16 fields.
}

func (md *metadata) initMetadata(vbno, fmask metadataMask) *metadata {
	md.hdr = uint64((uint64(vbno) << 16) | uint64(fmask))
	return md
}

func (md *metadata) vbno() uint16 {
	return uint16((md.hdr & 0xffff0000) >> 16)
}

func (md *metadata) fmask() uint16 {
	return uint16(md.hdr & 0xffff)
}

func (md *metadata) setaccess(access uint64) *metadata {
	if access > 0xfffff {
		access = 0xfffff
	}
	md.hdr = (md.hdr & 0x000fffff00000000) | ((access & 0xfffff) << 32)
	return md
}

func (md *metadata) access() uint64 {
	return uint64((md.hdr & 0xfffff00000000) >> 32)
}

func (md *metadata) setbnseq(seqno uint64) *metadata {
	md.fields[mdlookup[mdOffsetmaskBnseq&md.hdr]] = seqno
	return md
}

func (md *metadata) bnseq() uint64 {
	return md.fields[mdlookup[mdOffsetmaskBnseq&md.hdr]]
}

func (md *metadata) setddseq(seqno uint64) *metadata {
	md.fields[mdlookup[mdOffsetmaskDdseq&md.hdr]] = seqno
	return md
}

func (md *metadata) ddseq() uint64 {
	return md.fields[mdlookup[mdOffsetmaskDdseq&md.hdr]]
}

func (md *metadata) setmvalue(mvalue uint64, level byte) *metadata {
	mvalue = (mvalue & 0xfffffffffffffff8) | uint64(level&0x7)
	md.fields[mdlookup[mdOffsetmaskMvalue&md.hdr]] = mvalue
	return md
}

func (md *metadata) mvalue() (uint64, byte) {
	mvalue := md.fields[mdlookup[mdOffsetmaskMvalue&md.hdr]]
	return mvalue & 0xfffffffffffffff8, byte(mvalue & 0x7)
}

func (md *metadata) setvbuuid(vbuuid uint64) *metadata {
	md.fields[mdlookup[mdOffsetmaskVbuuid&md.hdr]] = vbuuid
	return md
}

func (md *metadata) vbuuid() uint64 {
	return md.fields[mdlookup[mdOffsetmaskVbuuid&md.hdr]]
}
