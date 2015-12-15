package llrb

import "unsafe"

var mdlookup = [16]int{
	0, // 0x0
	1, // 0x1 bnseq
	1, // 0x2 ddseq
	2, // 0x3 ddseq bnseq
	1, // 0x4 mvalue
	2, // 0x5 mvalue bnseq
	2, // 0x6 mvalue ddseq
	3, // 0x7 mvalue ddseq bnseq
	1, // 0x8 vbuuid
	2, // 0x9 vbuuid bnseq
	2, // 0xa vbuuid ddseq
	3, // 0xb vbuuid ddseq bnseq
	2, // 0xc vbuuid mvalue
	3, // 0xd vbuuid mvalue bnseq
	3, // 0xe vbuuid mvalue ddseq
	4, // 0xf vbuuid mvalue ddseq bnseq
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
	// ksize(52:64) access(32:52) vbno(16:32)
	// reserved deleted dirty black (vbuuid mvalue bnseq)[:12]
	hdr uint64
	// vbuuid mvalue ddseq bnseq
	fields [16]uint64 // maximum 16 fields.
}

func (md *metadata) initMetadata(vbno uint16, fmask metadataMask) *metadata {
	md.hdr = md.hdr&0xffffffff00000000 | ((uint64(vbno) << 16) | uint64(fmask))
	return md
}

func (md *metadata) sizeof() int {
	n, sz := 8, int(unsafe.Sizeof(uint64(0)))
	for i := uint64(0); i < 3; i++ {
		off := (md.hdr >> (i * 4)) & 0xf
		n += (mdlookup[off] * sz)
	}
	return n
}

func (md *metadata) isbnseq() bool {
	return (md.hdr & uint64(mdFlagBornseqno)) != 0
}

func (md *metadata) isddseq() bool {
	return (md.hdr & uint64(mdFlagDeadseqno)) != 0
}

func (md *metadata) ismvalue() bool {
	return (md.hdr & uint64(mdFlagMvalue)) != 0
}

func (md *metadata) isvbuuid() bool {
	return (md.hdr & uint64(mdFlagVbuuid)) != 0
}

func (md *metadata) setblack() *metadata {
	md.hdr = md.hdr | 0x1000
	return md
}

func (md *metadata) isblack() bool {
	return (md.hdr & 0x1000) == 0x1000
}

func (md *metadata) setred() *metadata {
	md.hdr = md.hdr & 0xffffffffffffefff // clear the bit
	return md
}

func (md *metadata) isred() bool {
	return !md.isblack()
}

func (md *metadata) togglelink() *metadata {
	md.hdr = md.hdr ^ 0x1000
	return md
}

func (md *metadata) isdirty() bool {
	return (md.hdr & 0x2000) == 0x2000
}

func (md *metadata) setdirty() *metadata {
	md.hdr = md.hdr | 0x2000
	return md
}

func (md *metadata) cleardirty() *metadata {
	md.hdr = md.hdr & 0xffffffffffffdfff
	return md
}

func (md *metadata) isdeleted() bool {
	return (md.hdr & 0x4000) == 0x4000
}

func (md *metadata) setdeleted() *metadata {
	md.hdr = md.hdr | 0x4000
	return md
}

func (md *metadata) setvbno(vbno uint16) *metadata {
	md.hdr = (md.hdr & 0xffffffff0000ffff) | (uint64(vbno) << 16)
	return md
}

func (md *metadata) vbno() uint16 {
	return uint16((md.hdr & 0xffff0000) >> 16)
}

func (md *metadata) fmask() metadataMask {
	return metadataMask(md.hdr & 0xffff)
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
	off := mdlookup[mdOffsetmaskBnseq&md.hdr]
	md.fields[off-1] = seqno
	return md
}

func (md *metadata) bnseq() uint64 {
	off := mdlookup[mdOffsetmaskBnseq&md.hdr]
	return md.fields[off-1]
}

func (md *metadata) setddseq(seqno uint64) *metadata {
	off := mdlookup[mdOffsetmaskDdseq&md.hdr]
	md.fields[off-1] = seqno
	return md
}

func (md *metadata) ddseq() uint64 {
	off := mdlookup[mdOffsetmaskDdseq&md.hdr]
	return md.fields[off-1]
}

func (md *metadata) setmvalue(mvalue uint64, level byte) *metadata {
	mvalue = (mvalue & 0xfffffffffffffff8) | uint64(level&0x7)
	off := mdlookup[mdOffsetmaskMvalue&md.hdr]
	md.fields[off-1] = mvalue
	return md
}

func (md *metadata) mvalue() (uint64, byte) {
	off := mdlookup[mdOffsetmaskMvalue&md.hdr]
	mvalue := md.fields[off-1]
	return mvalue & 0xfffffffffffffff8, byte(mvalue & 0x7)
}

func (md *metadata) setvbuuid(vbuuid uint64) *metadata {
	off := mdlookup[mdOffsetmaskVbuuid&md.hdr]
	md.fields[off-1] = vbuuid
	return md
}

func (md *metadata) vbuuid() uint64 {
	off := mdlookup[mdOffsetmaskVbuuid&md.hdr]
	return md.fields[off-1]
}

func (md *metadata) setkeysize(size int) *metadata {
	md.hdr = (md.hdr & 0x000fffffffffffff) | ((uint64(size) & 0xfff) << 52)
	return md
}

func (md *metadata) keysize() int {
	return int((md.hdr & 0xfff0000000000000) >> 52)
}
