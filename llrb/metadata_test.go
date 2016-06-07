package llrb

import "math/rand"
import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestInitMetadata(t *testing.T) {
	md := &metadata{}
	// vbno, fmask
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableVbuuid()
	md.initMetadata(0xacac /*vbno*/, fmask)
	if md.vbno() != 0xacac {
		t.Errorf("expected %v, got %v", 0xacac, md.vbno())
	} else if md.access() != 0 {
		t.Errorf("expected %v, got %v", 0, md.access())
	} else if md.fmask() != 0xf {
		t.Errorf("expected %v, got %v", 0xf, md.fmask())
	} else if md.sizeof() != 40 {
		t.Errorf("expected %v, got %v", 40, md.sizeof())
	}

	// bnseq, ddseq, vbuuid
	dotest1 := func(md *metadata, bnseq, ddseq, vbuuid uint64) {
		if md.isbnseq() && (md.setbnseq(bnseq).bnseq() != bnseq) {
			t.Errorf("expected %v, got %v", bnseq, md.bnseq())
		} else if md.isddseq() && (md.setddseq(ddseq).ddseq() != ddseq) {
			t.Errorf("expected %v, got %v", ddseq, md.ddseq())
		} else if md.isvbuuid() && (md.setvbuuid(vbuuid).vbuuid() != vbuuid) {
			t.Errorf("expected %v, got %v", vbuuid, md.vbuuid())
		}
	}
	for i := 0; i < 10000; i++ {
		md := randomMetadata()
		dotest1(md, 0xabcdef0123456789, 0xabcdef0123456789, 0xabcdef<<40)
	}
}

func TestAccessMetadata(t *testing.T) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableVbuuid()
	md.initMetadata(0xacac /*vbno*/, fmask)
	if x := md.setaccess(0x2).access(); x != 0x2 {
		t.Errorf("expected %x, got %x", 0x2, x)
	} else if x := md.setaccess(0xfffff).access(); x != 0xfffff {
		t.Errorf("expected %x, got %x", 0xfffff, x)
	} else if x := md.setaccess(0x100000).access(); x != 0xfffff {
		t.Errorf("expected %x, got %x", 0xfffff, x)
	}
}

func TestFlagsMetadata(t *testing.T) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableVbuuid()
	md.initMetadata(0xacac /*vbno*/, fmask)
	// black
	if md.setblack().isblack() == false {
		t.Errorf("expected true, got false")
	} else if md.setred().isred() == false {
		t.Errorf("expected true, got false")
	} else if md.isblack() {
		t.Errorf("expected false, got true")
	} else if md.togglelink().isblack() == false {
		t.Errorf("expected true, got false")
	}
	// dirty
	if md.isdirty() {
		t.Errorf("expected false, got true")
	} else if md.setdirty().isdirty() == false {
		t.Errorf("expected true, got false")
	} else if md.cleardirty().isdirty() {
		t.Errorf("expected false, got true")
	}
	// deleted
	if md.isdeleted() {
		t.Errorf("expected false, got true")
	} else if md.setdeleted().isdeleted() == false {
		t.Errorf("expected true, got false")
	}
}

func TestMvalueMetadata(t *testing.T) {
	for i := uint64(0); i < 10000; i++ {
		md := randomMetadata()
		md.initMetadata(0, md.fmask().enableMvalue())
		mvalue := (uint64(0xabcdef0123456789) + i) & 0xfffffffffffffff8
		mval := md.setmvalue(mvalue).mvalue()
		if md.ismvalue() {
			if mvalue != mval {
				t.Errorf("expected %v, for %v", mvalue, mval)
			}
		} else {
			t.Errorf("expected mvalue to be enabled")
		}
	}
}

func TestFposMetadata(t *testing.T) {
	for i := uint64(0); i < 10000; i++ {
		md := randomMetadata()
		md.initMetadata(0, md.fmask().enableFpos())
		offset := (uint64(0x12345678) + i) & 0x07FFFFFFFFFFFFFF
		for level := byte(0); level < 32; level++ {
			md.setfpos(level, offset)
			lvl, off := md.setfpos(level, offset).fpos()
			if md.isfpos() {
				if int64(offset) != off {
					t.Errorf("expected %v, got %v", offset, off)
				} else if level != lvl {
					t.Errorf("expected %v, got %v", level, lvl)
				}
			} else {
				t.Errorf("expected fpos to be enabled")
			}
		}
	}
}

func BenchmarkMdSizeof(b *testing.B) {
	md := &metadata{}
	for i := 0; i < b.N; i++ {
		md.sizeof()
	}
}

func BenchmarkMdInitmeta(b *testing.B) {
	md := &metadata{}
	for i := 0; i < b.N; i++ {
		md.initMetadata(0x10, 0x10)
	}
}

func BenchmarkMdGetvbno(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.vbno()
	}
}

func BenchmarkMdGetfmask(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.fmask()
	}
}

func BenchmarkMdSetaccess(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.setaccess(0x12345)
	}
}

func BenchmarkMdGetaccess(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	md.setaccess(0x12345)
	for i := 0; i < b.N; i++ {
		md.access()
	}
}

func BenchmarkMdSetbnseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, metadataMask(0).enableBornSeqno())
	for i := 0; i < b.N; i++ {
		md.setbnseq(0x12345)
	}
}

func BenchmarkMdGetbnseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, metadataMask(0).enableBornSeqno())
	md.setbnseq(0x12345)
	for i := 0; i < b.N; i++ {
		md.bnseq()
	}
}

func BenchmarkMdSetddseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, metadataMask(0).enableBornSeqno().enableDeadSeqno())
	for i := 0; i < b.N; i++ {
		md.setddseq(0x12345)
	}
}

func BenchmarkMdGetddseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, metadataMask(0).enableBornSeqno().enableDeadSeqno())
	md.setddseq(0x12345)
	for i := 0; i < b.N; i++ {
		md.ddseq()
	}
}

func BenchmarkMdSetMvalue(b *testing.B) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno().enableMvalue()
	md.initMetadata(0x10, fmask)
	for i := 0; i < b.N; i++ {
		md.setmvalue(0x12345)
	}
}

func BenchmarkMdGetmvalue(b *testing.B) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno().enableMvalue()
	md.initMetadata(0x10, fmask)
	md.setmvalue(0x12345)
	for i := 0; i < b.N; i++ {
		md.mvalue()
	}
}

func BenchmarkMdSetvbuuid(b *testing.B) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableVbuuid()
	md.initMetadata(0x10, fmask)
	for i := 0; i < b.N; i++ {
		md.setvbuuid(0x12345)
	}
}

func BenchmarkMdGetvbuuid(b *testing.B) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableVbuuid()
	md.initMetadata(0x10, fmask)
	md.setvbuuid(0x12345)
	for i := 0; i < b.N; i++ {
		md.vbuuid()
	}
}

func BenchmarkMdSetfpos(b *testing.B) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableFpos()
	md.initMetadata(0x10, fmask)
	for i := 0; i < b.N; i++ {
		md.setfpos(4, 0x12345)
	}
}

func BenchmarkMdGetfpos(b *testing.B) {
	md := &metadata{}
	fmask := metadataMask(0).enableBornSeqno().enableDeadSeqno()
	fmask = fmask.enableMvalue().enableFpos()
	md.initMetadata(0x10, fmask)
	md.setfpos(4, 0x12345)
	for i := 0; i < b.N; i++ {
		md.fpos()
	}
}

func randomMetadata() *metadata {
	var flags metadataMask
	if rand.Intn(2) == 1 {
		flags.enableBornSeqno()
	}
	if rand.Intn(2) == 1 {
		flags.enableDeadSeqno()
	}
	if rand.Intn(2) == 1 {
		flags.enableMvalue()
	}
	if rand.Intn(2) == 1 {
		flags.enableVbuuid()
	}
	if rand.Intn(2) == 1 {
		flags.enableFpos()
	}
	return (&metadata{}).initMetadata(0, flags)
}
