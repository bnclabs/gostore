package llrb

import "math/rand"
import "testing"

func TestInitMetadata(t *testing.T) {
	md := &metadata{}
	// vbno, fmask
	md.initMetadata(0xacac /*vbno*/, 0xbeaf /*fmask*/)
	if md.vbno() != 0xacac {
		t.Errorf("expected %v, got %v", 0xacac, md.vbno())
	} else if md.access() != 0 {
		t.Errorf("expected %v, got %v", 0, md.access())
	} else if md.fmask() != 0xbeaf {
		t.Errorf("expected %v, got %v", 0xbeaf, md.fmask())
	}

	// bnseq, ddseq, vbuuid
	dotest1 := func(md *metadata, bnseq, ddseq, vbuuid uint64) {
		if md.isbnseq() && (md.setbnseq(bnseq).bnseq() != bnseq) {
			t.Errorf("expected %v, got %v", bnseq, md.bnseq())
		} else if md.setbnseq(bnseq).bnseq() != 0 {
			t.Errorf("expected %v, got %v", 0, md.bnseq())
		} else if md.isddseq() && (md.setddseq(ddseq).ddseq() != ddseq) {
			t.Errorf("expected %v, got %v", ddseq, md.ddseq())
		} else if md.setddseq(ddseq).ddseq() != 0 {
			t.Errorf("expected %v, got %v", 0, md.ddseq())
		} else if md.isvbuuid() && (md.setvbuuid(vbuuid).vbuuid() != vbuuid) {
			t.Errorf("expected %v, got %v", vbuuid, md.vbuuid())
		} else if md.setvbuuid(vbuuid).vbuuid() != 0 {
			t.Errorf("expected %v, got %v", 0, md.vbuuid())
		}
	}
	for i := 0; i < 10000; i++ {
		md := randomMetadata()
		dotest1(md, 0xabcdef0123456789, 0xabcdef0123456789, 0xabcdef<<40)
	}
}

func TestAccessMetadata(t *testing.T) {
	md := &metadata{}
	md.initMetadata(0xacac /*vbno*/, 0xbeaf /*fmask*/)
	if x := md.setaccess(0x2).access(); x != 0x2 {
		t.Errorf("expected %x, got %x", 0x2, x)
	} else if x := md.setaccess(0xfffff).access(); x != 0xfffff {
		t.Errorf("expected %x, got %x", 0xfffff, x)
	} else if x := md.setaccess(0x100000).access(); x != 0xfffff {
		t.Errorf("expected %x, got %x", 0xfffff, x)
	}
}

func TestMvalueMetadata(t *testing.T) {
	for i := uint64(0); i < 10000; i++ {
		md := randomMetadata()
		mvalue := (uint64(0xabcdef0123456789) + i) & 0xfffffffffffffff8
		for level := byte(0); level < 8; level++ {
			mval, lvl := md.setmvalue(mvalue, level).mvalue()
			if md.ismvalue() {
				if mvalue != mval {
					t.Errorf("expected %v, for %v", mvalue, mval)
				} else if level != lvl {
					t.Errorf("expected %v, for %v", level, lvl)
				}
			} else {
				if 0 != mval {
					t.Errorf("expected %v, for %v", 0, mval)
				} else if 0 != lvl {
					t.Errorf("expected %v, for %v", 0, lvl)
				}
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
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.setbnseq(0x12345)
	}
}

func BenchmarkMdGetbnseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	md.setbnseq(0x12345)
	for i := 0; i < b.N; i++ {
		md.bnseq()
	}
}

func BenchmarkMdSetddseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.setddseq(0x12345)
	}
}

func BenchmarkMdGetddseq(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	md.setddseq(0x12345)
	for i := 0; i < b.N; i++ {
		md.ddseq()
	}
}

func BenchmarkMdSetMvalue(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.setmvalue(0x12345, 4)
	}
}

func BenchmarkMdGetmvalue(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	md.setmvalue(0x12345, 4)
	for i := 0; i < b.N; i++ {
		md.mvalue()
	}
}

func BenchmarkMdSetvbuuid(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	for i := 0; i < b.N; i++ {
		md.setvbuuid(0x12345)
	}
}

func BenchmarkMdGetvbuuid(b *testing.B) {
	md := &metadata{}
	md.initMetadata(0x10, 0x10)
	md.setvbuuid(0x12345)
	for i := 0; i < b.N; i++ {
		md.vbuuid()
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
	return (&metadata{}).initMetadata(0, flags)
}
