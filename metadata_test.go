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
		if md.setbnseq(bnseq).bnseq() != bnseq {
			t.Errorf("expected %v, got %v", bnseq, md.bnseq())
		} else if md.setbnseq(ddseq).ddseq() != ddseq {
			t.Errorf("expected %v, got %v", ddseq, md.ddseq())
		} else if md.setvbuuid(vbuuid).vbuuid() != vbuuid {
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
			if mvalue != mval {
				t.Errorf("expected %v, for %v", mvalue, mval)
			} else if level != lvl {
				t.Errorf("expected %v, for %v", level, lvl)
			}
		}
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
