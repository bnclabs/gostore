package bubt

import "testing"

func TestZentry(t *testing.T) {
	ze := zentry(make([]byte, zentrysize))
	// test delete flag
	if ze.setdeleted(); ze.isdeleted() == false {
		t.Errorf("unexpected false")
	} else if ze.cleardeleted(); ze.isdeleted() == true {
		t.Errorf("unexpected true")
	}
	// test vlog flag
	if ze.setvlog(); ze.isvlog() == false {
		t.Errorf("unexpected false")
	} else if ze.clearvlog(); ze.isvlog() == true {
		t.Errorf("unexpected true")
	}

	seqno := uint64(0x234567812345678)
	if ze.setseqno(seqno); ze.seqno() != seqno {
		t.Errorf("expected %x, got %x", seqno, ze.seqno())
	}
	keylen := uint64(0x1234)
	if ze.setkeylen(keylen); ze.keylen() != keylen {
		t.Errorf("expected %x, got %x", keylen, ze.keylen())
	}
	valuelen := uint64(0x12345678)
	if ze.setvaluelen(valuelen); ze.valuelen() != valuelen {
		t.Errorf("expected %x, got %x", valuelen, ze.valuelen())
	}
}
