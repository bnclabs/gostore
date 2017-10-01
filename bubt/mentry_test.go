package bubt

import "testing"

func TestMentry(t *testing.T) {
	ze := mentry(make([]byte, zentrysize))
	keylen := uint64(0x234567812345678)
	if ze.setkeylen(keylen); ze.keylen() != keylen {
		t.Errorf("expected %x, got %x", keylen, ze.keylen())
	}
	vpos := uint64(0x12345678)
	if ze.setvpos(vpos); ze.vpos() != vpos {
		t.Errorf("expected %x, got %x", vpos, ze.vpos())
	}
}
