package bubt

import "testing"

func TestMEntry(t *testing.T) {
	me := mentry(make([]byte, 1024))

	klen, vpos, rpos := uint16(0x1234), uint64(0x12345668), uint64(0x98776654)
	me.setkeylen(klen).setvaluepos(vpos).setreducepos(rpos)
	if me.keylen() != klen {
		t.Errorf("unexpected %x", me.keylen())
	} else if me.valuepos() != vpos {
		t.Errorf("unexpected %x", me.valuepos())
	} else if me.reducepos() != rpos {
		t.Errorf("unexpected %x", me.reducepos())
	}
}
