package api

import "testing"

func TestLookupones(t *testing.T) {
	countones := func(b uint8) (c byte) {
		for c = 0; b != 0; b >>= 1 { // count set bits
			c += byte(b & 1)
		}
		return c
	}
	for i, val := range lookupones {
		if countones(uint8(i)) != byte(val) {
			t.Errorf("expected %v, got %v", countones(uint8(i)), val)
		}
	}
}

func TestKeymask(t *testing.T) {
	km := Keymask(0)
	if km.IsTxn() == true {
		t.Errorf("unexpected true")
	} else if km.IsValue() == true {
		t.Errorf("unexpected true")
	} else if km.IsBornseqno() == true {
		t.Errorf("unexpected true")
	} else if km.IsDeadseqno() == true {
		t.Errorf("unexpected true")
	} else if km.IsUuid() == true {
		t.Errorf("unexpected true")
	} else if km.SetTxn().IsTxn() == false {
		t.Errorf("unexpected false")
	} else if km.SetValue().IsValue() == false {
		t.Errorf("unexpected false")
	} else if km.SetBornseqno().IsBornseqno() == false {
		t.Errorf("unexpected false")
	} else if km.SetDeadseqno().IsDeadseqno() == false {
		t.Errorf("unexpected false")
	} else if km.SetUuid().IsUuid() == false {
		t.Errorf("unexpected false")
	}
}
