package api

import "testing"

func TestKeyflags(t *testing.T) {
	f := Keyflags(0)
	f = f.Setblack().Setdirty().Setdeleted()
	if f.Isblack() == false {
		t.Errorf("unexpected false")
	} else if f.Isdirty() == false {
		t.Errorf("unexpected false")
	} else if f.Isdeleted() == false {
		t.Errorf("unexpected false")
	} else if f.Cleardirty().Isdirty() == true {
		t.Errorf("unexpected true")
	} else if f.Cleardeleted().Isdeleted() == true {
		t.Errorf("unexpected true")
	} else if f.Togglelink().Isblack() == true {
		t.Errorf("unexpected true")
	} else if f.Togglelink().Isred() == false {
		t.Errorf("unexpected false")
	} else if f.Setred().Isred() == false {
		t.Errorf("unexpected false")
	}
}
