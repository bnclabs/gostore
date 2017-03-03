package bubt

import "testing"

func TestZFlags(t *testing.T) {
	var flags zentryFlags
	flags = flags.setvalfile()
	if flags.isvalfile() == false {
		t.Errorf("expected true")
	}
	flags = flags.setdeleted()
	if flags.isdeleted() == false {
		t.Errorf("expected true")
	}
	flags = flags.clearvalfile()
	if flags.isvalfile() == true {
		t.Errorf("expected false")
	}
	flags = flags.cleardeleted()
	if flags.isdeleted() == true {
		t.Errorf("expected false")
	}
}
