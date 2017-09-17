package llrb

import "testing"

func TestMaxheight(t *testing.T) {
	if maxheight(0) != 0 {
		t.Errorf("unexpected %v", maxheight(0))
	} else if maxheight(1) != 3 {
		t.Errorf("unexpected %v", maxheight(1))
	} else if maxheight(2) != 6 {
		t.Errorf("unexpected %v", maxheight(2))
	} else if maxheight(5) != 4.643856189774724 {
		t.Errorf("unexpected %v", maxheight(5))
	} else if maxheight(99) != 13.258713240159219 {
		t.Errorf("unexpected %v", maxheight(99))
	}
}
