package api

import "testing"

func TestScalarClock(t *testing.T) {
	clock := ScalarClock(10)
	if rclock := clock.Update(nil); rclock != clock {
		t.Errorf("expected %v, got %v", clock, rclock)
	} else if rclock := clock.Update(10); rclock != 20 {
		t.Errorf("expected %v, got %v", 20, rclock)
	} else if rclock := clock.Clone(); rclock != clock {
		t.Errorf("expected %v, got %v", clock, rclock)
	} else if ok := clock.Less(10); ok == true {
		t.Errorf("expected %v, got %v", false, true)
	} else if ok := clock.Less(9); ok == false {
		t.Errorf("expected %v, got %v", true, false)
	} else if ok := clock.LessEqual(10); ok == false {
		t.Errorf("expected %v, got %v", true, false)
	}
}
