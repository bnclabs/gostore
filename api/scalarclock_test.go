package api

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestScalarClock(t *testing.T) {
	clock := Scalarclock(10)
	refclk := Scalarclock(20)
	if rclock := clock.Update(nil); rclock != clock {
		t.Errorf("expected %v, got %v", clock, rclock)
	} else if rclock := clock.Update(10); rclock != refclk {
		t.Errorf("expected %v, got %v", refclk, rclock)
	} else if rclock := clock.Clone(); rclock != clock {
		t.Errorf("expected %v, got %v", clock, rclock)
	} else if ok := clock.Less(Scalarclock(10)); ok == true {
		t.Errorf("expected %v, got %v", false, true)
	} else if ok := clock.Less(Scalarclock(9)); ok == true {
		t.Errorf("expected %v, got %v", true, ok)
	} else if ok := clock.LessEqual(Scalarclock(10)); ok == false {
		t.Errorf("expected %v, got %v", true, false)
	}
}

func TestScalarClockJson(t *testing.T) {
	ref := Scalarclock(10)
	buf := ref.JSONMarshal(nil)
	clock := ref.JSONUnmarshal(buf)
	if clock != ref {
		t.Errorf("expected %v, got %v", ref, clock)
	}
}

func TestScalarClockBinary(t *testing.T) {
	ref := Scalarclock(10)
	buf := ref.Marshal(nil)
	clock := ref.Unmarshal(buf)
	if clock != ref {
		t.Errorf("expected %v, got %v", ref, clock)
	}
}
