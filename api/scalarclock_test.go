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

	// Updates
	ref := "10"
	testcases := []interface{}{
		int8(10), uint8(10), int16(10), uint16(10),
		int32(10), uint32(10), int64(10), uint64(10), int(10), uint(10),
	}
	for _, tcase := range testcases {
		out := Scalarclock(0).Update(tcase).(Scalarclock).String()
		if out != ref {
			t.Errorf("expected %q, got %q", ref, out)
		}
	}
}

func TestScalarClockJson(t *testing.T) {
	ref := Scalarclock(10)
	buf := ref.JSONMarshal(nil)
	clock, err := ref.JSONUnmarshal(buf)
	if err != nil {
		t.Error(err)
	} else if clock != ref {
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
	// unmarshal corner case
	if ref, out := "0", ref.Unmarshal(nil).(Scalarclock).String(); ref != out {
		t.Errorf("expected %v, got %v", ref, out)
	}
}
