package lib

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestFindFirstSet8(t *testing.T) {
	if x := Bit8(0).Findfirstset(); x != -1 {
		t.Errorf("expected %v, got %v", -1, x)
	} else if x = Bit8(0x80).Findfirstset(); x != 7 {
		t.Errorf("expected %v, got %v", 7, x)
	} else if x = Bit8(0x10).Findfirstset(); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	}
}

func TestClearbit8(t *testing.T) {
	for i := uint8(0); i < 8; i++ {
		if x := Bit8(1 << i).Clearbit(uint8(i)); x != 0 {
			t.Errorf("expected %v, got %v", 0, x)
		}
	}
}

func TestSetbit8(t *testing.T) {
	for i := uint8(0); i < 8; i++ {
		if x := Bit8(0).Setbit(uint8(i)); x != uint8(1<<i) {
			t.Errorf("expected %v, got %v", uint8(1<<i), x)
		}
	}
}

func TestZerosin8(t *testing.T) {
	if x := Bit8(0).Zeros(); x != 8 {
		t.Errorf("expected %v, got %v", 8, x)
	} else if x = Bit8(0xaa).Zeros(); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	} else if x = Bit8(0x55).Zeros(); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	}
}

func BenchmarkFindFSet8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Bit8(0x80).Findfirstset()
	}
}

func BenchmarkClearbit8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Bit8(0x80).Clearbit(7)
	}
}

func BenchmarkSetbit8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Bit8(0x80).Setbit(7)
	}
}

func BenchmarkZerosin8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Bit8(0xaa).Zeros()
	}
}
