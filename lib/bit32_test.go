package lib

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestZerosin32(t *testing.T) {
	if x := Bit32(0).Zeros(); x != 32 {
		t.Errorf("expected %v, got %v", 0, x)
	} else if x := Bit32(1).Zeros(); x != 31 {
		t.Errorf("expected %v, got %v", 32, x)
	} else if x = Bit32(0xaaaaaaaa).Zeros(); x != 16 {
		t.Errorf("expected %v, got %v", 16, x)
	} else if x = Bit32(0x55555555).Zeros(); x != 16 {
		t.Errorf("expected %v, got %v", 16, x)
	}
}

func BenchmarkZerosin32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Bit32(0xaaaaaaaa).Zeros()
	}
}
