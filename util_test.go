package llrb

import "testing"
import "fmt"

var _ = fmt.Sprintf("dummy")

func TestFindFSet8(t *testing.T) {
	if x := findfirstset8(uint8(0)); x != -1 {
		t.Errorf("expected %v, got %v", -1, x)
	} else if x = findfirstset8(uint8(0x80)); x != 7 {
		t.Errorf("expected %v, got %v", 7, x)
	} else if x = findfirstset8(uint8(0x10)); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	}
}

func TestClearbit8(t *testing.T) {
	for i := uint8(0); i < 8; i++ {
		if x := clearbit8(uint8(1<<i), uint8(i)); x != 0 {
			t.Errorf("expected %v, got %v", 0, x)
		}
	}
}

func TestSetbit8(t *testing.T) {
	for i := uint8(0); i < 8; i++ {
		if x := setbit8(0, uint8(i)); x != uint8(1<<i) {
			t.Errorf("expected %v, got %v", uint8(1<<i), x)
		}
	}
}

func TestZerosin8(t *testing.T) {
	if x := zerosin8(0); x != 8 {
		t.Errorf("expected %v, got %v", 8, x)
	} else if x = zerosin8(0xaa); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	} else if x = zerosin8(0x55); x != 4 {
		t.Errorf("expected %v, got %v", 4, x)
	}
}

func TestZerosin32(t *testing.T) {
	if x := zerosin32(0); x != 32 {
		t.Errorf("expected %v, got %v", 0, x)
	} else if x := zerosin32(1); x != 31 {
		t.Errorf("expected %v, got %v", 32, x)
	} else if x = zerosin32(0xaaaaaaaa); x != 16 {
		t.Errorf("expected %v, got %v", 16, x)
	} else if x = zerosin32(0x55555555); x != 16 {
		t.Errorf("expected %v, got %v", 16, x)
	}
}

func BenchmarkFindFSet8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		findfirstset8(0x80)
	}
}

func BenchmarkClearbit8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		clearbit8(0x80, 7)
	}
}

func BenchmarkSetbit8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		setbit8(0x80, 7)
	}
}

func BenchmarkZerosin8(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zerosin8(0xaa)
	}
}

func BenchmarkZerosin32(b *testing.B) {
	for i := 0; i < b.N; i++ {
		zerosin32(0xaaaaaaaa)
	}
}
