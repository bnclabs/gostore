package lib

import "testing"
import "fmt"
import "reflect"

var _ = fmt.Sprintf("dummy")

func TestHistogramInt(t *testing.T) {
	h := NewhistorgramInt64(3, 97, 3)
	for i := 1; i <= 100; i++ {
		h.Add(int64(i))
	}

	if x, y := int64(1), h.Min(); x != y {
		t.Errorf("Min() expected %v, got %v", x, y)
	} else if x, y := int64(100), h.Max(); x != y {
		t.Errorf("Max() expected %v, got %v", x, y)
	} else if x, y := int64(100), h.Samples(); x != y {
		t.Errorf("Samples() expected %v, got %v", x, y)
	} else if x, y := int64(100*101)/2, h.Sum(); x != y {
		t.Errorf("Sum() expected %v, got %v", x, y)
	} else if x, y := h.Sum()/h.Samples(), h.Mean(); x != y {
		t.Errorf("Mean() expected %v, got %v", x, y)
	} else if x, y := 883.5, h.Variance(); x != y {
		t.Errorf("Variance() expected %v, got %v", x, y)
	} else if x, y := 29.723727895403698, h.SD(); x != y {
		t.Errorf("SD() expected %v, got %v", x, y)
	}

	// check histogram
	samples := []int64{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17}

	ref := map[string]int64{"12": 11, "15": 14, "+": 17, "6": 6, "9": 8}
	h = NewhistorgramInt64(6, 15, 3)
	for _, sample := range samples {
		h.Add(sample)
	}
	if data := h.Stats(); reflect.DeepEqual(ref, data) == false {
		t.Errorf("expected %v, got %v", ref, data)
	}

	ref = map[string]int64{"12": 11, "15": 14, "+": 17, "6": 6, "3": 3, "9": 8}
	h = NewhistorgramInt64(3, 16, 3)
	for _, sample := range samples {
		h.Add(sample)
	}
	if data := h.Stats(); reflect.DeepEqual(ref, data) == false {
		t.Errorf("expected %v, got %v", ref, data)
	}

	ref = map[string]int64{"9": 8, "12": 11, "0": 0, "3": 3, "6": 6, "+": 17}
	h = NewhistorgramInt64(2, 14, 3)
	for _, sample := range samples {
		h.Add(sample)
	}
	if data := h.Stats(); !reflect.DeepEqual(ref, data) {
		t.Errorf("expected %v, got %v", ref, data)
	}
}

func BenchmarkHtgintAdd(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)

	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
}

func BenchmarkHtgintCount(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.Samples()
	}
}

func BenchmarkHtgintSum(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.Sum()
	}
}

func BenchmarkHtgintMean(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.Mean()
	}
}

func BenchmarkHtgintVar(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.Variance()
	}
}

func BenchmarkHtgintSd(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.SD()
	}
}

func BenchmarkHtgintStats(b *testing.B) {
	htg := NewhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.Stats()
	}
}
