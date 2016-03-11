package storage

import "testing"
import "fmt"
import "reflect"

var _ = fmt.Sprintf("dummy")

func TestHistogramInt(t *testing.T) {
	h := newhistorgramInt64(3, 97, 3)
	for i := 1; i <= 100; i++ {
		h.add(int64(i))
	}

	if x, y := int64(1), h.min(); x != y {
		t.Errorf("min() expected %v, got %v", x, y)
	} else if x, y := int64(100), h.max(); x != y {
		t.Errorf("max() expected %v, got %v", x, y)
	} else if x, y := int64(100), h.samples(); x != y {
		t.Errorf("samples() expected %v, got %v", x, y)
	} else if x, y := int64(100*101)/2, h.total(); x != y {
		t.Errorf("total() expected %v, got %v", x, y)
	} else if x, y := h.total()/h.samples(), h.mean(); x != y {
		t.Errorf("mean() expected %v, got %v", x, y)
	} else if x, y := 883.5, h.variance(); x != y {
		t.Errorf("variance() expected %v, got %v", x, y)
	} else if x, y := 29.723727895403698, h.sd(); x != y {
		t.Errorf("sd() expected %v, got %v", x, y)
	}

	// check histogram
	samples := []int64{0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17}

	ref := map[string]int64{"12": 11, "15": 14, ">": 17, "6": 6, "9": 8}
	h = newhistorgramInt64(6, 15, 3)
	for _, sample := range samples {
		h.add(sample)
	}
	if data := h.stats(); reflect.DeepEqual(ref, data) == false {
		t.Errorf("expected %v, got %v", ref, data)
	}

	ref = map[string]int64{"12": 11, "15": 14, ">": 17, "6": 6, "3": 3, "9": 8}
	h = newhistorgramInt64(3, 16, 3)
	for _, sample := range samples {
		h.add(sample)
	}
	if data := h.stats(); reflect.DeepEqual(ref, data) == false {
		t.Errorf("expected %v, got %v", ref, data)
	}

	ref = map[string]int64{"9": 8, "12": 11, "0": 0, "3": 3, "6": 6, ">": 17}
	h = newhistorgramInt64(2, 14, 3)
	for _, sample := range samples {
		h.add(sample)
	}
	if data := h.stats(); !reflect.DeepEqual(ref, data) {
		t.Errorf("expected %v, got %v", ref, data)
	}
}

func BenchmarkHtgintAdd(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)

	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
}

func BenchmarkHtgintCount(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.samples()
	}
}

func BenchmarkHtgintSum(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.total()
	}
}

func BenchmarkHtgintMean(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.mean()
	}
}

func BenchmarkHtgintVar(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.variance()
	}
}

func BenchmarkHtgintSd(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.sd()
	}
}

func BenchmarkHtgintStats(b *testing.B) {
	htg := newhistorgramInt64(1, int64(b.N), 5)
	for i := 0; i <= b.N; i++ {
		htg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		htg.stats()
	}
}
