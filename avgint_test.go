package storage

import "testing"

func TestAverageInt(t *testing.T) {
	avg := &averageInt{}
	for i := 1; i <= 100; i++ {
		avg.add(int64(i))
	}
	if x, y := int64(1), avg.min(); x != y {
		t.Errorf("min() expected %v, got %v", x, y)
	} else if x, y := int64(100), avg.max(); x != y {
		t.Errorf("max() expected %v, got %v", x, y)
	} else if x, y := int64(100), avg.samples(); x != y {
		t.Errorf("samples() expected %v, got %v", x, y)
	} else if x, y := int64(100*101)/2, avg.total(); x != y {
		t.Errorf("total() expected %v, got %v", x, y)
	} else if x, y := avg.total()/avg.samples(), avg.mean(); x != y {
		t.Errorf("mean() expected %v, got %v", x, y)
	} else if x, y := 883.5, avg.variance(); x != y {
		t.Errorf("variance() expected %v, got %v", x, y)
	} else if x, y := 29.723727895403698, avg.sd(); x != y {
		t.Errorf("sd() expected %v, got %v", x, y)
	}
}

func BenchmarkAvgintAdd(b *testing.B) {
	avg := &averageInt{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
}

func BenchmarkAvgintCount(b *testing.B) {
	avg := &averageInt{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.samples()
	}
}

func BenchmarkAvgintSum(b *testing.B) {
	avg := &averageInt{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.total()
	}
}

func BenchmarkAvgintMean(b *testing.B) {
	avg := &averageInt{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.mean()
	}
}

func BenchmarkAvgintVar(b *testing.B) {
	avg := &averageInt{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.variance()
	}
}

func BenchmarkAvgintSd(b *testing.B) {
	avg := &averageInt{}
	for i := 0; i <= b.N; i++ {
		avg.add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.sd()
	}
}
