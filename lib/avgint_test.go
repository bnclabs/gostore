package lib

import "testing"

func TestAverageInt(t *testing.T) {
	avg := &AverageInt64{}
	for i := 1; i <= 100; i++ {
		avg.Add(int64(i))
	}
	if x, y := int64(1), avg.Min(); x != y {
		t.Errorf("Min() expected %v, got %v", x, y)
	} else if x, y := int64(100), avg.Max(); x != y {
		t.Errorf("Max() expected %v, got %v", x, y)
	} else if x, y := int64(100), avg.Samples(); x != y {
		t.Errorf("Samples() expected %v, got %v", x, y)
	} else if x, y := int64(100*101)/2, avg.Sum(); x != y {
		t.Errorf("Sum() expected %v, got %v", x, y)
	} else if x, y := avg.Sum()/avg.Samples(), avg.Mean(); x != y {
		t.Errorf("Mean() expected %v, got %v", x, y)
	} else if x, y := int64(883), avg.Variance(); x != y {
		t.Errorf("Variance() expected %v, got %v", x, y)
	} else if x, y := int64(29), avg.SD(); x != y {
		t.Errorf("SD() expected %v, got %v", x, y)
	}
}

func BenchmarkAvgintAdd(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.Add(int64(i))
	}
}

func BenchmarkAvgintCount(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.Samples()
	}
}

func BenchmarkAvgintSum(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.Sum()
	}
}

func BenchmarkAvgintMean(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.Mean()
	}
}

func BenchmarkAvgintVar(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.Variance()
	}
}

func BenchmarkAvgintSd(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= b.N; i++ {
		avg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.SD()
	}
}

func BenchmarkAvgclone(b *testing.B) {
	avg := &AverageInt64{}
	for i := 0; i <= 1000; i++ {
		avg.Add(int64(i))
	}
	b.ResetTimer()
	for i := 0; i <= b.N; i++ {
		avg.Clone()
	}
}
