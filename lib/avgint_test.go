package lib

import "testing"

func TestAverageInt(t *testing.T) {
	avg := &AverageInt64{}

	if mean := avg.Mean(); mean != 0 {
		t.Errorf("expected 0, got %v", mean)
	} else if variance := avg.Variance(); variance != 0 {
		t.Errorf("expected 0, got %v", variance)
	} else if sd := avg.SD(); sd != 0 {
		t.Errorf("expected 0, got %v", sd)
	}

	// start populating.
	for i := 1; i <= 100; i++ {
		avg.Add(int64(i))
	}
	// validate
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
	// stats
	stats := avg.Stats()
	if x, y := int64(1), stats["min"].(int64); x != y {
		t.Errorf("Min() expected %v, got %v", x, y)
	} else if x, y := int64(100), stats["max"].(int64); x != y {
		t.Errorf("Max() expected %v, got %v", x, y)
	} else if x, y := int64(100), stats["samples"].(int64); x != y {
		t.Errorf("Samples() expected %v, got %v", x, y)
	}
	x, y := avg.Sum()/stats["samples"].(int64), stats["mean"].(int64)
	if x != y {
		t.Errorf("expected %v, got %v", x, y)
	}
	if x, y := avg.Sum()/avg.Samples(), avg.Mean(); x != y {
		t.Errorf("Mean() expected %v, got %v", x, y)
	} else if x, y := int64(883), stats["variance"].(int64); x != y {
		t.Errorf("Variance() expected %v, got %v", x, y)
	} else if x, y := int64(29), stats["stddeviance"].(int64); x != y {
		t.Errorf("SD() expected %v, got %v", x, y)
	}

	// clone
	newavg := avg.Clone()
	// validate
	if x, y := int64(1), newavg.Min(); x != y {
		t.Errorf("Min() expected %v, got %v", x, y)
	} else if x, y := int64(100), newavg.Max(); x != y {
		t.Errorf("Max() expected %v, got %v", x, y)
	} else if x, y := int64(100), newavg.Samples(); x != y {
		t.Errorf("Samples() expected %v, got %v", x, y)
	} else if x, y := int64(100*101)/2, newavg.Sum(); x != y {
		t.Errorf("Sum() expected %v, got %v", x, y)
	} else if x, y := newavg.Sum()/newavg.Samples(), newavg.Mean(); x != y {
		t.Errorf("Mean() expected %v, got %v", x, y)
	} else if x, y := int64(883), newavg.Variance(); x != y {
		t.Errorf("Variance() expected %v, got %v", x, y)
	} else if x, y := int64(29), newavg.SD(); x != y {
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
