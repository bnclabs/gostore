package lib

import "math"

// AverageInt64 compute statistical mean, median and variance.
type AverageInt64 struct {
	n      int64
	minval int64
	maxval int64
	sum    int64
	sumsq  float64
	init   bool
}

// Add a sample.
func (av *AverageInt64) Add(sample int64) {
	av.n++
	av.sum += sample
	f := float64(sample)
	av.sumsq += f * f
	if av.init == false || sample < av.minval {
		av.minval = sample
		av.init = true
	}
	if av.maxval < sample {
		av.maxval = sample
	}
}

func (av *AverageInt64) Min() int64 {
	return av.minval
}

func (av *AverageInt64) Max() int64 {
	return av.maxval
}

func (av *AverageInt64) Samples() int64 {
	return av.n
}

func (av *AverageInt64) Sum() int64 {
	return av.sum
}

func (av *AverageInt64) Mean() int64 {
	if av.n == 0 {
		return 0
	}
	return int64(float64(av.sum) / float64(av.n))
}

func (av *AverageInt64) Variance() float64 {
	if av.n == 0 {
		return 0
	}
	n_f, mean_f := float64(av.n), float64(av.Mean())
	return (av.sumsq / n_f) - (mean_f * mean_f)
}

func (av *AverageInt64) SD() float64 {
	if av.n == 0 {
		return 0
	}
	return math.Sqrt(av.Variance())
}

func (av *AverageInt64) Clone() *AverageInt64 {
	newav := (*av)
	return &newav
}

func (av *AverageInt64) stats() map[string]interface{} {
	stats := map[string]interface{}{
		"samples":     av.Samples(),
		"min":         av.Min(),
		"max":         av.Max(),
		"mean":        av.Mean(),
		"variance":    av.Variance(),
		"stddeviance": av.SD(),
	}
	return stats
}
