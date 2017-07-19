package lib

import "math"

// AverageInt64 compute statistical mean, median and variance for a
// sample set of int64 numbers.
type AverageInt64 struct {
	n      int64
	minval int64
	maxval int64
	sum    int64
	sumsq  float64
	init   bool
}

// Add a new sample.
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

// Min return minimum value from sample.
func (av *AverageInt64) Min() int64 {
	return av.minval
}

// Max return maximum value from sample.
func (av *AverageInt64) Max() int64 {
	return av.maxval
}

// Samples return total number of samples in the set.
func (av *AverageInt64) Samples() int64 {
	return av.n
}

// Sum return the sum of all sample values.
func (av *AverageInt64) Sum() int64 {
	return av.sum
}

// Mean return the average value of all samples.
func (av *AverageInt64) Mean() int64 {
	if av.n == 0 {
		return 0
	}
	return int64(float64(av.sum) / float64(av.n))
}

// Variance return the squared deviation of a random sample from
// its mean.
func (av *AverageInt64) Variance() int64 {
	if av.n == 0 {
		return 0
	}
	nF, meanF := float64(av.n), float64(av.Mean())
	return int64((av.sumsq / nF) - (meanF * meanF))
}

// SD return by how much the samples differ from the mean value of
// sample set.
func (av *AverageInt64) SD() int64 {
	if av.n == 0 {
		return 0
	}
	return int64(math.Sqrt(float64(av.Variance())))
}

// Clone copies the entire instance.
func (av *AverageInt64) Clone() *AverageInt64 {
	newav := (*av)
	return &newav
}

// Stats return a map of statistics.
func (av *AverageInt64) Stats() map[string]interface{} {
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
