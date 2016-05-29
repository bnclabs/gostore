package storage

import "math"

type averageInt64 struct {
	n      int64
	minval int64
	maxval int64
	sum    int64
	sumsq  float64
	init   bool
}

func (av *averageInt64) add(sample int64) {
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

func (av *averageInt64) min() int64 {
	return av.minval
}

func (av *averageInt64) max() int64 {
	return av.maxval
}

func (av *averageInt64) samples() int64 {
	return av.n
}

func (av *averageInt64) total() int64 {
	return av.sum
}

func (av *averageInt64) mean() int64 {
	if av.n == 0 {
		return 0
	}
	return int64(float64(av.sum) / float64(av.n))
}

func (av *averageInt64) variance() float64 {
	if av.n == 0 {
		return 0
	}
	n_f, mean_f := float64(av.n), float64(av.mean())
	return (av.sumsq / n_f) - (mean_f * mean_f)
}

func (av *averageInt64) sd() float64 {
	if av.n == 0 {
		return 0
	}
	return math.Sqrt(av.variance())
}

func (av *averageInt64) clone() *averageInt64 {
	newav := (*av)
	return &newav
}

func (av *averageInt64) stats() map[string]interface{} {
	stats := map[string]interface{}{
		"samples":     av.samples(),
		"min":         av.min(),
		"max":         av.max(),
		"mean":        av.mean(),
		"variance":    av.variance(),
		"stddeviance": av.sd(),
	}
	return stats
}
