package llrb

import "math"

type averageInt struct {
	n      int64
	minval int64
	maxval int64
	sum    int64
	sumsq  float64
	init   bool
}

func (av *averageInt) add(sample int64) {
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

func (av *averageInt) min() int64 {
	return av.minval
}

func (av *averageInt) max() int64 {
	return av.maxval
}

func (av *averageInt) samples() int64 {
	return av.n
}

func (av *averageInt) total() int64 {
	return av.sum
}

func (av *averageInt) mean() int64 {
	return int64(float64(av.sum) / float64(av.n))
}

func (av *averageInt) variance() float64 {
	n_f, mean_f := float64(av.n), float64(av.mean())
	return (av.sumsq / n_f) - (mean_f * mean_f)
}

func (av *averageInt) sd() float64 {
	return math.Sqrt(av.variance())
}
