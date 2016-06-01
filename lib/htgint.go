package lib

import "math"
import "strconv"

// HistogramInt64 statistical histogram.
type HistogramInt64 struct {
	// stats
	n         int64
	minval    int64
	maxval    int64
	sum       int64
	sumsq     float64
	histogram []int64
	// setup
	init  bool
	from  int64
	till  int64
	width int64
}

// NewhistorgramInt64 return a new histogram object.
func NewhistorgramInt64(from, till, width int64) *HistogramInt64 {
	from = (from / width) * width
	till = (till / width) * width
	h := &HistogramInt64{from: from, till: till, width: width}
	h.histogram = make([]int64, 1+((till-from)/width)+1)
	return h
}

func (h *HistogramInt64) Add(sample int64) {
	h.n++
	h.sum += sample
	f := float64(sample)
	h.sumsq += f * f
	if h.init == false || sample < h.minval {
		h.minval = sample
		h.init = true
	}
	if h.maxval < sample {
		h.maxval = sample
	}

	if sample < h.from {
		h.histogram[0]++
	} else if sample >= h.till {
		h.histogram[len(h.histogram)-1]++
	} else {
		h.histogram[((sample-h.from)/h.width)+1]++
	}
}

func (h *HistogramInt64) Min() int64 {
	return h.minval
}

func (h *HistogramInt64) Max() int64 {
	return h.maxval
}

func (h *HistogramInt64) Samples() int64 {
	return h.n
}

func (h *HistogramInt64) Sum() int64 {
	return h.sum
}

func (h *HistogramInt64) Mean() int64 {
	if h.n == 0 {
		return 0
	}
	return int64(float64(h.sum) / float64(h.n))
}

func (h *HistogramInt64) Variance() float64 {
	if h.n == 0 {
		return 0
	}
	n_f, mean_f := float64(h.n), float64(h.Mean())
	return (h.sumsq / n_f) - (mean_f * mean_f)
}

func (h *HistogramInt64) SD() float64 {
	if h.n == 0 {
		return 0
	}
	return math.Sqrt(h.Variance())
}

func (h *HistogramInt64) Stats() map[string]int64 {
	m := make(map[string]int64)
	cumm := int64(0)
	for i := len(h.histogram) - 1; i >= 0; i-- {
		if h.histogram[i] == 0 {
			continue
		}
		for j := 0; j <= i; j++ {
			v := h.histogram[j]
			key := strconv.Itoa(int(h.from + (int64(j) * h.width)))
			cumm += v
			if j == i {
				m["+"] = cumm
			} else {
				m[key] = cumm
			}
		}
		break
	}
	return m
}

func (h *HistogramInt64) Fullstats() map[string]interface{} {
	hmap := make(map[string]interface{})
	for k, v := range h.Stats() {
		hmap[k] = v
	}
	return map[string]interface{}{
		"samples":     h.Samples(),
		"min":         h.Min(),
		"max":         h.Max(),
		"mean":        h.Mean(),
		"variance":    h.Variance(),
		"stddeviance": h.SD(),
		"histogram":   hmap,
	}
}

func (h *HistogramInt64) Clone() *HistogramInt64 {
	newh := *h
	newh.histogram = make([]int64, len(h.histogram))
	copy(newh.histogram, h.histogram)
	return &newh
}
