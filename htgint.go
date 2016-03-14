package storage

import "math"
import "strconv"

type histogramInt64 struct {
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

func newhistorgramInt64(from, till, width int64) *histogramInt64 {
	from = (from / width) * width
	till = (till / width) * width
	h := &histogramInt64{from: from, till: till, width: width}
	h.histogram = make([]int64, 1+((till-from)/width)+1)
	return h
}

func (h *histogramInt64) add(sample int64) {
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

func (h *histogramInt64) min() int64 {
	return h.minval
}

func (h *histogramInt64) max() int64 {
	return h.maxval
}

func (h *histogramInt64) samples() int64 {
	return h.n
}

func (h *histogramInt64) total() int64 {
	return h.sum
}

func (h *histogramInt64) mean() int64 {
	if h.n == 0 {
		return 0
	}
	return int64(float64(h.sum) / float64(h.n))
}

func (h *histogramInt64) variance() float64 {
	if h.n == 0 {
		return 0
	}
	n_f, mean_f := float64(h.n), float64(h.mean())
	return (h.sumsq / n_f) - (mean_f * mean_f)
}

func (h *histogramInt64) sd() float64 {
	if h.n == 0 {
		return 0
	}
	return math.Sqrt(h.variance())
}

func (h *histogramInt64) stats() map[string]int64 {
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

func (h *histogramInt64) fullstats() map[string]interface{} {
	hmap := make(map[string]interface{})
	for k, v := range h.stats() {
		hmap[k] = v
	}
	return map[string]interface{}{
		"samples":     h.samples(),
		"min":         h.min(),
		"max":         h.max(),
		"mean":        h.mean(),
		"variance":    h.variance(),
		"stddeviance": h.sd(),
		"histogram":   hmap,
	}
}

func (h *histogramInt64) clone() *histogramInt64 {
	newh := *h
	newh.histogram = make([]int64, len(h.histogram))
	copy(newh.histogram, h.histogram)
	return &newh
}
