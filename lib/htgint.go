package lib

import "math"
import "sort"
import "fmt"
import "strings"
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

// Add a sample to this histogram.
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

// Min return minimum value from sample.
func (h *HistogramInt64) Min() int64 {
	return h.minval
}

// Max return maximum value from sample.
func (h *HistogramInt64) Max() int64 {
	return h.maxval
}

// Samples return total number of samples in the set.
func (h *HistogramInt64) Samples() int64 {
	return h.n
}

// Sum return the sum of all sample values.
func (h *HistogramInt64) Sum() int64 {
	return h.sum
}

// Mean return the average value of all samples.
func (h *HistogramInt64) Mean() int64 {
	if h.n == 0 {
		return 0
	}
	return int64(float64(h.sum) / float64(h.n))
}

// Variance return the squared deviation of a random sample from
// its mean.
func (h *HistogramInt64) Variance() int64 {
	if h.n == 0 {
		return 0
	}
	nF, meanF := float64(h.n), float64(h.Mean())
	return int64((h.sumsq / nF) - (meanF * meanF))
}

// SD return by how much the samples differ from the mean value of
// sample set.
func (h *HistogramInt64) SD() int64 {
	if h.n == 0 {
		return 0
	}
	return int64(math.Sqrt(float64(h.Variance())))
}

// Clone copies the entire instance.
func (h *HistogramInt64) Clone() *HistogramInt64 {
	newh := *h
	newh.histogram = make([]int64, len(h.histogram))
	copy(newh.histogram, h.histogram)
	return &newh
}

// Stats return a map of histogram.
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

// Fullstats includes mean,variance,stddeviance in the Stats().
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

// Logstring return Fullstats as loggable string.
func (h *HistogramInt64) Logstring() string {
	stats, keys := h.Fullstats(), []string{}
	// everything except histogram
	for k := range stats {
		if k == "histogram" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ss := []string{}
	for _, key := range keys {
		ss = append(ss, fmt.Sprintf(`"%v": %v`, key, stats[key]))
	}
	// sort historgram
	hkeys := []int{}
	histogram := stats["histogram"].(map[string]interface{})
	for k := range histogram {
		if k == "+" {
			continue
		}
		n, _ := strconv.Atoi(k)
		hkeys = append(hkeys, n)
	}
	sort.Ints(hkeys)
	hs := []string{}
	for _, k := range hkeys {
		ks := strconv.Itoa(k)
		hs = append(hs, fmt.Sprintf(`"%v": %v`, ks, histogram[ks]))
	}
	hs = append(hs, fmt.Sprintf(`"%v": %v`, "+", histogram["+"]))
	s := "{" + strings.Join(hs, ",") + "}"
	ss = append(ss, fmt.Sprintf(`"histogram": %v`, s))
	return "{" + strings.Join(ss, ",") + "}"
}
