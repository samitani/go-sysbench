package runner

import (
	"fmt"
	"math"
	"strings"
	"sync"
)

type Histogram struct {
	size     int
	rangeMin float64
	rangeMax float64

	rangeMult   float64
	rangeDeduct float64

	array []int
	mu    sync.Mutex
}

func NewHistogram(size int, rangeMin, rangeMax float64) *Histogram {
	rangeDeduct := math.Log(rangeMin)
	rangeMult := float64(size-1) / (math.Log(rangeMax) - rangeDeduct)

	return &Histogram{
		rangeMin:    rangeMin,
		rangeMax:    rangeMax,
		rangeMult:   rangeMult,
		rangeDeduct: rangeDeduct,
		size:        size,
		array:       make([]int, size),
	}
}

func (h *Histogram) Add(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	i := int(math.Ceil((math.Log(value) - h.rangeDeduct) * h.rangeMult))

	if i < 0 {
		i = 0
	} else if i >= h.size {
		i = h.size - 1
	}

	h.array[i] += 1
}

func (h *Histogram) Print() {
	fmt.Println("       value  ------------- distribution ------------- count")

	h.mu.Lock()
	defer h.mu.Unlock()

	var maxcnt int = 0
	for _, c := range h.array {
		if c > maxcnt {
			maxcnt = c
		}
	}

	for i, c := range h.array {
		if c == 0 {
			continue
		}

		width := int(math.Floor(float64(c*40/maxcnt) + 0.5))

		fmt.Printf("%12.3f |%-40s %d\n",
			h.decimal(i),               /* value */
			strings.Repeat("*", width), /* distribution */
			c)                          /* count */
	}
}

func (h *Histogram) totalCount() int {
	var total int = 0

	for _, c := range h.array {
		if c == 0 {
			continue
		}
		total += c
	}
	return total

}

func (h *Histogram) percentile(p int) float64 {
	if p < 0 || p > 100 {
		return 0
	}
	if h.totalCount() == 0 {
		return 0
	}
	nmax := int(math.Ceil(float64(h.totalCount()*p) / 100.0))

	var i, c, cumulative int
	for i, c = range h.array {
		if c == 0 {
			continue
		}
		cumulative += c
		if cumulative >= nmax {
			break
		}
	}
	return h.decimal(i)
}

func (h *Histogram) Percentile(p int) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.percentile(p)
}

// atomic function to get percentile and clear values
func (h *Histogram) GetPercentileAndReset(p int) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()

	retval := h.percentile(p)
	h.array = make([]int, h.size)

	return retval
}

func (h *Histogram) decimal(i int) float64 {
	return math.Exp((float64(i) / h.rangeMult) + h.rangeDeduct)
}
