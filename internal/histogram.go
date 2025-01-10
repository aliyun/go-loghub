package internal

import (
	"fmt"
	"math"

	"go.uber.org/atomic"
)

type TimeHistogram struct {
	Count     atomic.Int64
	Sum       atomic.Float64
	SumSquare atomic.Float64
}

func (h *TimeHistogram) AddSample(us float64) {
	h.Count.Inc()
	h.Sum.Add(us)
	h.SumSquare.Add(us * us)
}

func (h *TimeHistogram) String() string {
	avg := h.Avg()
	stdDev := h.StdDev()
	count := h.Count.Load()
	return fmt.Sprintf("{avg: %.1fus, stdDev: %.1fus, count: %d}", avg, stdDev, count)
}

func (h *TimeHistogram) Avg() float64 {
	count := h.Count.Load()
	if count == 0 {
		return 0
	}
	return h.Sum.Load() / float64(count)
}

func (h *TimeHistogram) StdDev() float64 {
	count := h.Count.Load()
	if count < 2 {
		return 0
	}
	div := float64(count * (count - 1))
	num := (float64(count) * h.SumSquare.Load()) - math.Pow(h.Sum.Load(), 2)
	return math.Sqrt(num / div)
}
