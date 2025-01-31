package runner

import (
	"testing"
	// "fmt"
)

func TestHistogramAdd(t *testing.T) {
	h := NewHistogram(1024, 0.001, 100000)

	h.Add(0.002)
	h.Add(0.01)
	h.Add(1.0)
	h.Add(500.0)
	h.Add(500.0)
	h.Add(99999.0)

	expectedTotal := 6
	total := h.TotalCount()

	if total != expectedTotal {
		t.Errorf("Expected total count %d, got %d", expectedTotal, total)
	}
}

func TestGetPercentile(t *testing.T) {
	h := NewHistogram(128, 0.001, 100000)

	h.Add(0.0002)
	h.Add(0.01)
	h.Add(1.0)
	h.Add(500.0)
	h.Add(99999.0)

	p50 := h.Percentile(50)
	p100 := h.Percentile(100)

	if p50 <= 0.01 {
		t.Errorf("Expected positive 50th percentile, got %f", p50)
	}

	if p100 <= 99999.0 {
		t.Errorf("Expected 100th percentile to be 99999.0, got %f", p100)
	}
}

func TestPrint(t *testing.T) {
	h := NewHistogram(1024, 0.001, 100000)

	h.Add(0.002)
	h.Add(0.01)
	h.Add(0.01)
	h.Add(1.0)
	h.Add(500.0)
	h.Add(99999.0)

	h.Print()
}
