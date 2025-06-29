package sysbench

import (
	"fmt"
	"testing"
)

const (
	tHistogramSize = 1024
	tHistogramMin  = 0.001
	tHistogramMax  = 100000
)

func TestHistogramAdd(t *testing.T) {
	h := NewHistogram(tHistogramSize, tHistogramMin, tHistogramMax)

	h.Add(0.002)
	h.Add(0.01)
	h.Add(1.0)
	h.Add(500.0)
	h.Add(500.0)
	h.Add(99999.0)

	expectedTotal := 6
	total := h.totalCount()

	if total != expectedTotal {
		t.Errorf("Expected total count %d, got %d", expectedTotal, total)
	}
}

func TestHistogramAddOverValue(t *testing.T) {
	h := NewHistogram(tHistogramSize, tHistogramMin, tHistogramMax)

	h.Add(100000 + 1)

	p100 := h.Percentile(100)

	if fmt.Sprintf("%.3f", p100) != "100000.000" {
		t.Errorf("Expected 100th percentile to be 100000.0, got %f", p100)
	}
}

func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram(tHistogramSize, tHistogramMin, tHistogramMax)

	p100 := h.Percentile(100)
	expectedValue := 0.0

	if p100 != expectedValue {
		t.Errorf("Expected 100th percentile to be %f, got %f", expectedValue, p100)
	}
}

func TestGetPercentileAndReset(t *testing.T) {
	h := NewHistogram(tHistogramSize, tHistogramMin, tHistogramMax)

	h.Add(1.0)
	h.Add(2.0)
	h.Add(3.0)
	p50 := h.GetPercentileAndReset(50)

	if fmt.Sprintf("%.1f", p50) != "2.0" {
		t.Errorf("Expected 50th percentile to be 2.0, got %g", p50)
	}

	expectedTotal := 0
	total := h.totalCount()

	if total != expectedTotal {
		t.Errorf("Expected total count %d, got %d", expectedTotal, total)
	}
}

func TestGetPercentile(t *testing.T) {
	h := NewHistogram(tHistogramSize, tHistogramMin, tHistogramMax)

	h.Add(0.0002)
	h.Add(0.01)
	h.Add(1.007)
	h.Add(502.204)
	h.Add(100000.000)

	p50 := h.Percentile(50)
	p100 := h.Percentile(100)

	if fmt.Sprintf("%.3f", p50) != "1.025" {
		t.Errorf("Expected 50th percentile to be 1.025, got %f", p50)
	}

	if fmt.Sprintf("%.3f", p100) != "100000.000" {
		t.Errorf("Expected 100th percentile to be 100000.000, got %f", p100)
	}
}
