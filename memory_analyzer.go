package logcache

import (
	"time"

	"runtime"

	sigar "github.com/cloudfoundry/gosigar"
)

// MemoryAnalyzer reports the available and total memory.
type MemoryAnalyzer struct {
	// metrics
	setAvail func(value float64)
	setTotal func(value float64)
	setHeap  func(value float64)

	// cache
	lastResult time.Time
	avail      uint64
	total      uint64
	heap       uint64
}

// NewMemoryAnalyzer creates and returns a new MemoryAnalyzer.
func NewMemoryAnalyzer(m Metrics) *MemoryAnalyzer {
	return &MemoryAnalyzer{
		setAvail: m.NewGauge("AvailableSystemMemory"),
		setHeap:  m.NewGauge("HeapInUseMemory"),
		setTotal: m.NewGauge("TotalSystemMemory"),
	}
}

// Memory returns the heap memory and total system memory.
func (a *MemoryAnalyzer) Memory() (heapInUse, total uint64) {
	if time.Since(a.lastResult) > 5*time.Second {
		a.lastResult = time.Now()

		var m sigar.Mem
		m.Get()

		a.avail = m.ActualFree
		a.total = m.Total

		a.setAvail(float64(m.ActualFree))
		a.setTotal(float64(m.Total))

		var rm runtime.MemStats
		runtime.ReadMemStats(&rm)

		a.heap = rm.HeapInuse
		a.setHeap(float64(a.heap))
	}

	return a.heap, a.total
}
