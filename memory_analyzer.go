package logcache

import (
	"time"

	sigar "code.cloudfoundry.org/gosigar"
)

// MemoryAnalyzer reports the available and total memory.
type MemoryAnalyzer struct {
	// metrics
	setAvail func(value float64)
	setTotal func(value float64)

	// cache
	lastResult time.Time
	avail      uint64
	total      uint64
}

// NewMemoryAnalyzer creates and returns a new MemoryAnalyzer.
func NewMemoryAnalyzer(m Metrics) *MemoryAnalyzer {
	return &MemoryAnalyzer{
		setAvail: m.NewGauge("AvailableSystemMemory"),
		setTotal: m.NewGauge("TotalSystemMemory"),
	}
}

// Memory returns the available and total system memory.
func (a *MemoryAnalyzer) Memory() (available, total uint64) {
	if time.Since(a.lastResult) > time.Minute {
		a.lastResult = time.Now()

		var m sigar.Mem
		m.Get()

		a.avail = m.ActualFree
		a.total = m.Total

		a.setAvail(float64(m.ActualFree))
		a.setTotal(float64(m.Total))
	}

	return a.avail, a.total
}
