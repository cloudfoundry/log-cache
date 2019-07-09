package store

import "code.cloudfoundry.org/go-loggregator/metrics"

// PruneConsultant keeps track of the available memory on the system and tries
// to utilize as much memory as possible while not being a bad neighbor.
type PruneConsultant struct {
	m Memory

	percentToFill float64
	stepBy        int
	reportMemory  metrics.Gauge
}

// Memory is used to give information about system memory.
type Memory interface {
	// Memory returns in-use heap memory and total system memory.
	Memory() (heap, avail, total uint64)
}

// NewPruneConsultant returns a new PruneConsultant.
func NewPruneConsultant(stepBy int, percentToFill float64, m Memory) *PruneConsultant {
	return &PruneConsultant{
		m:             m,
		percentToFill: percentToFill,
		stepBy:        stepBy,
	}
}

func (pc *PruneConsultant) SetMemoryReporter(mr metrics.Gauge) {
	pc.reportMemory = mr
}

// Prune reports how many entries should be removed.
func (pc *PruneConsultant) GetQuantityToPrune(storeCount int64) int {
	heap, _, total := pc.m.Memory()

	heapPercentage := float64(heap*100) / float64(total)

	if pc.reportMemory != nil {
		pc.reportMemory.Set(heapPercentage)
	}

	if heapPercentage > pc.percentToFill {
		percentageToPrune := (heapPercentage - pc.percentToFill) / heapPercentage
		return int(float64(storeCount) * percentageToPrune)
	}

	return 0
}
