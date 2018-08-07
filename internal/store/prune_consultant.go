package store

// PruneConsultant keeps track of the available memory on the system and tries
// to utilize as much memory as possible while not being a bad neighbor.
type PruneConsultant struct {
	m Memory

	percentToFill float64
	stepBy        int
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

// Prune reports how many entries should be removed.
func (pc *PruneConsultant) GetQuantityToPrune(storeCount int64) int {
	heap, avail, total := pc.m.Memory()

	heapPercentage := float64(heap*100) / float64(total)
	if heapPercentage > pc.percentToFill {
		percentageToPrune := (heapPercentage - pc.percentToFill) / heapPercentage
		return int(float64(storeCount) * percentageToPrune)
	}

	availablePercentage := float64(avail*100) / float64(total)
	if availablePercentage < 20 {
		percentageToPrune := (20 - availablePercentage) / heapPercentage
		return int(float64(storeCount) * percentageToPrune)
	}

	return 0
}
