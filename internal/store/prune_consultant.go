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
	// Memory returns available and total system memory.
	Memory() (available, total uint64)
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
func (a *PruneConsultant) Prune() int {
	avail, total := a.m.Memory()
	if 100-float64(avail*100)/float64(total) > a.percentToFill {
		return -a.stepBy
	}

	return 0
}
