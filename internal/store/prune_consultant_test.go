package store_test

import (
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PruneConsultant", func() {
	var (
		sm *spyMemory
		c  *store.PruneConsultant
	)

	BeforeEach(func() {
		sm = newSpyMemory()
		c = store.NewPruneConsultant(5, 50, sm)
	})

	It("does not prune any entries if memory utilization is under allotment", func() {
		sm.available = 100
		sm.total = 100

		Expect(c.Prune()).To(BeZero())
	})

	It("prunes entries if memory utilization is over allotment", func() {
		sm.available = 49
		sm.total = 100

		Expect(c.Prune()).To(Equal(5))
	})
})

type spyMemory struct {
	available, total uint64
}

func newSpyMemory() *spyMemory {
	return &spyMemory{}
}

func (s *spyMemory) Memory() (uint64, uint64) {
	return s.available, s.total
}
