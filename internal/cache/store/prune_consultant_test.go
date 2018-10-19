package store_test

import (
	"code.cloudfoundry.org/log-cache/internal/cache/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("PruneConsultant", func() {
	var (
		sm         *spyMemory
		c          *store.PruneConsultant
		storeCount int64
	)

	BeforeEach(func() {
		sm = newSpyMemory()
		c = store.NewPruneConsultant(5, 70, sm)
		storeCount = 1000
	})

	It("does not prune any entries if memory utilization is under allotment", func() {
		sm.avail = 30
		sm.heap = 70
		sm.total = 100

		Expect(c.GetQuantityToPrune(storeCount)).To(Equal(0))
	})

	It("prunes entries if memory utilization is over allotment", func() {
		sm.avail = 100
		sm.heap = 71
		sm.total = 100

		Expect(c.GetQuantityToPrune(storeCount)).To(Equal(14))
	})
})

type spyMemory struct {
	heap, avail, total uint64
}

func newSpyMemory() *spyMemory {
	return &spyMemory{}
}

func (s *spyMemory) Memory() (uint64, uint64, uint64) {
	return s.heap, s.avail, s.total
}
