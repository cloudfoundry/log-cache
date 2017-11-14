package store_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Store", func() {
	var (
		s *store.Store
	)

	BeforeEach(func() {
		s = store.NewStore(5)
	})

	It("stores given data", func() {
		e1 := buildEnvelope("a")
		e2 := buildEnvelope("b")
		e3 := buildEnvelope("a")

		s.Put([]*loggregator_v2.Envelope{e1, e2})
		s.Put([]*loggregator_v2.Envelope{e3})

		envelopes := s.Get("a")
		Expect(envelopes).To(HaveLen(2))

		for _, e := range envelopes {
			Expect(e.SourceId).To(Equal("a"))
		}
	})

	It("is thread safe", func() {
		e1 := buildEnvelope("a")
		go func() {
			s.Put([]*loggregator_v2.Envelope{e1})
		}()

		Eventually(func() int { return len(s.Get("a")) }).Should(Equal(1))
	})
})

func buildEnvelope(sourceID string) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		SourceId: sourceID,
	}
}
