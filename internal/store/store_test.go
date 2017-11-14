package store_test

import (
	"time"

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

	It("fetches data based on time and source ID", func() {
		e1 := buildEnvelope(0, "a")
		e2 := buildEnvelope(1, "b")
		e3 := buildEnvelope(2, "a")
		e4 := buildEnvelope(3, "a")

		s.Put([]*loggregator_v2.Envelope{e1, e2})
		s.Put([]*loggregator_v2.Envelope{e3, e4})

		start := time.Unix(0, 0)
		end := time.Unix(0, 3)
		envelopes := s.Get("a", start, end)
		Expect(envelopes).To(HaveLen(2))

		for _, e := range envelopes {
			Expect(e.SourceId).To(Equal("a"))
		}
	})

	It("is thread safe", func() {
		e1 := buildEnvelope(0, "a")
		go func() {
			s.Put([]*loggregator_v2.Envelope{e1})
		}()

		start := time.Unix(0, 0)
		end := time.Unix(9999, 0)

		Eventually(func() int { return len(s.Get("a", start, end)) }).Should(Equal(1))
	})
})

func buildEnvelope(timestamp int64, sourceID string) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		Timestamp: timestamp,
		SourceId:  sourceID,
	}
}
