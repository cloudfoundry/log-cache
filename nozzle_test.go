package logcache_test

import (
	"sync"

	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Nozzle", func() {
	var (
		n               *logcache.Nozzle
		streamConnector *spyStreamConnector
		logCache        *spyLogCache
	)

	BeforeEach(func() {
		streamConnector = newSpyStreamConnector()
		logCache = newSpyLogCache()
		addr := logCache.start()

		n = logcache.NewNozzle(streamConnector, addr)
		go n.Start()
	})

	It("connects and reads from a logs provider server", func() {
		addEnvelope(1, "some-source-id", streamConnector)
		addEnvelope(2, "some-source-id", streamConnector)
		addEnvelope(3, "some-source-id", streamConnector)

		Eventually(streamConnector.requests).Should(HaveLen(1))
		Expect(streamConnector.requests()[0].ShardId).To(Equal("log-cache"))
		Eventually(streamConnector.envelopes).Should(HaveLen(0))
	})

	It("writes each envelope to the LogCache", func() {
		addEnvelope(1, "some-source-id", streamConnector)
		addEnvelope(2, "some-source-id", streamConnector)
		addEnvelope(3, "some-source-id", streamConnector)

		Eventually(logCache.getEnvelopes).Should(HaveLen(3))
		Expect(logCache.getEnvelopes()[0].Timestamp).To(Equal(int64(1)))
		Expect(logCache.getEnvelopes()[1].Timestamp).To(Equal(int64(2)))
		Expect(logCache.getEnvelopes()[2].Timestamp).To(Equal(int64(3)))
	})
})

func addEnvelope(timestamp int64, sourceID string, c *spyStreamConnector) {
	c.envelopes <- []*loggregator_v2.Envelope{
		{
			Timestamp: timestamp,
			SourceId:  sourceID,
		},
	}
}

type spyStreamConnector struct {
	mu        sync.Mutex
	requests_ []*loggregator_v2.EgressBatchRequest
	envelopes chan []*loggregator_v2.Envelope
}

func newSpyStreamConnector() *spyStreamConnector {
	return &spyStreamConnector{
		envelopes: make(chan []*loggregator_v2.Envelope, 100),
	}
}

func (s *spyStreamConnector) Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests_ = append(s.requests_, req)

	return func() []*loggregator_v2.Envelope {
		select {
		case e := <-s.envelopes:
			return e
		default:
			return nil
		}
	}
}

func (s *spyStreamConnector) requests() []*loggregator_v2.EgressBatchRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	reqs := make([]*loggregator_v2.EgressBatchRequest, len(s.requests_))
	copy(reqs, s.requests_)

	return reqs
}
