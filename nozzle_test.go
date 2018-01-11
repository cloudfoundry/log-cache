package logcache_test

import (
	"sync"

	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Nozzle", func() {
	var (
		n               *logcache.Nozzle
		streamConnector *spyStreamConnector
		logCache        *spyLogCache
		metricMap       *spyMetrics
	)

	BeforeEach(func() {
		tlsConfig, err := newTLSConfig(
			Cert("log-cache-ca.crt"),
			Cert("log-cache.crt"),
			Cert("log-cache.key"),
			"log-cache",
		)
		Expect(err).ToNot(HaveOccurred())
		streamConnector = newSpyStreamConnector()
		metricMap = newSpyMetrics()
		logCache = newSpyLogCache(tlsConfig)
		addr := logCache.start()

		n = logcache.NewNozzle(streamConnector, addr,
			logcache.WithNozzleMetrics(metricMap),
			logcache.WithNozzleDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
		)
		go n.Start()
	})

	It("connects and reads from a logs provider server", func() {
		addEnvelope(1, "some-source-id", streamConnector)
		addEnvelope(2, "some-source-id", streamConnector)
		addEnvelope(3, "some-source-id", streamConnector)

		Eventually(streamConnector.requests).Should(HaveLen(1))
		Expect(streamConnector.requests()[0].ShardId).To(Equal("log-cache"))
		Expect(streamConnector.requests()[0].UsePreferredTags).To(BeTrue())
		Expect(streamConnector.requests()[0].Selectors).To(HaveLen(5))

		Expect(streamConnector.requests()[0].Selectors).To(ConsistOf(
			[]*loggregator_v2.Selector{
				{
					Message: &loggregator_v2.Selector_Log{
						Log: &loggregator_v2.LogSelector{},
					},
				},
				{
					Message: &loggregator_v2.Selector_Gauge{
						Gauge: &loggregator_v2.GaugeSelector{},
					},
				},
				{
					Message: &loggregator_v2.Selector_Counter{
						Counter: &loggregator_v2.CounterSelector{},
					},
				},
				{
					Message: &loggregator_v2.Selector_Timer{
						Timer: &loggregator_v2.TimerSelector{},
					},
				},
				{
					Message: &loggregator_v2.Selector_Event{
						Event: &loggregator_v2.EventSelector{},
					},
				},
			},
		))

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

	It("writes Ingress, Egress and Err metrics", func() {
		addEnvelope(1, "some-source-id", streamConnector)
		addEnvelope(2, "some-source-id", streamConnector)
		addEnvelope(3, "some-source-id", streamConnector)

		Eventually(metricMap.getter("Ingress")).Should(Equal(uint64(3)))
		Eventually(metricMap.getter("Ingress")).Should(Equal(uint64(3)))
		Eventually(metricMap.getter("Err")).Should(Equal(uint64(0)))
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

type spyMetrics struct {
	mu sync.Mutex
	m  map[string]uint64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		m: make(map[string]uint64),
	}
}

func (s *spyMetrics) NewCounter(key string) func(uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = 0

	return func(i uint64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.m[key] += i
	}
}

func (s *spyMetrics) NewGauge(key string) func(float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = 0

	return func(i float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.m[key] = uint64(i)
	}
}

func (s *spyMetrics) getter(key string) func() uint64 {
	return func() uint64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		value, ok := s.m[key]
		if !ok {
			return 99999999999
		}
		return value
	}
}
