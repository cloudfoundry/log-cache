package nozzle_test

import (
	"code.cloudfoundry.org/go-loggregator/metrics/testhelpers"
	"log"
	"sync"

	"code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	. "code.cloudfoundry.org/log-cache/internal/nozzle"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/log-cache/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Nozzle", func() {
	var (
		n               *Nozzle
		streamConnector *spyStreamConnector
		logCache        *testing.SpyLogCache
		spyMetrics      *testhelpers.SpyMetricsRegistry
		logger 			*log.Logger
	)

	Context("With custom envelope selectors", func() {
		BeforeEach(func() {
			tlsConfig, err := testing.NewTLSConfig(
				testing.Cert("log-cache-ca.crt"),
				testing.Cert("log-cache.crt"),
				testing.Cert("log-cache.key"),
				"log-cache",
			)
			Expect(err).ToNot(HaveOccurred())
			streamConnector = newSpyStreamConnector()
			spyMetrics = testhelpers.NewMetricsRegistry()
			logCache = testing.NewSpyLogCache(tlsConfig)
			logger = log.New(GinkgoWriter, "", log.LstdFlags)
			addr := logCache.Start()

			n = NewNozzle(streamConnector, addr, "log-cache", spyMetrics, logger,
				WithDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
				WithSelectors("gauge", "timer", "event"),
			)
			go n.Start()
		})

		It("only asks for the requested selectors", func() {
			Eventually(streamConnector.requests).Should(HaveLen(1))
			Expect(streamConnector.requests()[0].Selectors).To(ConsistOf(
				[]*loggregator_v2.Selector{
					{
						Message: &loggregator_v2.Selector_Gauge{
							Gauge: &loggregator_v2.GaugeSelector{},
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
	})

	Context("With default envelope selectors", func() {
		BeforeEach(func() {
			tlsConfig, err := testing.NewTLSConfig(
				testing.Cert("log-cache-ca.crt"),
				testing.Cert("log-cache.crt"),
				testing.Cert("log-cache.key"),
				"log-cache",
			)
			Expect(err).ToNot(HaveOccurred())
			streamConnector = newSpyStreamConnector()
			spyMetrics = testhelpers.NewMetricsRegistry()
			logCache = testing.NewSpyLogCache(tlsConfig)
			logger = log.New(GinkgoWriter, "", log.LstdFlags)
			addr := logCache.Start()

			n = NewNozzle(streamConnector, addr, "log-cache", spyMetrics, logger,
				WithDialOpts(grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))),
				WithSelectors("log", "gauge", "counter", "timer", "event"),
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

			Eventually(logCache.GetEnvelopes).Should(HaveLen(3))
			Expect(logCache.GetEnvelopes()[0].Timestamp).To(Equal(int64(1)))
			Expect(logCache.GetEnvelopes()[1].Timestamp).To(Equal(int64(2)))
			Expect(logCache.GetEnvelopes()[2].Timestamp).To(Equal(int64(3)))
		})

		It("writes Ingress, Egress and Err metrics", func() {
			addEnvelope(1, "some-source-id", streamConnector)
			addEnvelope(2, "some-source-id", streamConnector)
			addEnvelope(3, "some-source-id", streamConnector)

			Eventually(logCache.GetEnvelopes).Should(HaveLen(3))
			Eventually(func() float64 {
				return spyMetrics.GetMetricValue("nozzle_ingress", nil)
			}).Should(Equal(3.0))
			Eventually(func() float64 {
				return spyMetrics.GetMetricValue("nozzle_egress", nil)
			}).Should(Equal(3.0))
			Eventually(func() float64 {
				return spyMetrics.GetMetricValue("nozzle_err", nil)
			}).Should(BeZero())
		})
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
