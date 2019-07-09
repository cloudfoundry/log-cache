package routing_test

import (
	"code.cloudfoundry.org/go-loggregator/metrics"
	"code.cloudfoundry.org/go-loggregator/metrics/testhelpers"
	"io/ioutil"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BatchedIngressClient", func() {
	var (
		m             *testhelpers.SpyMetricsRegistry
		spyDropped    metrics.Counter
		ingressClient *spyIngressClient
		c             *routing.BatchedIngressClient
	)

	BeforeEach(func() {
		m = testhelpers.NewMetricsRegistry()
		spyDropped = m.NewCounter("nodeX_dropped")
		ingressClient = newSpyIngressClient()
		c = routing.NewBatchedIngressClient(5, time.Hour, ingressClient, spyDropped, log.New(ioutil.Discard, "", 0))
	})

	It("sends envelopes with LocalOnly set to true", func() {
		for i := 0; i < 5; i++ {
			_, err := c.Send(context.Background(), &rpc.SendRequest{
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{Timestamp: int64(i)},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())
		}

		Eventually(ingressClient.Requests).Should(HaveLen(1))
		Expect(ingressClient.Requests()[0].Envelopes.Batch).To(HaveLen(5))
		Expect(ingressClient.Requests()[0].LocalOnly).To(BeTrue())

		for i, e := range ingressClient.Requests()[0].Envelopes.Batch {
			Expect(e).To(Equal(
				&loggregator_v2.Envelope{
					Timestamp: int64(i),
				},
			))
		}
	})

	It("sends envelopes by batches because of size", func() {
		for i := 0; i < 5; i++ {
			_, err := c.Send(context.Background(), &rpc.SendRequest{
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{Timestamp: int64(i)},
					},
				},
			})
			Expect(err).ToNot(HaveOccurred())
		}

		Eventually(ingressClient.Requests).Should(HaveLen(1))
		Expect(ingressClient.Requests()[0].Envelopes.Batch).To(HaveLen(5))
	})

	It("sends envelopes by batches because of interval", func() {
		c = routing.NewBatchedIngressClient(5, time.Microsecond, ingressClient, spyDropped, log.New(ioutil.Discard, "", 0))
		_, err := c.Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{Timestamp: 1},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(ingressClient.Requests).Should(HaveLen(1))
		Expect(ingressClient.Requests()[0].Envelopes.Batch).To(HaveLen(1))
	})

	It("increments a dropped counter", func() {
		go func(ingressClient *spyIngressClient) {
			for {
				// Force ingress client to block 100ms
				ingressClient.mu.Lock()
				time.Sleep(100 * time.Millisecond)
				ingressClient.mu.Unlock()
			}
		}(ingressClient)

		for i := 0; i < 25000; i++ {
			c.Send(context.Background(), &rpc.SendRequest{
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{Timestamp: 1},
					},
				},
			})
		}

		Eventually(func() float64 {
			return m.GetMetricValue("nodeX_dropped", nil)
		}).ShouldNot(BeZero())
	})
})
