package routing_test

import (
	"io/ioutil"
	"log"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	rpc "code.cloudfoundry.org/log-cache/rpc/logcache_v1"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BatchedIngressClient", func() {
	var (
		spyMetrics    *spyMetrics
		ingressClient *spyIngressClient
		c             *routing.BatchedIngressClient
	)

	BeforeEach(func() {
		spyMetrics = newSpyMetrics()
		ingressClient = newSpyIngressClient()
		c = routing.NewBatchedIngressClient(5, time.Hour, ingressClient, spyMetrics, log.New(ioutil.Discard, "", 0))
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
		c = routing.NewBatchedIngressClient(5, time.Microsecond, ingressClient, spyMetrics, log.New(ioutil.Discard, "", 0))
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

		Eventually(spyMetrics.GetDelta("Dropped")).ShouldNot(BeZero())
	})
})

type spyMetrics struct {
	mu      sync.Mutex
	metrics map[string]uint64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		metrics: make(map[string]uint64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(uint64) {
	return func(delta uint64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.metrics[name] += delta
	}
}

func (s *spyMetrics) GetDelta(name string) func() uint64 {
	return func() uint64 {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.metrics[name]
	}
}
