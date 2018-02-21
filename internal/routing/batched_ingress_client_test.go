package routing_test

import (
	"io/ioutil"
	"log"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BatchedIngressClient", func() {
	var (
		spyIngressClient *spyIngressClient
		c                *routing.BatchedIngressClient
	)

	BeforeEach(func() {
		spyIngressClient = newSpyIngressClient()
		c = routing.NewBatchedIngressClient(5, time.Hour, spyIngressClient, log.New(ioutil.Discard, "", 0))
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

		Eventually(spyIngressClient.Requests).Should(HaveLen(1))
		Expect(spyIngressClient.Requests()[0].Envelopes.Batch).To(HaveLen(5))
	})

	It("sends envelopes by batches because of interval", func() {
		c = routing.NewBatchedIngressClient(5, time.Microsecond, spyIngressClient, log.New(ioutil.Discard, "", 0))
		_, err := c.Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{Timestamp: 1},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(spyIngressClient.Requests).Should(HaveLen(1))
		Expect(spyIngressClient.Requests()[0].Envelopes.Batch).To(HaveLen(1))
	})
})
