package end2end_test

import (
	"context"
	"fmt"
	"log"
	"time"

	gologcache "code.cloudfoundry.org/go-log-cache"
	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	logcache "code.cloudfoundry.org/log-cache"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("LogCache", func() {
	var (
		addrs []string
		node1 *logcache.LogCache
		node2 *logcache.LogCache

		client *gologcache.Client
	)

	BeforeEach(func() {
		run++
		addrs = []string{
			fmt.Sprintf("127.0.0.1:%d", 9999+(run*runIncBy)),
			fmt.Sprintf("127.0.0.1:%d", 10000+(run*runIncBy)),
		}

		node1 = logcache.New(
			logcache.WithAddr(addrs[0]),
			logcache.WithClustered(0, addrs, grpc.WithInsecure()),
			logcache.WithLogger(log.New(GinkgoWriter, "", 0)),
		)

		node2 = logcache.New(
			logcache.WithAddr(addrs[1]),
			logcache.WithClustered(1, addrs, grpc.WithInsecure()),
			logcache.WithLogger(log.New(GinkgoWriter, "", 0)),
		)

		node1.Start()
		node2.Start()

		client = gologcache.NewClient(addrs[0], gologcache.WithViaGRPC(grpc.WithInsecure()))
	})

	It("reads data from Log Cache", func() {
		_, err := ingressClient(node1.Addr()).Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
					{SourceId: "a", Timestamp: 2},
					{SourceId: "b", Timestamp: 3},
					{SourceId: "b", Timestamp: 4},
					{SourceId: "c", Timestamp: 5},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = ingressClient(node2.Addr()).Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 6},
					{SourceId: "a", Timestamp: 7},
					{SourceId: "b", Timestamp: 8},
					{SourceId: "b", Timestamp: 9},
					{SourceId: "c", Timestamp: 10},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() []int64 {
			es, err := client.Read(context.Background(), "a", time.Unix(0, 0))
			Expect(err).ToNot(HaveOccurred())

			var result []int64
			for _, e := range es {
				result = append(result, e.GetTimestamp())
			}
			return result

		}, 5).Should(Equal([]int64{1, 2, 6, 7}))
	})
})
