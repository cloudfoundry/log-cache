package end2end_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache"
	"code.cloudfoundry.org/log-cache/pkg/client"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogCache", func() {
	var (
		lc_addrs     []string
		node1        *cache.LogCache
		node2        *cache.LogCache
		lc_scheduler *scheduler.Scheduler
		lc_client    *client.Client

		// run is used to set varying port numbers
		// it is incremented for each spec
		run      int
		runIncBy = 3
	)

	BeforeEach(func() {
		run++
		lc_addrs = []string{
			fmt.Sprintf("127.0.0.1:%d", 9999+(run*runIncBy)),
			fmt.Sprintf("127.0.0.1:%d", 10000+(run*runIncBy)),
		}

		node1 = cache.New(
			cache.WithAddr(lc_addrs[0]),
			cache.WithClustered(0, lc_addrs, grpc.WithInsecure()),
			cache.WithLogger(log.New(GinkgoWriter, "", 0)),
		)

		node2 = cache.New(
			cache.WithAddr(lc_addrs[1]),
			cache.WithClustered(1, lc_addrs, grpc.WithInsecure()),
			cache.WithLogger(log.New(GinkgoWriter, "", 0)),
		)

		node1.Start()
		node2.Start()

		lc_client = client.NewClient(lc_addrs[0], client.WithViaGRPC(grpc.WithInsecure()))
	})

	AfterEach(func() {
		node1.Close()
		node2.Close()
	})

	It("reads data from Log Cache", func() {
		Eventually(func() []int64 {
			ic1, cleanup1 := ingressClient(node1.Addr())
			defer cleanup1()

			ic2, cleanup2 := ingressClient(node2.Addr())
			defer cleanup2()

			_, err := ic1.Send(context.Background(), &rpc.SendRequest{
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

			_, err = ic2.Send(context.Background(), &rpc.SendRequest{
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{SourceId: "a", Timestamp: 1000000006},
						{SourceId: "a", Timestamp: 1000000007},
						{SourceId: "b", Timestamp: 1000000008},
						{SourceId: "b", Timestamp: 1000000009},
						{SourceId: "c", Timestamp: 1000000010},
					},
				},
			})

			Expect(err).ToNot(HaveOccurred())
			es, err := lc_client.Read(context.Background(), "a", time.Unix(0, 0), client.WithLimit(500))
			if err != nil {
				return nil
			}

			var result []int64
			for _, e := range es {
				result = append(result, e.GetTimestamp())
			}
			return result

		}, 5).Should(And(
			ContainElement(int64(1)),
			ContainElement(int64(2)),
			ContainElement(int64(1000000006)),
			ContainElement(int64(1000000007)),
		))
	})
})

func ingressClient(addr string) (client rpc.IngressClient, cleanup func()) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return rpc.NewIngressClient(conn), func() {
		conn.Close()
	}
}
