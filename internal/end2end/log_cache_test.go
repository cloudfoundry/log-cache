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
		addrs     []string
		node1     *logcache.LogCache
		node2     *logcache.LogCache
		scheduler *logcache.Scheduler

		client *gologcache.Client

		// Run is incremented for each spec. It is used to set the port
		// numbers.
		run      int
		runIncBy = 3
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

		scheduler = logcache.NewScheduler(
			addrs, // Log Cache addrs
			logcache.WithSchedulerInterval(50*time.Millisecond),
		)

		node1.Start()
		node2.Start()
		scheduler.Start()

		client = gologcache.NewClient(addrs[0], gologcache.WithViaGRPC(grpc.WithInsecure()))
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
			es, err := client.Read(context.Background(), "a", time.Unix(0, 0), gologcache.WithLimit(500))
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
