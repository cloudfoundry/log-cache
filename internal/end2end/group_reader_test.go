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

var _ = Describe("GroupReader", func() {
	var (
		// Run is incremented for each spec. It is used to set the port
		// numbers.
		run      int
		addrs    []string
		logCache *logcache.LogCache

		node1     *logcache.GroupReader
		node2     *logcache.GroupReader
		scheduler *logcache.Scheduler

		client1  *gologcache.GroupReaderClient
		client2  *gologcache.GroupReaderClient
		lcClient *gologcache.Client
	)

	BeforeEach(func() {
		run++
		addrs = []string{
			fmt.Sprintf("127.0.0.1:%d", 9999+(run*3)),
			fmt.Sprintf("127.0.0.1:%d", 10000+(run*3)),
		}

		logCache = logcache.New(
			logcache.WithAddr(fmt.Sprintf("127.0.0.1:%d", 10001+(run*3))),
			logcache.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		logCache.Start()

		addr := logCache.Addr()

		node1 = logcache.NewGroupReader(addr, addrs, 0,
			logcache.WithGroupReaderLogger(log.New(GinkgoWriter, "", 0)),
		)
		node2 = logcache.NewGroupReader(addr, addrs, 1,
			logcache.WithGroupReaderLogger(log.New(GinkgoWriter, "", 0)),
		)

		scheduler = logcache.NewScheduler(
			addrs,
			logcache.WithSchedulerInterval(50*time.Millisecond),
		)

		node1.Start()
		node2.Start()
		scheduler.Start()

		lcClient = gologcache.NewClient(addr, gologcache.WithViaGRPC(grpc.WithInsecure()))
		client1 = gologcache.NewGroupReaderClient(addrs[0], gologcache.WithViaGRPC(grpc.WithInsecure()))
		client2 = gologcache.NewGroupReaderClient(addrs[1], gologcache.WithViaGRPC(grpc.WithInsecure()))
	})

	It("keeps track of groups", func() {
		go func(client1, client2 *gologcache.GroupReaderClient) {
			for range time.Tick(25 * time.Millisecond) {
				client1.AddToGroup(context.Background(), "some-name", "a")
				client2.AddToGroup(context.Background(), "some-name", "b")
			}
		}(client1, client2)

		Eventually(func() []string {
			m, err := client1.Group(context.Background(), "some-name")
			if err != nil {
				return nil
			}

			return m.SourceIDs
		}, 5).Should(ConsistOf("a", "b"))
	})

	It("reads from several source IDs", func() {
		go func(client1, client2 *gologcache.GroupReaderClient) {
			for {
				client1.AddToGroup(context.Background(), "some-name", "a")
				client2.AddToGroup(context.Background(), "some-name", "b")
			}
		}(client1, client2)

		_, err := ingressClient(logCache.Addr()).Send(context.Background(), &rpc.SendRequest{
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

		Eventually(func() []int {
			envelopes, err := client1.Read(
				context.Background(),
				"some-name",
				time.Unix(0, 0),
				0,
			)
			if err != nil {
				return nil
			}

			var results []int
			for _, e := range envelopes {
				results = append(results, int(e.GetTimestamp()))
			}
			return results
		}, 5).Should(ConsistOf(1, 2, 3, 4))
	})

	It("shards data via requester_id", func() {
		go func(client1, client2 *gologcache.GroupReaderClient) {
			for range time.Tick(25 * time.Millisecond) {
				client1.AddToGroup(context.Background(), "some-name", "a")
				client2.AddToGroup(context.Background(), "some-name", "b")
			}
		}(client1, client2)

		go func(addr string) {
			for i := int64(0); ; i += 5 {
				ingressClient(addr).Send(context.Background(), &rpc.SendRequest{
					Envelopes: &loggregator_v2.EnvelopeBatch{
						Batch: []*loggregator_v2.Envelope{
							{SourceId: "a", Timestamp: i + 0},
							{SourceId: "a", Timestamp: i + 1},
							{SourceId: "b", Timestamp: i + 2},
							{SourceId: "b", Timestamp: i + 3},
							{SourceId: "c", Timestamp: i + 4},
						},
					},
				})
			}
		}(logCache.Addr())

		consume := func(client *gologcache.GroupReaderClient, c chan<- []string, requesterID uint64) {
			var lastTimestamp int64
			for {
				envelopes, _ := client.Read(
					context.Background(),
					"some-name",
					time.Unix(0, lastTimestamp),
					requesterID,
				)

				m := make(map[string]bool)
				for _, e := range envelopes {
					m[e.GetSourceId()] = true
				}

				var results []string
				for k := range m {
					results = append(results, k)
				}

				if len(results) == 0 {
					continue
				}

				lastTimestamp = envelopes[len(envelopes)-1].GetTimestamp() + 1

				c <- results
			}
		}

		reader1 := make(chan []string, 10000)
		reader2 := make(chan []string, 10000)

		go consume(client2, reader1, 0)
		go consume(client1, reader2, 1)

		Eventually(reader1, 5).Should(Receive((ConsistOf("a"))))
		Eventually(reader2, 5).Should(Receive((ConsistOf("b"))))

		var later1, later2 []string
		Eventually(reader1).Should(Receive(&later1))
		Eventually(reader2).Should(Receive(&later2))
		Expect(later1).ToNot(Equal(later2))
	})
})

func ingressClient(addr string) rpc.IngressClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return rpc.NewIngressClient(conn)
}
