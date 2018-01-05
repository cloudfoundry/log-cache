package logcache_test

import (
	"net"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GroupReader", func() {
	var (
		g *logcache.GroupReader
		c rpc.GroupReaderClient

		spy         *spyGroupReader
		spyLogCache *spyLogCache
	)

	BeforeEach(func() {
		spy = newSpyGroupReader()
		spyAddr := spy.start()

		spyLogCache = newSpyLogCache()
		logCacheAddr := spyLogCache.start()

		g = logcache.NewGroupReader(logCacheAddr, []string{"127.0.0.1:0", spyAddr}, 0)
		g.Start()

		c = newGroupReaderClient(g.Addr())
	})

	It("reads data from a group of sourceIDs", func() {
		spyLogCache.readEnvelopes["source-0"] = []*loggregator_v2.Envelope{
			{Timestamp: 98},
			{Timestamp: 99},
			{Timestamp: 101},
		}

		spyLogCache.readEnvelopes["source-1"] = []*loggregator_v2.Envelope{
			{Timestamp: 100},
			{Timestamp: 102},
			{Timestamp: 103},
		}

		_, err := c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-a",
			SourceId: "source-0",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-a",
			SourceId: "source-1",
		})
		Expect(err).ToNot(HaveOccurred())

		Eventually(func() []int64 {
			resp, err := c.Read(context.Background(), &rpc.GroupReadRequest{
				Name: "some-name-a",

				// [99,103)
				StartTime: 99,
				EndTime:   103,
			})
			Expect(err).ToNot(HaveOccurred())

			var result []int64
			for _, e := range resp.Envelopes.Batch {
				result = append(result, e.GetTimestamp())
			}

			return result
		}).Should(Equal([]int64{99, 100, 101, 102}))
	})

	It("keeps track of groups", func() {
		_, err := c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-a",
			SourceId: "some-id",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-b",
			SourceId: "some-other-id",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-a",
			SourceId: "some-other-id",
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := c.Group(context.Background(), &rpc.GroupRequest{
			Name: "some-name-a",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.SourceIds).To(ConsistOf("some-id", "some-other-id"))

		resp, err = c.Group(context.Background(), &rpc.GroupRequest{
			Name: "some-name-b",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.SourceIds).To(ConsistOf("some-other-id"))

		_, err = c.RemoveFromGroup(context.Background(), &rpc.RemoveFromGroupRequest{
			Name:     "some-name-b",
			SourceId: "some-other-id",
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err = c.Group(context.Background(), &rpc.GroupRequest{
			Name: "some-name-b",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.SourceIds).To(BeEmpty())
	})

	It("routes requests to the correct node", func() {
		// some-name-a hashes to 4464231820929349922 (node 0)
		_, err := c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-a",
			SourceId: "some-id",
		})

		Expect(err).ToNot(HaveOccurred())
		Consistently(spy.AddRequests).Should(BeEmpty())

		// some-name-c hashes to 14515125134919833977 (node 1)
		_, err = c.AddToGroup(context.Background(), &rpc.AddToGroupRequest{
			Name:     "some-name-c",
			SourceId: "some-id",
		})

		Expect(err).ToNot(HaveOccurred())
		Eventually(spy.AddRequests).Should(HaveLen(1))
		Expect(spy.AddRequests()[0].Name).To(Equal("some-name-c"))
		Expect(spy.AddRequests()[0].SourceId).To(Equal("some-id"))

		resp, err := c.Read(context.Background(), &rpc.GroupReadRequest{
			Name: "some-name-c",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Envelopes.Batch).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 1},
			&loggregator_v2.Envelope{Timestamp: 2},
		))
	})
})

func newGroupReaderClient(addr string) rpc.GroupReaderClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	return rpc.NewGroupReaderClient(conn)
}

type spyGroupReader struct {
	mu       sync.Mutex
	addReqs  []*rpc.AddToGroupRequest
	readReqs []*rpc.GroupReadRequest
}

func newSpyGroupReader() *spyGroupReader {
	return &spyGroupReader{}
}

func (s *spyGroupReader) start() string {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	go func() {
		srv := grpc.NewServer()

		rpc.RegisterGroupReaderServer(srv, s)
		if err := srv.Serve(lis); err != nil {
			panic(err)
		}
	}()

	return lis.Addr().String()
}

func (s *spyGroupReader) AddToGroup(c context.Context, r *rpc.AddToGroupRequest) (*rpc.AddToGroupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addReqs = append(s.addReqs, r)
	return &rpc.AddToGroupResponse{}, nil
}

func (s *spyGroupReader) AddRequests() []*rpc.AddToGroupRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.AddToGroupRequest, len(s.addReqs))
	copy(r, s.addReqs)
	return r
}

func (s *spyGroupReader) Read(c context.Context, r *rpc.GroupReadRequest) (*rpc.GroupReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readReqs = append(s.readReqs, r)

	return &rpc.GroupReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: []*loggregator_v2.Envelope{
				{Timestamp: 1},
				{Timestamp: 2},
			},
		},
	}, nil
}

func (s *spyGroupReader) getReadRequests() []*rpc.GroupReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.GroupReadRequest, len(s.readReqs))
	copy(r, s.readReqs)

	return r
}

func (s *spyGroupReader) RemoveFromGroup(context.Context, *rpc.RemoveFromGroupRequest) (*rpc.RemoveFromGroupResponse, error) {
	panic("not implemented")
}

func (s *spyGroupReader) Group(context.Context, *rpc.GroupRequest) (*rpc.GroupResponse, error) {
	panic("not implemented")
}
