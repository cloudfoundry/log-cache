package groups_test

import (
	"context"
	"errors"
	"log"
	"sync"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RPCReverseProxy", func() {
	var (
		r  *groups.RPCReverseProxy
		s0 *spyGroupReaderClient
		s1 *spyGroupReaderClient
		s2 *spyGroupReaderClient
		l  *spyLookup
	)

	BeforeEach(func() {
		s0 = newSpyGroupReaderClient()
		s1 = newSpyGroupReaderClient()
		s2 = newSpyGroupReaderClient()
		l = newSpyLookup()
		r = groups.NewRPCReverseProxy(
			[]logcache_v1.ShardGroupReaderClient{s0, s1, s2},
			0,
			l,
			log.New(GinkgoWriter, "", 0),
		)
	})

	It("proxies SetShardGroupRequests to the correct nodes", func() {
		l.results = []int{0, 2}
		req := &logcache_v1.SetShardGroupRequest{
			Name: "some-name-0",
		}
		s0.addErr = errors.New("some-err")
		s2.addErr = errors.New("some-err")
		_, err := r.SetShardGroup(context.Background(), req)
		Expect(err).To(MatchError("some-err, some-err"))
		Expect(s0.addToGroupRequests).To(ConsistOf(req))
		Expect(s2.addToGroupRequests).To(ConsistOf(req))

		l.results = []int{1, 2}
		req.LocalOnly = false
		_, err = r.SetShardGroup(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())
		Expect(s1.addToGroupRequests).To(ConsistOf(req))
		Expect(l.names).To(ConsistOf("some-name-0", "some-name-0"))
	})

	It("proxies local SetShardGroupRequests to the the local node", func() {
		req := &logcache_v1.SetShardGroupRequest{
			Name:      "some-name-0",
			LocalOnly: true,
		}

		l.results = []int{0, 2}

		_, err := r.SetShardGroup(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())
		Expect(s0.SetShardGroupRequests()).To(ConsistOf(req))
		Expect(s2.SetShardGroupRequests()).To(BeEmpty())
	})

	It("SetShardGroupRequests returns Unavailable when request is unroutable", func() {
		req := &logcache_v1.SetShardGroupRequest{
			Name: "some-name-0",
		}
		_, err := r.SetShardGroup(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))

		l.results = []int{1, 2}
		req.LocalOnly = true
		_, err = r.SetShardGroup(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})

	It("proxies ReadRequests to the correct node based on requester ID", func() {
		l.results = []int{0, 2}
		req := &logcache_v1.ShardGroupReadRequest{
			Name:        "some-name-0",
			RequesterId: 1,
		}
		s2.readErr = errors.New("some-err")
		_, err := r.Read(context.Background(), req)
		Expect(err).To(MatchError("some-err"))

		// This is more of a 'ping' message to other nodes so they know to
		// shard their data.
		Expect(s0.groupReadRequests).To(ConsistOf(&logcache_v1.ShardGroupReadRequest{
			Name:        "some-name-0",
			RequesterId: 1,
			Limit:       -1,
			LocalOnly:   true,
		}))
		req.LocalOnly = true
		Expect(s2.groupReadRequests).To(ConsistOf(req))

		l.results = []int{0, 1}
		req.LocalOnly = false
		_, err = r.Read(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())
		Expect(s1.groupReadRequests).To(ConsistOf(req))
		Expect(l.names).To(ConsistOf("some-name-0", "some-name-0"))

		l.results = []int{0, 2}
		s0.readErr = nil
		req.LocalOnly = true
		s0.groupReadRequests = nil
		s2.groupReadRequests = nil
		_, err = r.Read(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())
		Expect(s0.groupReadRequests).To(ConsistOf(req))
		Expect(s2.groupReadRequests).To(BeEmpty())
	})

	It("Read returns Unavailable when request is unroutable", func() {
		req := &logcache_v1.ShardGroupReadRequest{
			Name: "some-name-0",
		}
		_, err := r.Read(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))

		l.results = []int{1, 2}
		req.LocalOnly = true
		_, err = r.Read(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})

	It("proxies GroupRequests to the correct nodes", func() {
		l.results = []int{0, 1}
		req := &logcache_v1.ShardGroupRequest{
			Name: "some-name-0",
		}
		s0.groupErr = errors.New("some-err")
		s1.groupErr = errors.New("some-err")
		_, err := r.ShardGroup(context.Background(), req)
		req.LocalOnly = true
		Expect(append(s0.groupRequests, s1.groupRequests...)).To(ConsistOf(req))
		Expect(err).To(MatchError("some-err"))

		l.results = []int{0, 1}
		s0.groupErr = nil
		s1.groupErr = nil
		req.LocalOnly = false
		s0.groupRespSourceIDs = []string{"a", "b"}
		s1.groupRespSourceIDs = []string{"b", "c"}
		s0.groupRespRequesterIDs = []uint64{99, 100}
		s1.groupRespRequesterIDs = []uint64{100, 101}

		resp, err := r.ShardGroup(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())

		// Its eventually consistent, and just picks one to query
		Expect(resp.SubGroups).To(
			Or(
				ConsistOf(&logcache_v1.GroupedSourceIds{SourceIds: []string{"a", "b"}}),
				ConsistOf(&logcache_v1.GroupedSourceIds{SourceIds: []string{"b", "c"}}),
			),
		)
		Expect(resp.RequesterIds).To(
			Or(
				ConsistOf(uint64(99), uint64(100)),
				ConsistOf(uint64(100), uint64(101)),
			),
		)

		l.results = []int{0, 2}
		s0.groupErr = nil
		req.LocalOnly = true
		s0.groupRequests = nil
		s2.groupRequests = nil
		_, err = r.ShardGroup(context.Background(), req)
		Expect(err).ToNot(HaveOccurred())
		Expect(s0.groupRequests).To(ConsistOf(req))
		Expect(s2.groupRequests).To(BeEmpty())
	})

	It("ShardGroup returns Unavailable when request is unroutable", func() {
		req := &logcache_v1.ShardGroupRequest{
			Name: "some-name-0",
		}
		_, err := r.ShardGroup(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))

		l.results = []int{1, 2}
		req.LocalOnly = true
		_, err = r.ShardGroup(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})
})

type spyLookup struct {
	names   []string
	results []int
}

func newSpyLookup() *spyLookup {
	return &spyLookup{}
}

func (s *spyLookup) Lookup(name string) []int {
	s.names = append(s.names, name)
	return s.results
}

type spyGroupReaderClient struct {
	mu                 sync.Mutex
	addToGroupRequests []*logcache_v1.SetShardGroupRequest
	groupReadRequests  []*logcache_v1.ShardGroupReadRequest
	groupRequests      []*logcache_v1.ShardGroupRequest

	groupRespSourceIDs    []string
	groupRespRequesterIDs []uint64

	addErr    error
	removeErr error
	readErr   error
	groupErr  error
}

func newSpyGroupReaderClient() *spyGroupReaderClient {
	return &spyGroupReaderClient{}
}

func (s *spyGroupReaderClient) SetShardGroup(c context.Context, r *logcache_v1.SetShardGroupRequest, _ ...grpc.CallOption) (*logcache_v1.SetShardGroupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addToGroupRequests = append(s.addToGroupRequests, r)
	return &logcache_v1.SetShardGroupResponse{}, s.addErr
}

func (s *spyGroupReaderClient) setSetShardGroup(r []*logcache_v1.SetShardGroupRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addToGroupRequests = r
}

func (s *spyGroupReaderClient) SetShardGroupRequests() []*logcache_v1.SetShardGroupRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*logcache_v1.SetShardGroupRequest, len(s.addToGroupRequests))
	copy(r, s.addToGroupRequests)

	return r
}

func (s *spyGroupReaderClient) Read(c context.Context, r *logcache_v1.ShardGroupReadRequest, _ ...grpc.CallOption) (*logcache_v1.ShardGroupReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupReadRequests = append(s.groupReadRequests, r)
	return &logcache_v1.ShardGroupReadResponse{}, s.readErr
}

func (s *spyGroupReaderClient) ShardGroup(c context.Context, r *logcache_v1.ShardGroupRequest, _ ...grpc.CallOption) (*logcache_v1.ShardGroupResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupRequests = append(s.groupRequests, r)
	return &logcache_v1.ShardGroupResponse{
		SubGroups: []*logcache_v1.GroupedSourceIds{
			{SourceIds: s.groupRespSourceIDs},
		},
		RequesterIds: s.groupRespRequesterIDs,
	}, s.groupErr
}
