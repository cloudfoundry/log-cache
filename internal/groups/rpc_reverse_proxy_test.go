package groups_test

import (
	"context"
	"errors"
	"log"

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
		s1 *spyGroupReaderClient
		s2 *spyGroupReaderClient
		l  *spyLookup
	)

	BeforeEach(func() {
		s1 = newSpyGroupReaderClient()
		s2 = newSpyGroupReaderClient()
		l = newSpyLookup()
		r = groups.NewRPCReverseProxy(
			[]logcache_v1.GroupReaderClient{s1, s2},
			l,
			log.New(GinkgoWriter, "", 0),
		)
	})

	It("proxies AddToGroupRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache_v1.AddToGroupRequest{
			Name: "some-name-0",
		}
		s1.addErr = errors.New("some-err")
		_, err := r.AddToGroup(context.Background(), req)
		Expect(s1.addToGroupRequests).To(ConsistOf(req))
		Expect(err).To(MatchError("some-err"))

		l.result = 1
		_, err = r.AddToGroup(context.Background(), req)
		Expect(s2.addToGroupRequests).To(ConsistOf(req))
		Expect(err).ToNot(HaveOccurred())
		Expect(l.names).To(ConsistOf("some-name-0", "some-name-0"))
	})

	It("AddToGroupRequests returns Unavailable when request is unroutable", func() {
		l.result = -1
		req := &logcache_v1.AddToGroupRequest{
			Name: "some-name-0",
		}
		_, err := r.AddToGroup(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})

	It("proxies RemoveFromGroupRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache_v1.RemoveFromGroupRequest{
			Name: "some-name-0",
		}
		s1.removeErr = errors.New("some-err")
		_, err := r.RemoveFromGroup(context.Background(), req)
		Expect(s1.removeFromGroupRequests).To(ConsistOf(req))
		Expect(err).To(MatchError("some-err"))

		l.result = 1
		_, err = r.RemoveFromGroup(context.Background(), req)
		Expect(s2.removeFromGroupRequests).To(ConsistOf(req))
		Expect(err).ToNot(HaveOccurred())
	})

	It("RemoveFromGroupRequests returns Unavailable when request is unroutable", func() {
		l.result = -1
		req := &logcache_v1.RemoveFromGroupRequest{
			Name: "some-name-0",
		}
		_, err := r.RemoveFromGroup(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})

	It("proxies ReadRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache_v1.GroupReadRequest{
			Name: "some-name-0",
		}
		s1.readErr = errors.New("some-err")
		_, err := r.Read(context.Background(), req)
		Expect(s1.groupReadRequests).To(ConsistOf(req))
		Expect(err).To(MatchError("some-err"))

		l.result = 1
		_, err = r.Read(context.Background(), req)
		Expect(s2.groupReadRequests).To(ConsistOf(req))
		Expect(err).ToNot(HaveOccurred())
	})

	It("Read returns Unavailable when request is unroutable", func() {
		l.result = -1
		req := &logcache_v1.GroupReadRequest{
			Name: "some-name-0",
		}
		_, err := r.Read(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})

	It("proxies GroupRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache_v1.GroupRequest{
			Name: "some-name-0",
		}
		s1.groupErr = errors.New("some-err")
		_, err := r.Group(context.Background(), req)
		Expect(s1.groupRequests).To(ConsistOf(req))
		Expect(err).To(MatchError("some-err"))

		l.result = 1
		_, err = r.Group(context.Background(), req)
		Expect(s2.groupRequests).To(ConsistOf(req))
		Expect(err).ToNot(HaveOccurred())
	})

	It("Group returns Unavailable when request is unroutable", func() {
		l.result = -1
		req := &logcache_v1.GroupRequest{
			Name: "some-name-0",
		}
		_, err := r.Group(context.Background(), req)
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})
})

type spyLookup struct {
	names  []string
	result int
}

func newSpyLookup() *spyLookup {
	return &spyLookup{}
}

func (s *spyLookup) Lookup(name string) int {
	s.names = append(s.names, name)
	return s.result
}

type spyGroupReaderClient struct {
	addToGroupRequests      []*logcache_v1.AddToGroupRequest
	removeFromGroupRequests []*logcache_v1.RemoveFromGroupRequest
	groupReadRequests       []*logcache_v1.GroupReadRequest
	groupRequests           []*logcache_v1.GroupRequest

	addErr    error
	removeErr error
	readErr   error
	groupErr  error
}

func newSpyGroupReaderClient() *spyGroupReaderClient {
	return &spyGroupReaderClient{}
}

func (s *spyGroupReaderClient) AddToGroup(c context.Context, r *logcache_v1.AddToGroupRequest, _ ...grpc.CallOption) (*logcache_v1.AddToGroupResponse, error) {
	s.addToGroupRequests = append(s.addToGroupRequests, r)
	return &logcache_v1.AddToGroupResponse{}, s.addErr
}

func (s *spyGroupReaderClient) RemoveFromGroup(c context.Context, r *logcache_v1.RemoveFromGroupRequest, _ ...grpc.CallOption) (*logcache_v1.RemoveFromGroupResponse, error) {
	s.removeFromGroupRequests = append(s.removeFromGroupRequests, r)
	return &logcache_v1.RemoveFromGroupResponse{}, s.removeErr
}

func (s *spyGroupReaderClient) Read(c context.Context, r *logcache_v1.GroupReadRequest, _ ...grpc.CallOption) (*logcache_v1.GroupReadResponse, error) {
	s.groupReadRequests = append(s.groupReadRequests, r)
	return &logcache_v1.GroupReadResponse{}, s.readErr
}

func (s *spyGroupReaderClient) Group(c context.Context, r *logcache_v1.GroupRequest, _ ...grpc.CallOption) (*logcache_v1.GroupResponse, error) {
	s.groupRequests = append(s.groupRequests, r)
	return &logcache_v1.GroupResponse{}, s.groupErr
}
