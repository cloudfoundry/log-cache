package groups_test

import (
	"context"
	"errors"
	"log"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/log-cache/internal/groups"
	"google.golang.org/grpc"

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
			[]logcache.GroupReaderClient{s1, s2},
			l,
			log.New(GinkgoWriter, "", 0),
		)
	})

	It("proxies AddToGroupRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache.AddToGroupRequest{
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
	})

	It("proxies RemoveFromGroupRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache.RemoveFromGroupRequest{
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

	It("proxies ReadRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache.GroupReadRequest{
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

	It("proxies GroupRequests to the correct nodes", func() {
		l.result = 0
		req := &logcache.GroupRequest{
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
	addToGroupRequests      []*logcache.AddToGroupRequest
	removeFromGroupRequests []*logcache.RemoveFromGroupRequest
	groupReadRequests       []*logcache.GroupReadRequest
	groupRequests           []*logcache.GroupRequest

	addErr    error
	removeErr error
	readErr   error
	groupErr  error
}

func newSpyGroupReaderClient() *spyGroupReaderClient {
	return &spyGroupReaderClient{}
}

func (s *spyGroupReaderClient) AddToGroup(c context.Context, r *logcache.AddToGroupRequest, _ ...grpc.CallOption) (*logcache.AddToGroupResponse, error) {
	s.addToGroupRequests = append(s.addToGroupRequests, r)
	return &logcache.AddToGroupResponse{}, s.addErr
}

func (s *spyGroupReaderClient) RemoveFromGroup(c context.Context, r *logcache.RemoveFromGroupRequest, _ ...grpc.CallOption) (*logcache.RemoveFromGroupResponse, error) {
	s.removeFromGroupRequests = append(s.removeFromGroupRequests, r)
	return &logcache.RemoveFromGroupResponse{}, s.removeErr
}

func (s *spyGroupReaderClient) Read(c context.Context, r *logcache.GroupReadRequest, _ ...grpc.CallOption) (*logcache.GroupReadResponse, error) {
	s.groupReadRequests = append(s.groupReadRequests, r)
	return &logcache.GroupReadResponse{}, s.readErr
}

func (s *spyGroupReaderClient) Group(c context.Context, r *logcache.GroupRequest, _ ...grpc.CallOption) (*logcache.GroupResponse, error) {
	s.groupRequests = append(s.groupRequests, r)
	return &logcache.GroupResponse{}, s.groupErr
}
