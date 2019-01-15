package routing_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"sync"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IngressReverseProxy", func() {
	var (
		spyLookup              *spyLookup
		spyIngressRemoteClient *spyIngressClient
		spyIngressLocalClient  *spyIngressClient
		p                      *routing.IngressReverseProxy
	)

	BeforeEach(func() {
		spyLookup = newSpyLookup()
		spyIngressRemoteClient = newSpyIngressClient()
		spyIngressLocalClient = newSpyIngressClient()
		p = routing.NewIngressReverseProxy(spyLookup.Lookup, []rpc.IngressClient{
			spyIngressRemoteClient,
			spyIngressLocalClient,
		},
			1, // Point local at spyIngressLocalClient
			log.New(ioutil.Discard, "", 0))
	})

	It("uses the correct clients", func() {
		spyLookup.results["a"] = []int{0}
		spyLookup.results["b"] = []int{1}
		spyLookup.results["c"] = []int{0, 1}

		_, err := p.Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
					{SourceId: "b", Timestamp: 2},
					{SourceId: "c", Timestamp: 3},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyLookup.sourceIDs).To(ConsistOf("a", "b", "c"))

		Expect(spyIngressRemoteClient.reqs).To(ConsistOf(
			&rpc.SendRequest{
				LocalOnly: true,
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{SourceId: "a", Timestamp: 1},
						{SourceId: "c", Timestamp: 3},
					},
				},
			},
		))

		Expect(spyIngressLocalClient.reqs).To(ConsistOf(
			&rpc.SendRequest{
				LocalOnly: true,
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{SourceId: "b", Timestamp: 2},
						{SourceId: "c", Timestamp: 3},
					},
				},
			},
		))
	})

	It("routes local_only requests only to local client", func() {
		spyLookup.results["a"] = []int{0, 1}
		_, err := p.Send(context.Background(), &rpc.SendRequest{
			LocalOnly: true,
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyIngressLocalClient.reqs).To(ConsistOf(
			&rpc.SendRequest{
				LocalOnly: true,
				Envelopes: &loggregator_v2.EnvelopeBatch{
					Batch: []*loggregator_v2.Envelope{
						{SourceId: "a", Timestamp: 1},
					},
				},
			},
		))
		Expect(spyIngressRemoteClient.reqs).To(BeEmpty())
	})

	It("survives an unroutable request", func() {
		spyLookup.results["b"] = []int{1}

		_, err := p.Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
					{SourceId: "b", Timestamp: 2},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyIngressLocalClient.reqs).To(ConsistOf(&rpc.SendRequest{
			LocalOnly: true,
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "b", Timestamp: 2},
				},
			},
		}))
	})

	It("uses the given context", func() {
		spyLookup.results["a"] = []int{0}
		spyLookup.results["b"] = []int{1}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := p.Send(ctx, &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
					{SourceId: "b", Timestamp: 2},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyIngressRemoteClient.ctxs).ToNot(BeEmpty())
		Expect(spyIngressRemoteClient.ctxs[0].Done()).To(BeClosed())
	})

	It("does not return an error if one of the clients returns an error", func() {
		spyIngressRemoteClient.err = errors.New("some-error")

		spyLookup.results["a"] = []int{0}
		spyLookup.results["b"] = []int{1}

		_, err := p.Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
					{SourceId: "b", Timestamp: 2},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())
	})
})

type spyLookup struct {
	sourceIDs []string
	results   map[string][]int
}

func newSpyLookup() *spyLookup {
	return &spyLookup{
		results: make(map[string][]int),
	}
}

func (s *spyLookup) Lookup(sourceID string) []int {
	s.sourceIDs = append(s.sourceIDs, sourceID)
	return s.results[sourceID]
}

type spyIngressClient struct {
	mu   sync.Mutex
	ctxs []context.Context
	reqs []*rpc.SendRequest
	err  error
}

func newSpyIngressClient() *spyIngressClient {
	return &spyIngressClient{}
}

func (s *spyIngressClient) Send(ctx context.Context, in *rpc.SendRequest, opts ...grpc.CallOption) (*rpc.SendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ctxs = append(s.ctxs, ctx)
	s.reqs = append(s.reqs, in)
	return &rpc.SendResponse{}, s.err
}

func (s *spyIngressClient) Requests() []*rpc.SendRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.SendRequest, len(s.reqs))
	copy(r, s.reqs)
	return r
}
