package routing_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("IngressReverseProxy", func() {
	var (
		spyLookup         *spyLookup
		spyIngressClient1 *spyIngressClient
		spyIngressClient2 *spyIngressClient
		p                 *routing.IngressReverseProxy
	)

	BeforeEach(func() {
		spyLookup = newSpyLookup()
		spyIngressClient1 = newSpyIngressClient()
		spyIngressClient2 = newSpyIngressClient()
		p = routing.NewIngressReverseProxy(spyLookup.Lookup, []rpc.IngressClient{
			spyIngressClient1,
			spyIngressClient2,
		}, log.New(ioutil.Discard, "", 0))
	})

	It("uses the correct client", func() {
		spyLookup.results["a"] = 0
		spyLookup.results["b"] = 1

		_, err := p.Send(context.Background(), &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
					{SourceId: "b", Timestamp: 2},
				},
			},
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyLookup.sourceIDs).To(ConsistOf("a", "b"))

		Expect(spyIngressClient1.reqs).To(ConsistOf(&rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
				},
			},
		}))

		Expect(spyIngressClient2.reqs).To(ConsistOf(&rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "b", Timestamp: 2},
				},
			},
		}))
	})

	It("uses the given context", func() {
		spyLookup.results["a"] = 0
		spyLookup.results["b"] = 1

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

		Expect(spyIngressClient1.ctxs[0].Done()).To(BeClosed())
	})

	It("does not return an error if one of the clients returns an error", func() {
		spyIngressClient1.err = errors.New("some-error")

		spyLookup.results["a"] = 0
		spyLookup.results["b"] = 1

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
	results   map[string]int
}

func newSpyLookup() *spyLookup {
	return &spyLookup{
		results: make(map[string]int),
	}
}

func (s *spyLookup) Lookup(sourceID string) int {
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
