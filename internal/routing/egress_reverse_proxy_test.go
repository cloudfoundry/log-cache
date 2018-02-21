package routing_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EgressReverseProxy", func() {
	var (
		spyLookup        *spyLookup
		spyEgressClient1 *spyEgressClient
		spyEgressClient2 *spyEgressClient
		p                *routing.EgressReverseProxy
	)

	BeforeEach(func() {
		spyLookup = newSpyLookup()
		spyEgressClient1 = newSpyEgressClient()
		spyEgressClient2 = newSpyEgressClient()
		p = routing.NewEgressReverseProxy(spyLookup.Lookup, []rpc.EgressClient{
			spyEgressClient1,
			spyEgressClient2,
		}, 0, log.New(ioutil.Discard, "", 0))
	})

	It("uses the correct client", func() {
		spyLookup.results["a"] = 0
		spyLookup.results["b"] = 1
		expected := &rpc.ReadResponse{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
				},
			},
		}
		spyEgressClient1.readResp = expected

		resp, err := p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp).To(Equal(expected))

		_, err = p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "b",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyLookup.sourceIDs).To(ConsistOf("a", "b"))

		Expect(spyEgressClient1.reqs).To(ConsistOf(&rpc.ReadRequest{
			SourceId: "a",
		}))

		Expect(spyEgressClient2.reqs).To(ConsistOf(&rpc.ReadRequest{
			SourceId: "b",
		}))
	})

	It("uses the given context", func() {
		spyLookup.results["a"] = 0

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := p.Read(ctx, &rpc.ReadRequest{
			SourceId: "a",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyEgressClient1.ctxs[0].Done()).To(BeClosed())
	})

	It("returns an error if the clients returns an error", func() {
		spyEgressClient1.err = errors.New("some-error")

		spyLookup.results["a"] = 0
		spyLookup.results["b"] = 1

		_, err := p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "a",
		})
		Expect(err).To(HaveOccurred())
	})

	It("gets meta from the local store", func() {
		spyEgressClient1.metaResults = map[string]*rpc.MetaInfo{
			"source-1": &rpc.MetaInfo{
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
			"source-2": &rpc.MetaInfo{
				Count:           5,
				Expired:         6,
				OldestTimestamp: 7,
				NewestTimestamp: 8,
			},
		}

		resp, err := p.Meta(context.Background(), &rpc.MetaRequest{
			LocalOnly: true,
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.Meta).To(HaveKeyWithValue("source-1", &rpc.MetaInfo{
			Count:           1,
			Expired:         2,
			OldestTimestamp: 3,
			NewestTimestamp: 4,
		}))
		Expect(resp.Meta).To(HaveKeyWithValue("source-2", &rpc.MetaInfo{
			Count:           5,
			Expired:         6,
			OldestTimestamp: 7,
			NewestTimestamp: 8,
		}))

		Expect(spyEgressClient1.metaRequests).To(ConsistOf(&rpc.MetaRequest{LocalOnly: true}))
		Expect(spyEgressClient2.metaRequests).To(BeEmpty())
	})

	It("gets sourceIds from the remote store and the local store", func() {
		spyEgressClient1.metaResults = map[string]*rpc.MetaInfo{
			"source-1": &rpc.MetaInfo{
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
			"source-2": &rpc.MetaInfo{
				Count:           5,
				Expired:         6,
				OldestTimestamp: 7,
				NewestTimestamp: 8,
			},
		}

		spyEgressClient2.metaResults = map[string]*rpc.MetaInfo{
			"source-3": &rpc.MetaInfo{
				Count:           9,
				Expired:         10,
				OldestTimestamp: 11,
				NewestTimestamp: 12,
			},
		}

		resp, err := p.Meta(context.Background(), &rpc.MetaRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Meta).To(HaveKeyWithValue("source-1", &rpc.MetaInfo{
			Count:           1,
			Expired:         2,
			OldestTimestamp: 3,
			NewestTimestamp: 4,
		}))
		Expect(resp.Meta).To(HaveKeyWithValue("source-2", &rpc.MetaInfo{
			Count:           5,
			Expired:         6,
			OldestTimestamp: 7,
			NewestTimestamp: 8,
		}))
		Expect(resp.Meta).To(HaveKeyWithValue("source-3", &rpc.MetaInfo{
			Count:           9,
			Expired:         10,
			OldestTimestamp: 11,
			NewestTimestamp: 12,
		}))

		Expect(spyEgressClient1.metaRequests).To(ConsistOf(&rpc.MetaRequest{LocalOnly: true}))
		Expect(spyEgressClient2.metaRequests).To(ConsistOf(&rpc.MetaRequest{LocalOnly: true}))
	})

	It("uses the given context for meta", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p.Meta(ctx, &rpc.MetaRequest{
			LocalOnly: true,
		})

		Expect(spyEgressClient1.ctxs[0].Done()).To(BeClosed())
	})

	It("returns an error if one of the remotes returns an error", func() {
		spyEgressClient2.metaErr = errors.New("errors")

		_, err := p.Meta(context.Background(), &rpc.MetaRequest{})
		Expect(err).To(HaveOccurred())
	})
})

type spyEgressClient struct {
	readResp *rpc.ReadResponse
	ctxs     []context.Context
	reqs     []*rpc.ReadRequest
	err      error

	metaRequests []*rpc.MetaRequest
	metaResults  map[string]*rpc.MetaInfo
	metaErr      error
}

func newSpyEgressClient() *spyEgressClient {
	return &spyEgressClient{
		readResp: &rpc.ReadResponse{},
	}
}

func (s *spyEgressClient) Read(ctx context.Context, in *rpc.ReadRequest, opts ...grpc.CallOption) (*rpc.ReadResponse, error) {
	s.ctxs = append(s.ctxs, ctx)
	s.reqs = append(s.reqs, in)
	return s.readResp, s.err
}

func (s *spyEgressClient) Meta(ctx context.Context, r *rpc.MetaRequest, opts ...grpc.CallOption) (*rpc.MetaResponse, error) {
	s.ctxs = append(s.ctxs, ctx)
	s.metaRequests = append(s.metaRequests, r)
	metaInfo := make(map[string]*rpc.MetaInfo)
	for id, m := range s.metaResults {
		metaInfo[id] = m
	}

	return &rpc.MetaResponse{
		Meta: metaInfo,
	}, s.metaErr
}
