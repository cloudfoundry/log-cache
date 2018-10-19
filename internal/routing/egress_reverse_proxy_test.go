package routing_test

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EgressReverseProxy", func() {
	var (
		spyLookup              *spyLookup
		spyEgressLocalClient   *spyEgressClient
		spyEgressRemoteClient1 *spyEgressClient
		spyEgressRemoteClient2 *spyEgressClient
		p                      *routing.EgressReverseProxy
	)

	BeforeEach(func() {
		spyLookup = newSpyLookup()
		spyEgressLocalClient = newSpyEgressClient()
		spyEgressRemoteClient1 = newSpyEgressClient()
		spyEgressRemoteClient2 = newSpyEgressClient()
		p = routing.NewEgressReverseProxy(spyLookup.Lookup, []rpc.EgressClient{
			spyEgressLocalClient,
			spyEgressRemoteClient1,
			spyEgressRemoteClient2,
		}, 0, log.New(ioutil.Discard, "", 0),
			routing.WithMetaCacheDuration(50*time.Millisecond),
		)
	})

	It("uses a correct client", func() {
		spyLookup.results["a"] = []int{0}
		spyLookup.results["b"] = []int{1}
		spyLookup.results["c"] = []int{1, 2}
		expected := &rpc.ReadResponse{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{SourceId: "a", Timestamp: 1},
				},
			},
		}
		spyEgressLocalClient.readResp = expected

		resp, err := p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "a",
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp).To(Equal(expected))

		_, err = p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "b",
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "c",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyLookup.sourceIDs).To(ConsistOf("a", "b", "c"))

		Expect(spyEgressLocalClient.reqs).To(ConsistOf(&rpc.ReadRequest{
			SourceId: "a",
		}))

		Expect(spyEgressRemoteClient1.reqs).To(ContainElement(&rpc.ReadRequest{
			SourceId: "b",
		}))

		Expect(len(spyEgressRemoteClient1.reqs) + len(spyEgressRemoteClient2.reqs)).To(Equal(2))
		Expect(append(spyEgressRemoteClient1.reqs, spyEgressRemoteClient2.reqs...)).To(ContainElement(&rpc.ReadRequest{
			SourceId: "c",
		}))
	})

	It("evenly distributes requests between remote clients", func() {
		spyLookup.results["a"] = []int{1, 2}
		for i := 0; i < 1000; i++ {
			_, err := p.Read(context.Background(), &rpc.ReadRequest{
				SourceId: "a",
			})
			Expect(err).ToNot(HaveOccurred())
		}

		Expect(len(spyEgressRemoteClient1.reqs) + len(spyEgressRemoteClient2.reqs)).To(Equal(1000))
		Expect(len(spyEgressRemoteClient1.reqs)).To(BeNumerically("~", 500, 100))
		Expect(len(spyEgressRemoteClient2.reqs)).To(BeNumerically("~", 500, 100))
	})

	It("prefers the local client", func() {
		spyLookup.results["a"] = []int{0, 1, 2}

		for i := 0; i < 1000; i++ {
			_, err := p.Read(context.Background(), &rpc.ReadRequest{
				SourceId: "a",
			})
			Expect(err).ToNot(HaveOccurred())
		}

		Expect(spyEgressLocalClient.reqs).To(HaveLen(1000))
		Expect(spyEgressLocalClient.reqs).To(ContainElement(&rpc.ReadRequest{
			SourceId: "a",
		}))
	})

	It("returns an Unavailable error for an unroutable request", func() {
		_, err := p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "c",
		})
		Expect(grpc.Code(err)).To(Equal(codes.Unavailable))
	})

	It("uses the given context", func() {
		spyLookup.results["a"] = []int{0}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := p.Read(ctx, &rpc.ReadRequest{
			SourceId: "a",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyEgressLocalClient.ctxs[0].Done()).To(BeClosed())
	})

	It("returns an error if the clients returns an error", func() {
		spyEgressLocalClient.err = errors.New("some-error")

		spyLookup.results["a"] = []int{0}
		spyLookup.results["b"] = []int{1}

		_, err := p.Read(context.Background(), &rpc.ReadRequest{
			SourceId: "a",
		})
		Expect(err).To(HaveOccurred())
	})

	It("gets meta from the local store", func() {
		spyEgressLocalClient.metaResults = map[string]*rpc.MetaInfo{
			"source-1": {
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
			"source-2": {
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

		Expect(spyEgressLocalClient.metaRequests).To(ConsistOf(&rpc.MetaRequest{LocalOnly: true}))
		Expect(spyEgressRemoteClient1.metaRequests).To(BeEmpty())
	})

	It("gets sourceIds from the remote store and the local store", func() {
		spyEgressLocalClient.metaResults = map[string]*rpc.MetaInfo{
			"source-1": {
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
			"source-2": {
				Count:           5,
				Expired:         6,
				OldestTimestamp: 7,
				NewestTimestamp: 8,
			},
		}

		spyEgressRemoteClient1.metaResults = map[string]*rpc.MetaInfo{
			"source-3": {
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

		Expect(spyEgressLocalClient.metaRequests).To(ConsistOf(&rpc.MetaRequest{LocalOnly: true}))
		Expect(spyEgressRemoteClient1.metaRequests).To(ConsistOf(&rpc.MetaRequest{LocalOnly: true}))
	})

	It("gets sourceIds from the cache rather than the meta store", func() {
		spyEgressLocalClient.metaResults = map[string]*rpc.MetaInfo{}
		spyEgressRemoteClient1.metaResults = map[string]*rpc.MetaInfo{}

		_, err := p.Meta(context.Background(), &rpc.MetaRequest{})
		Expect(err).ToNot(HaveOccurred())

		_, err = p.Meta(context.Background(), &rpc.MetaRequest{})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyEgressLocalClient.metaCalls).To(Equal(1))
		Expect(spyEgressRemoteClient1.metaCalls).To(Equal(1))
	})

	It("gets sourceIds from the cache rather than the meta store with local only", func() {
		spyEgressLocalClient.metaResults = map[string]*rpc.MetaInfo{
			"source-1": {},
			"source-2": {},
		}
		spyEgressRemoteClient1.metaResults = map[string]*rpc.MetaInfo{
			"source-3": {},
		}

		respA, err := p.Meta(context.Background(), &rpc.MetaRequest{LocalOnly: true})
		Expect(err).ToNot(HaveOccurred())

		respB, err := p.Meta(context.Background(), &rpc.MetaRequest{LocalOnly: false})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyEgressLocalClient.metaCalls).To(Equal(2))
		Expect(spyEgressRemoteClient1.metaCalls).To(Equal(1))

		Expect(respA.Meta).To(HaveLen(2))
		Expect(respB.Meta).To(HaveLen(3))
	})

	It("times out the meta cache", func() {
		spyEgressLocalClient.metaResults = map[string]*rpc.MetaInfo{}
		spyEgressRemoteClient1.metaResults = map[string]*rpc.MetaInfo{}

		Eventually(func() int {
			_, err := p.Meta(context.Background(), &rpc.MetaRequest{})
			Expect(err).ToNot(HaveOccurred())

			return spyEgressRemoteClient1.metaCalls
		}, 2).Should(BeNumerically(">", 1))
	})

	It("uses the given context for meta", func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		p.Meta(ctx, &rpc.MetaRequest{
			LocalOnly: true,
		})

		Expect(spyEgressLocalClient.ctxs[0].Done()).To(BeClosed())
	})

	It("returns partial results if some of the remotes return an error", func() {
		spyEgressRemoteClient1.metaErr = errors.New("errors")

		result, err := p.Meta(context.Background(), &rpc.MetaRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(result).ToNot(BeNil())
	})

	It("returns an error if all of the remotes returns an error", func() {
		spyEgressLocalClient.metaErr = errors.New("errors")
		spyEgressRemoteClient1.metaErr = errors.New("errors")
		spyEgressRemoteClient2.metaErr = errors.New("errors")

		_, err := p.Meta(context.Background(), &rpc.MetaRequest{})
		Expect(err).To(HaveOccurred())
	})
})

type spyEgressClient struct {
	readResp *rpc.ReadResponse
	ctxs     []context.Context
	reqs     []*rpc.ReadRequest
	err      error

	metaCalls    int
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
	s.metaCalls += 1
	s.ctxs = append(s.ctxs, ctx)
	s.metaRequests = append(s.metaRequests, r)
	metaInfo := make(map[string]*rpc.MetaInfo)
	for id, m := range s.metaResults {
		metaInfo[id] = m
	}

	if s.metaErr != nil {
		return nil, s.metaErr
	}

	return &rpc.MetaResponse{
		Meta: metaInfo,
	}, nil
}
