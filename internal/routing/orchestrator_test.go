package routing_test

import (
	"context"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/log-cache/internal/routing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Orchestrator", func() {
	var (
		spyHasher      *spyHasher
		spyMetaFetcher *spyMetaFetcher
		spyRangeSetter *spyRangeSetter
		o              *routing.Orchestrator
	)

	BeforeEach(func() {
		spyHasher = newSpyHasher()
		spyMetaFetcher = newSpyMetaFetcher()
		spyRangeSetter = newSpyRangeSetter()
		o = routing.NewOrchestrator("a", spyHasher.Hash, spyMetaFetcher, spyRangeSetter)
	})

	It("always keep the latest term", func() {
		// SpyHasher returns 0 by default
		spyMetaFetcher.results = []string{"a"}
		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   1,
				Term:  1,
			},
		})

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   1,
				Term:  2,
			},
		})

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 1,
				End:   2,
				Term:  2,
			},
		})

		resp, err := o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf([]*rpc.Range{
			{
				Start: 0,
				End:   1,
				Term:  2,
			},
			{
				Start: 1,
				End:   2,
				Term:  2,
			},
		}))
	})

	It("keeps older terms with meta available", func() {
		// SpyHasher returns 0 by default
		spyMetaFetcher.results = []string{"a"}

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 0,
				End:   1,
				Term:  1,
			},
		})

		o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 1,
				End:   2,
				Term:  2,
			},
		})

		resp, err := o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf([]*rpc.Range{
			{
				Start: 0,
				End:   1,
				Term:  1,
			},
			{
				Start: 1,
				End:   2,
				Term:  2,
			},
		}))
	})

	It("survives race the detector", func() {
		var wg sync.WaitGroup
		wg.Add(1)
		go func(o *routing.Orchestrator) {
			wg.Done()
			for i := 0; i < 100; i++ {
				o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
			}
		}(o)
		wg.Wait()

		for i := 0; i < 100; i++ {
			o.AddRange(context.Background(), &rpc.AddRangeRequest{
				Range: &rpc.Range{
					Start: 1,
					End:   2,
					Term:  2,
				},
			})
		}
	})

	It("passes through SetRanges requests and keeps track of the latest ranges", func() {
		expected := &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"a": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{
							Term: 1,
						},
					},
				},
			},
		}
		o.SetRanges(context.Background(), expected)
		resp, err := o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyRangeSetter.requests).To(ConsistOf(expected))
		Expect(resp.Ranges).To(ConsistOf(&rpc.Range{
			Term: 1,
		}))
	})
})

type spyMetaFetcher struct {
	results []string
}

func newSpyMetaFetcher() *spyMetaFetcher {
	return &spyMetaFetcher{}
}

func (s *spyMetaFetcher) Meta() []string {
	return s.results
}

type spyHasher struct {
	mu      sync.Mutex
	ids     []string
	results []uint64
}

func newSpyHasher() *spyHasher {
	return &spyHasher{}
}

func (s *spyHasher) Hash(id string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, id)

	if len(s.results) == 0 {
		return 0
	}

	r := s.results[0]
	s.results = s.results[1:]
	return r
}

type spyRangeSetter struct {
	mu       sync.Mutex
	requests []*rpc.SetRangesRequest
}

func newSpyRangeSetter() *spyRangeSetter {
	return &spyRangeSetter{}
}

func (s *spyRangeSetter) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, in)
	return &rpc.SetRangesResponse{}, nil
}
