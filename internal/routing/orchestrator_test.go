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
		spyRangeSetter *spyRangeSetter
		o              *routing.Orchestrator
	)

	BeforeEach(func() {
		spyRangeSetter = newSpyRangeSetter()
		o = routing.NewOrchestrator(spyRangeSetter)
	})

	It("keeps track of the ranges", func() {
		_, err := o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 1,
				End:   2,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		_, err = o.AddRange(context.Background(), &rpc.AddRangeRequest{
			Range: &rpc.Range{
				Start: 3,
				End:   4,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err := o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf(
			&rpc.Range{
				Start: 1,
				End:   2,
			},

			&rpc.Range{
				Start: 3,
				End:   4,
			},
		))

		_, err = o.RemoveRange(context.Background(), &rpc.RemoveRangeRequest{
			Range: &rpc.Range{
				Start: 1,
				End:   2,
			},
		})
		Expect(err).ToNot(HaveOccurred())

		resp, err = o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Ranges).To(ConsistOf(
			&rpc.Range{
				Start: 3,
				End:   4,
			},
		))
	})

	It("survives race the detector", func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func(o *routing.Orchestrator) {
			wg.Done()
			for i := 0; i < 100; i++ {
				o.ListRanges(context.Background(), &rpc.ListRangesRequest{})
			}
		}(o)

		go func(o *routing.Orchestrator) {
			wg.Done()
			for i := 0; i < 100; i++ {
				o.RemoveRange(context.Background(), &rpc.RemoveRangeRequest{
					Range: &rpc.Range{
						Start: 1,
						End:   2,
					},
				})
			}
		}(o)

		wg.Wait()

		for i := 0; i < 100; i++ {
			o.AddRange(context.Background(), &rpc.AddRangeRequest{
				Range: &rpc.Range{
					Start: 1,
					End:   2,
				},
			})
		}
	})

	It("passes through SetRanges requests", func() {
		expected := &rpc.SetRangesRequest{
			Ranges: map[string]*rpc.Ranges{
				"a": &rpc.Ranges{
					Ranges: []*rpc.Range{
						{
							Start: 1,
						},
					},
				},
			},
		}
		o.SetRanges(context.Background(), expected)
		Expect(spyRangeSetter.requests).To(ConsistOf(expected))
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
