package routing

import (
	"context"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
)

// Orchestrator manages the Log Cache node's routes.
type Orchestrator struct {
	mu     sync.Mutex
	ranges []rpc.Range

	localAddr string
	f         MetaFetcher
	s         RangeSetter
	hasher    func(string) uint64
}

// MetaFetcher returns meta for the local store.
type MetaFetcher interface {
	// Meta returns item that the node is responsible for.
	Meta() []string
}

// MetaFetcherFunc transforms a function into a MetaFetcher.
type MetaFetcherFunc func() []string

// Meta implements MetaFetcher.
func (f MetaFetcherFunc) Meta() []string {
	return f()
}

type RangeSetter interface {
	// SetRanges is used as a pass through for the orchestration service's
	// SetRanges method.
	SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error)
}

// NewOrchestrator returns a new Orchestrator.
func NewOrchestrator(localAddr string, hasher func(string) uint64, f MetaFetcher, s RangeSetter) *Orchestrator {
	return &Orchestrator{
		localAddr: localAddr,
		hasher:    hasher,
		f:         f,
		s:         s,
	}
}

// AddRanges adds a range (from the scheduler) for data to be routed to.
func (o *Orchestrator) AddRange(ctx context.Context, r *rpc.AddRangeRequest) (*rpc.AddRangeResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.ranges = append(o.ranges, *r.Range)

	return &rpc.AddRangeResponse{}, nil
}

// ListRanges returns all the ranges that are currently active. This includes
// the ones with the latest term, and older ones that there is meta for.
func (o *Orchestrator) ListRanges(ctx context.Context, r *rpc.ListRangesRequest) (*rpc.ListRangesResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Find latest term.
	var latestTerm uint64
	for _, r := range o.ranges {
		if latestTerm < r.Term {
			latestTerm = r.Term
		}
	}

	// Separate old vs latest ranges.
	var oldRanges []*rpc.Range
	var ranges []*rpc.Range
	for i := range o.ranges {
		r := o.ranges[i]
		if r.Term != latestTerm {
			oldRanges = append(oldRanges, &r)
			continue
		}
		ranges = append(ranges, &r)
	}

	// Keep latest ranges, and add back any old ones that have a hash
	// associated with it that are not covered by a newer range.
	for _, k := range o.f.Meta() {
		h := o.hasher(k)

		if o.findRange(h, ranges) >= 0 {
			continue
		}

		// Not covered in newer range, try old range.
		if i := o.findRange(h, oldRanges); i >= 0 {
			ranges = append(ranges, oldRanges[i])
		}
	}

	// Update results
	o.ranges = nil
	for _, r := range ranges {
		o.ranges = append(o.ranges, *r)
	}

	return &rpc.ListRangesResponse{
		Ranges: ranges,
	}, nil
}

func (o *Orchestrator) findRange(h uint64, rs []*rpc.Range) int {
	for i, r := range rs {
		if h < r.Start || h > r.End {
			// Outside of range
			continue
		}
		return i
	}

	return -1
}

// SetRanges saves the ranges and passes them along to the RangeSetter.
func (o *Orchestrator) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	if x, ok := in.Ranges[o.localAddr]; ok {
		o.mu.Lock()
		o.ranges = nil
		for _, r := range x.Ranges {
			o.ranges = append(o.ranges, *r)
		}
		o.mu.Unlock()
	}

	return o.s.SetRanges(ctx, in)
}
