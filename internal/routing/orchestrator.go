package routing

import (
	"context"
	"sync"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
)

// Orchestrator manages the Log Cache node's routes.
type Orchestrator struct {
	mu     sync.RWMutex
	ranges []*rpc.Range

	s RangeSetter
}

type RangeSetter interface {
	// SetRanges is used as a pass through for the orchestration service's
	// SetRanges method.
	SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error)
}

// RangeSetterFunc turns a function into a RangeSetter.
type RangeSetterFunc func(in *rpc.SetRangesRequest)

// SetRanges implements RangeSetter
func (f RangeSetterFunc) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	f(in)
	return &rpc.SetRangesResponse{}, nil
}

// NewOrchestrator returns a new Orchestrator.
func NewOrchestrator(s RangeSetter) *Orchestrator {
	return &Orchestrator{
		s: s,
	}
}

// AddRange adds a range (from the scheduler) for data to be routed to.
func (o *Orchestrator) AddRange(ctx context.Context, r *rpc.AddRangeRequest) (*rpc.AddRangeResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.ranges = append(o.ranges, r.Range)

	return &rpc.AddRangeResponse{}, nil
}

// RemoveRange removes a range (form the scheduler) for the data to be routed
// to.
func (o *Orchestrator) RemoveRange(ctx context.Context, req *rpc.RemoveRangeRequest) (*rpc.RemoveRangeResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for i, r := range o.ranges {
		if *r == *req.Range {
			o.ranges = append(o.ranges[:i], o.ranges[i+1:]...)
			break
		}
	}

	return &rpc.RemoveRangeResponse{}, nil
}

// ListRanges returns all the ranges that are currently active.
func (o *Orchestrator) ListRanges(ctx context.Context, r *rpc.ListRangesRequest) (*rpc.ListRangesResponse, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return &rpc.ListRangesResponse{
		Ranges: o.ranges,
	}, nil
}

// SetRanges passes them along to the RangeSetter.
func (o *Orchestrator) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	return o.s.SetRanges(ctx, in)
}
