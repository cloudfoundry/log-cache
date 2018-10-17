package routing

import (
	"context"
	"sync"

	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
)

// OrchestratorAgent manages the Log Cache node's routes.
type OrchestratorAgent struct {
	mu     sync.RWMutex
	ranges []*rpc.Range

	s RangeSetter
}

type RangeSetter interface {
	// SetRanges is used as a pass through for the orchestration service's
	// SetRanges method.
	SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error)
}

// NewOrchestratorAgent returns a new OrchestratorAgent.
func NewOrchestratorAgent(s RangeSetter) *OrchestratorAgent {
	return &OrchestratorAgent{
		s: s,
	}
}

// AddRange adds a range (from the scheduler) for data to be routed to.
func (o *OrchestratorAgent) AddRange(ctx context.Context, r *rpc.AddRangeRequest) (*rpc.AddRangeResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.ranges = append(o.ranges, r.Range)

	return &rpc.AddRangeResponse{}, nil
}

// RemoveRange removes a range (form the scheduler) for the data to be routed
// to.
func (o *OrchestratorAgent) RemoveRange(ctx context.Context, req *rpc.RemoveRangeRequest) (*rpc.RemoveRangeResponse, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	for i, r := range o.ranges {
		if r.Start == req.Range.Start && r.End == req.Range.End {
			o.ranges = append(o.ranges[:i], o.ranges[i+1:]...)
			break
		}
	}

	return &rpc.RemoveRangeResponse{}, nil
}

// ListRanges returns all the ranges that are currently active.
func (o *OrchestratorAgent) ListRanges(ctx context.Context, r *rpc.ListRangesRequest) (*rpc.ListRangesResponse, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return &rpc.ListRangesResponse{
		Ranges: o.ranges,
	}, nil
}

// SetRanges passes them along to the RangeSetter.
func (o *OrchestratorAgent) SetRanges(ctx context.Context, in *rpc.SetRangesRequest) (*rpc.SetRangesResponse, error) {
	return o.s.SetRanges(ctx, in)
}
