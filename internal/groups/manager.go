package groups

import (
	"context"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Manager manages groups. It implements logcache.GroupReader.
type Manager struct {
	mu      sync.RWMutex
	m       map[string]groupInfo
	s       DataStorage
	timeout time.Duration
}

// DataStorage is used to store data for a given group.
type DataStorage interface {
	// Get fetches envelopes from the store based on the source ID, start and
	// end time. Start is inclusive while end is not: [start..end).
	Get(
		name string,
		start time.Time,
		end time.Time,
		envelopeType store.EnvelopeType,
		limit int,
		descending bool,
		requesterID uint64,
	) []*loggregator_v2.Envelope

	// Add starts fetching data for the given sourceID.
	Add(name, sourceID string)

	// AddRequester adds a requester ID for a given group.
	AddRequester(name string, requesterID uint64)

	// Remove stops fetching data for the given sourceID.
	Remove(name, sourceID string)

	// RemoveRequester removes a requester ID for a given group.
	RemoveRequester(name string, requesterID uint64)
}

// NewManager creates a new Manager to manage groups.
func NewManager(s DataStorage, timeout time.Duration) *Manager {
	return &Manager{
		m:       make(map[string]groupInfo),
		s:       s,
		timeout: timeout,
	}
}

// AddToGroup creates the given group if it does not exist or adds the
// sourceID if it does. The source ID will expire after a configurable amount
// of time. Therefore, the source ID should be constantly added. It is a NOP
// to add a source ID to a group if the source ID already exists.
func (m *Manager) AddToGroup(ctx context.Context, r *logcache.AddToGroupRequest, _ ...grpc.CallOption) (*logcache.AddToGroupResponse, error) {
	if r.GetName() == "" || r.GetSourceId() == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "name and source_id fields are required")
	}

	if len(r.GetName()) > 128 || len(r.GetSourceId()) > 128 {
		return nil, grpc.Errorf(codes.InvalidArgument, "name and source_id fields can only be 128 bytes long")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	gi, ok := m.m[r.Name]
	if !ok {
		gi.sourceIDs = make(map[string]*time.Timer)
		gi.requesterIDs = make(map[uint64]time.Time)
	}

	// Ensure that sourceID is not already tracked.
	if expire, ok := gi.sourceIDs[r.SourceId]; ok {
		m.resetExpire(expire)
		return &logcache.AddToGroupResponse{}, nil
	}

	gi.sourceIDs[r.SourceId] = time.AfterFunc(m.timeout, func() {
		m.removeFromGroup(r.Name, r.SourceId)
	})

	m.m[r.Name] = gi
	m.s.Add(r.Name, r.SourceId)

	return &logcache.AddToGroupResponse{}, nil
}

// RemoveFromGroup removes a source ID from the given group. If that was the
// last entry, then the group is removed. If the group already didn't exist,
// then it is a NOP.
func (m *Manager) RemoveFromGroup(ctx context.Context, r *logcache.RemoveFromGroupRequest, _ ...grpc.CallOption) (*logcache.RemoveFromGroupResponse, error) {
	m.removeFromGroup(r.Name, r.SourceId)
	return &logcache.RemoveFromGroupResponse{}, nil
}

func (m *Manager) removeFromGroup(name, sourceID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.m[name]
	if !ok {
		return
	}

	if _, ok := a.sourceIDs[sourceID]; ok {
		delete(a.sourceIDs, sourceID)
		m.s.Remove(name, sourceID)
	}
	m.m[name] = a

	if len(m.m[name].sourceIDs) == 0 {
		delete(m.m, name)
	}
}

// Read reads from a group. As a side effect, this first prunes any expired
// requesters for the group. This is to ensure that the current read will read
// from the most sourceIDs necessary.
func (m *Manager) Read(ctx context.Context, r *logcache.GroupReadRequest, _ ...grpc.CallOption) (*logcache.GroupReadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gi, ok := m.m[r.Name]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "unknown group name: %s", r.GetName())
	}

	if _, ok := gi.requesterIDs[r.RequesterId]; !ok {
		m.s.AddRequester(r.Name, r.RequesterId)
	}
	gi.requesterIDs[r.RequesterId] = time.Now()

	// Check for expired requesters
	for k, v := range m.m[r.Name].requesterIDs {
		if time.Since(v) >= m.timeout {
			delete(m.m[r.Name].requesterIDs, k)
			m.s.RemoveRequester(r.Name, k)
		}
	}

	if r.GetEndTime() == 0 {
		r.EndTime = time.Now().UnixNano()
	}

	if r.GetLimit() == 0 {
		r.Limit = 100
	}

	batch := m.s.Get(
		r.GetName(),
		time.Unix(0, r.GetStartTime()),
		time.Unix(0, r.GetEndTime()),
		m.convertEnvelopeType(r.GetEnvelopeType()),
		int(r.GetLimit()),
		false,
		r.RequesterId,
	)

	return &logcache.GroupReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	}, nil
}

// Group returns information about the given group. If the group does not
// exist, the returned sourceID slice will be empty, but an error will not be
// returned.
func (m *Manager) Group(ctx context.Context, r *logcache.GroupRequest, _ ...grpc.CallOption) (*logcache.GroupResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a := m.m[r.Name]

	var reqIds []uint64
	for k := range a.requesterIDs {
		reqIds = append(reqIds, k)
	}

	var sourceIDs []string
	for sourceID := range a.sourceIDs {
		sourceIDs = append(sourceIDs, sourceID)
	}

	return &logcache.GroupResponse{
		SourceIds:    sourceIDs,
		RequesterIds: reqIds,
	}, nil
}

func (m *Manager) resetExpire(t *time.Timer) {
	if !t.Stop() && len(t.C) != 0 {
		<-t.C
	}
	t.Reset(m.timeout)
}

func (m *Manager) convertEnvelopeType(t logcache.EnvelopeTypes) store.EnvelopeType {
	switch t {
	case logcache.EnvelopeTypes_LOG:
		return &loggregator_v2.Log{}
	case logcache.EnvelopeTypes_COUNTER:
		return &loggregator_v2.Counter{}
	case logcache.EnvelopeTypes_GAUGE:
		return &loggregator_v2.Gauge{}
	case logcache.EnvelopeTypes_TIMER:
		return &loggregator_v2.Timer{}
	case logcache.EnvelopeTypes_EVENT:
		return &loggregator_v2.Event{}
	default:
		return nil
	}
}

type groupInfo struct {
	sourceIDs    map[string]*time.Timer
	requesterIDs map[uint64]time.Time
}
