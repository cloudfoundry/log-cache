package groups

import (
	"context"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Manager manages groups. It implements logcache_v1.GroupReader.
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
		envelopeTypes []store.EnvelopeType,
		limit int,
		descending bool,
		requesterID uint64,
	) []*loggregator_v2.Envelope

	// Add starts fetching data for the given sourceID.
	Add(name, sourceID string)

	// AddRequester adds a requester ID for a given group.
	AddRequester(name string, requesterID uint64, remoteOnly bool)

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
func (m *Manager) AddToGroup(ctx context.Context, r *logcache_v1.AddToGroupRequest, _ ...grpc.CallOption) (*logcache_v1.AddToGroupResponse, error) {
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
		return &logcache_v1.AddToGroupResponse{}, nil
	}

	gi.sourceIDs[r.SourceId] = time.AfterFunc(m.timeout, func() {
		m.removeFromGroup(r.Name, r.SourceId)
	})

	m.m[r.Name] = gi
	m.s.Add(r.Name, r.SourceId)

	return &logcache_v1.AddToGroupResponse{}, nil
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
func (m *Manager) Read(ctx context.Context, r *logcache_v1.GroupReadRequest, _ ...grpc.CallOption) (*logcache_v1.GroupReadResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	gi, ok := m.m[r.Name]
	if !ok {
		return nil, grpc.Errorf(codes.NotFound, "unknown group name: %s", r.GetName())
	}

	if _, ok := gi.requesterIDs[r.RequesterId]; !ok {
		// Negative limit implies that we are only pinging the requester ID
		// and don't want any data.
		m.s.AddRequester(r.Name, r.RequesterId, r.GetLimit() < 0)
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

	if r.GetLimit() < 0 {
		// Negative limit implies that we are only pinging the requester ID
		// and don't want any data.
		return &logcache_v1.GroupReadResponse{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: nil,
			},
		}, nil
	}

	if r.GetLimit() == 0 {
		r.Limit = 100
	}
	var t []store.EnvelopeType
	for _, e := range r.GetEnvelopeTypes() {
		t = append(t, m.convertEnvelopeType(e))
	}
	batch := m.s.Get(
		r.GetName(),
		time.Unix(0, r.GetStartTime()),
		time.Unix(0, r.GetEndTime()),
		t,
		int(r.GetLimit()),
		false,
		r.RequesterId,
	)

	return &logcache_v1.GroupReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	}, nil
}

// Group returns information about the given group. If the group does not
// exist, the returned sourceID slice will be empty, but an error will not be
// returned.
func (m *Manager) Group(ctx context.Context, r *logcache_v1.GroupRequest, _ ...grpc.CallOption) (*logcache_v1.GroupResponse, error) {
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

	return &logcache_v1.GroupResponse{
		SourceIds:    sourceIDs,
		RequesterIds: reqIds,
	}, nil
}

// ListGroups returns all the group names.
func (m *Manager) ListGroups() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []string
	for name := range m.m {
		results = append(results, name)
	}

	return results
}

func (m *Manager) resetExpire(t *time.Timer) {
	if !t.Stop() && len(t.C) != 0 {
		<-t.C
	}
	t.Reset(m.timeout)
}

func (m *Manager) convertEnvelopeType(t logcache_v1.EnvelopeType) store.EnvelopeType {
	switch t {
	case logcache_v1.EnvelopeType_LOG:
		return &loggregator_v2.Log{}
	case logcache_v1.EnvelopeType_COUNTER:
		return &loggregator_v2.Counter{}
	case logcache_v1.EnvelopeType_GAUGE:
		return &loggregator_v2.Gauge{}
	case logcache_v1.EnvelopeType_TIMER:
		return &loggregator_v2.Timer{}
	case logcache_v1.EnvelopeType_EVENT:
		return &loggregator_v2.Event{}
	default:
		return nil
	}
}

type groupInfo struct {
	sourceIDs    map[string]*time.Timer
	requesterIDs map[uint64]time.Time
}
