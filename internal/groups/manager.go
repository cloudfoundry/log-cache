package groups

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"google.golang.org/grpc"
)

// Manager manages groups. It implements logcache.GroupReader.
type Manager struct {
	mu sync.RWMutex
	m  map[string][]string
	s  DataStorage
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
	) []*loggregator_v2.Envelope

	// Add starts fetching data for the given sourceID.
	Add(name, sourceID string)

	// Remove stops fetching data for the given sourceID.
	Remove(name, sourceID string)
}

// NewManager creates a new Manager to manage groups.
func NewManager(s DataStorage) *Manager {
	return &Manager{
		m: make(map[string][]string),
		s: s,
	}
}

// AddToGroup creates the given group if it does not exist or adds the
// sourceID if it does.
func (m *Manager) AddToGroup(ctx context.Context, r *logcache.AddToGroupRequest, _ ...grpc.CallOption) (*logcache.AddToGroupResponse, error) {
	if r.GetName() == "" || r.GetSourceId() == "" {
		return nil, errors.New("name and source_id fields are required")
	}

	if r.GetName() == "read" {
		return nil, errors.New("name 'read' is reserved")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.m[r.Name] = append(m.m[r.Name], r.SourceId)
	m.s.Add(r.Name, r.SourceId)

	return &logcache.AddToGroupResponse{}, nil
}

// RemoveFromGroup removes a source ID from the given group. If that was the
// last entry, then the group is removed. If the group already didn't exist,
// then it is a nop.
func (m *Manager) RemoveFromGroup(ctx context.Context, r *logcache.RemoveFromGroupRequest, _ ...grpc.CallOption) (*logcache.RemoveFromGroupResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	a, ok := m.m[r.Name]
	if !ok {
		return &logcache.RemoveFromGroupResponse{}, nil
	}

	for i, x := range a {
		if x == r.SourceId {
			m.m[r.Name] = append(a[:i], a[i+1:]...)
			m.s.Remove(r.Name, r.SourceId)
			break
		}
	}

	if len(m.m[r.Name]) == 0 {
		delete(m.m, r.Name)
	}
	return &logcache.RemoveFromGroupResponse{}, nil
}

func (m *Manager) Read(ctx context.Context, r *logcache.GroupReadRequest, _ ...grpc.CallOption) (*logcache.GroupReadResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.m[r.Name]
	if !ok {
		return nil, fmt.Errorf("unknown group name: %s", r.GetName())
	}

	if r.GetEndTime() == 0 {
		r.EndTime = time.Now().UnixNano()
	}

	batch := m.s.Get(r.GetName(), time.Unix(0, r.GetStartTime()), time.Unix(0, r.GetEndTime()), nil, 100)

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
	return &logcache.GroupResponse{
		SourceIds: m.m[r.Name],
	}, nil
}
