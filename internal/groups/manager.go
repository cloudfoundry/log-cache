package groups

import (
	"context"
	"fmt"
	"io"
	"sync"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"google.golang.org/grpc"
)

// Manager manages groups. It implements logcache.GroupReader.
type Manager struct {
	mu sync.RWMutex
	m  map[string]groupInfo
	s  func() DataStorage
}

// DataStorage is used to store data for a given group.
type DataStorage interface {
	io.Closer

	// Next reads the next envelope batch from storage. This is a destructive
	// method that prunes data. Meaning the data can only be fetched once.
	Next() []*loggregator_v2.Envelope

	// Add starts fetching data for the given sourceID.
	Add(sourceID string)

	// Remove stops fetching data for the given sourceID.
	Remove(sourceID string)
}

// NewManager creates a new Manager to manage groups.
func NewManager(s func() DataStorage) *Manager {
	return &Manager{
		m: make(map[string]groupInfo),
		s: s,
	}
}

// AddToGroup creates the given group if it does not exist or adds the
// sourceID if it does.
func (m *Manager) AddToGroup(ctx context.Context, r *logcache.AddToGroupRequest, _ ...grpc.CallOption) (*logcache.AddToGroupResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	s, ok := m.m[r.Name]
	if !ok {
		s.s = m.s()
	}
	s.sourceIDs = append(s.sourceIDs, r.SourceId)
	s.s.Add(r.SourceId)

	m.m[r.Name] = s

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

	for i, x := range a.sourceIDs {
		if x == r.SourceId {
			a.sourceIDs = append(a.sourceIDs[:i], a.sourceIDs[i+1:]...)
			a.s.Remove(r.SourceId)
			break
		}
	}
	m.m[r.Name] = a

	if len(a.sourceIDs) == 0 {
		a.s.Close()
		delete(m.m, r.Name)
	}
	return &logcache.RemoveFromGroupResponse{}, nil
}

func (m *Manager) Read(ctx context.Context, r *logcache.GroupReadRequest, _ ...grpc.CallOption) (*logcache.GroupReadResponse, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	g, ok := m.m[r.Name]
	if !ok {
		return nil, fmt.Errorf("unknown group name: %s", r.GetName())
	}

	batch := g.s.Next()

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
		SourceIds: m.m[r.Name].sourceIDs,
	}, nil
}

type groupInfo struct {
	sourceIDs []string
	s         DataStorage
}
