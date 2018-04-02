package replication

import (
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

type OnDemandSnapshotStore struct {
	mu   sync.RWMutex
	meta *raft.SnapshotMeta
	sink *snapshotSink
}

func NewOnDemandSnapshotStore() *OnDemandSnapshotStore {
	return &OnDemandSnapshotStore{}
}

func (s *OnDemandSnapshotStore) Create(
	version raft.SnapshotVersion,
	index uint64,
	term uint64,
	configuration raft.Configuration,
	configurationIndex uint64,
	trans raft.Transport,
) (raft.SnapshotSink, error) {
	if version != 1 {
		return nil, errors.New("raft snapshot version must be 1")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.meta = &raft.SnapshotMeta{
		Version:            version,
		ID:                 "on-demand",
		Index:              index,
		Term:               term,
		Configuration:      configuration,
		ConfigurationIndex: configurationIndex,
	}

	s.sink = &snapshotSink{
		c: make(chan []byte, 1000),
	}

	return s.sink, nil
}

func (s *OnDemandSnapshotStore) List() ([]*raft.SnapshotMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.meta == nil {
		return nil, nil
	}

	return []*raft.SnapshotMeta{s.meta}, nil
}

func (s *OnDemandSnapshotStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.meta == nil || id != "on-demand" {
		return nil, nil, fmt.Errorf("unknown snapshot %s", id)
	}

	return s.meta, &snapshotReader{c: s.sink.c}, nil
}

type snapshotSink struct {
	once sync.Once
	c    chan []byte
}

func (s *snapshotSink) Write(p []byte) (n int, err error) {
	m := make([]byte, len(p))
	copy(m, p)
	select {
	case s.c <- m:
	default:
		return 0, errors.New("slow snapshot consumer")
	}

	return len(p), nil
}

func (s *snapshotSink) Close() error {
	s.once.Do(func() {
		close(s.c)
	})
	return nil
}

func (s *snapshotSink) ID() string {
	return "on-demand"
}

func (s *snapshotSink) Cancel() error {
	return nil
}

type snapshotReader struct {
	c      chan []byte
	closed bool
}

func (s *snapshotReader) Read(buffer []byte) (n int, err error) {
	if s.closed {
		return 0, io.EOF
	}

	d, ok := <-s.c
	if !ok {
		return 0, io.EOF
	}

	return copy(buffer, d), nil
}

func (s *snapshotReader) Close() error {
	s.closed = true
	return nil
}
