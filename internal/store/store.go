package store

import (
	"container/ring"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// Store is an in memory data store for envelopes. It will keep a bounded
// number and drop older data once that threshold is exceeded. All functions
// are thread safe.
type Store struct {
	mu     sync.RWMutex
	buffer *ring.Ring
}

// NewStore creates a new store.
func NewStore(size int) *Store {
	return &Store{
		buffer: ring.New(size),
	}
}

// Put adds a batch of envelopes into the store.
func (s *Store) Put(envs []*loggregator_v2.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range envs {
		s.buffer = s.buffer.Next()
		s.buffer.Value = e
	}
}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (s *Store) Get(sourceID string, start, end time.Time) []*loggregator_v2.Envelope {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var res []*loggregator_v2.Envelope
	s.buffer.Next().Do(func(v interface{}) {
		if v == nil {
			return
		}

		// We control how data is inserted and therefore can take the risk
		// that the type is not an Envelope.
		e := v.(*loggregator_v2.Envelope)

		// Time validation is [start..end)
		if e.SourceId == sourceID &&
			e.Timestamp >= start.UnixNano() &&
			e.Timestamp < end.UnixNano() {
			res = append(res, e)
		}
	})

	return res
}
