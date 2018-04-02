package store

import (
	"io"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// MultiStore contains several stores for each range. When the ranges are
// updated, ranges that are no longer tracked will be deleted (along with
// their data), otherwise the data will be left alone.
type MultiStore struct {
	mu              sync.RWMutex
	stores          map[logcache_v1.Range]SubStore
	subStoreCreator SubStoreCreator
	hasher          func(string) uint64
}

// SubStore stores and returns data.
type SubStore interface {
	io.Closer
	Get(index string, start time.Time, end time.Time, envelopeTypes []logcache_v1.EnvelopeType, limit int, descending bool) []*loggregator_v2.Envelope
	Meta() map[string]logcache_v1.MetaInfo
	Put(e *loggregator_v2.Envelope, index string)
}

// SubStoreCreator returns a new SubStore.
type SubStoreCreator func(logcache_v1.Range) SubStore

// NewMultiStore returns a new MultiStore.
func NewMultiStore(c SubStoreCreator, hasher func(string) uint64) *MultiStore {
	return &MultiStore{
		stores:          make(map[logcache_v1.Range]SubStore),
		subStoreCreator: c,
		hasher:          hasher,
	}
}

// Put adds data to the corresponding store (via the index).
func (s *MultiStore) Put(e *loggregator_v2.Envelope, index string) {
	ss := s.correspondingStore(index)
	if ss != nil {
		ss.Put(e, index)
	}
}

func (s *MultiStore) correspondingStore(index string) SubStore {
	s.mu.RLock()
	defer s.mu.RUnlock()

	idx := s.hasher(index)
	for r, ss := range s.stores {
		if idx >= r.Start && idx <= r.End {
			return ss
		}
	}
	return nil
}

// Get reads from the corresponding store (via the index) and returns the
// given data.
func (s *MultiStore) Get(
	index string,
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	ss := s.correspondingStore(index)
	if ss == nil {
		return nil
	}

	return ss.Get(index, start, end, envelopeTypes, limit, descending)
}

// Meta returns the MetaInfo aggregated across every store.
func (s *MultiStore) Meta() map[string]logcache_v1.MetaInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[string]logcache_v1.MetaInfo)
	for _, ss := range s.stores {
		for k, v := range ss.Meta() {
			m[k] = v
		}
	}
	return m

}

// SetRanges will create a new SubStore (via the SubStoreCreator) for each new
// range. It will Close and delete the reference to any SubStore that no
// longer corresponds to a range. This does not delete data from a SubStore
// that maintains a range.
func (s *MultiStore) SetRanges(rs []*logcache_v1.Range) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Prune out stores that do not correspond to a range.
	var notFound []logcache_v1.Range
	for k := range s.stores {
		var found bool
		for _, r := range rs {
			if *r == k {
				found = true
				break
			}
		}

		if !found {
			notFound = append(notFound, k)
		}
	}

	for _, r := range notFound {
		s.stores[r].Close()
		delete(s.stores, r)
	}

	for _, r := range rs {
		if _, ok := s.stores[*r]; !ok {
			s.stores[*r] = s.subStoreCreator(*r)
		}
	}
}
