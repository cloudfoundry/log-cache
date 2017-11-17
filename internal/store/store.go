package store

import (
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
)

// Store is an in memory data store for envelopes. It will keep a bounded
// number and drop older data once that threshold is exceeded. All functions
// are thread safe.
type Store struct {
	mu       sync.RWMutex
	timeTree *avltree.Tree
	size     int
}

// NewStore creates a new store.
func NewStore(size int) *Store {
	return &Store{
		timeTree: avltree.NewWith(utils.Int64Comparator),
		size:     size,
	}
}

// Put adds a batch of envelopes into the store.
func (s *Store) Put(envs []*loggregator_v2.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range envs {
		s.timeTree.Put(e.Timestamp, e)

		if s.timeTree.Size() > s.size {
			// Truncate the oldest envelope
			s.timeTree.Remove(s.timeTree.Left().Key)
		}
	}
}

// EnvelopeType is used to filter envelopes based on type.
type EnvelopeType interface{}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (s *Store) Get(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType EnvelopeType,
) []*loggregator_v2.Envelope {
	s.mu.RLock()
	defer s.mu.RUnlock()

	endNs := end.UnixNano()
	startNode, _ := s.timeTree.Ceiling(start.UnixNano())

	var res []*loggregator_v2.Envelope
	s.treeTraverse(startNode, true, false, func(e *loggregator_v2.Envelope) bool {
		// Time validation is [start..end)
		if e.SourceId == sourceID &&
			e.Timestamp >= start.UnixNano() &&
			e.Timestamp < end.UnixNano() &&
			s.checkEnvelopeType(e, envelopeType) {
			res = append(res, e)
		}

		return e.Timestamp <= endNs
	})

	return res
}

// treeTraverse walks the tree from left to right starting from the given
// node. It stops walking when the given function returns false. When invoking
// it for the first time, skipLeft should be set to false to ensure the
// starting value is the lowest value.
func (s *Store) treeTraverse(
	n *avltree.Node,
	skipLeft bool,
	skipParent bool,
	f func(*loggregator_v2.Envelope) bool,
) {
	if n == nil {
		return
	}

	if !skipLeft {
		s.treeTraverse(n.Children[0], false, true, f)
	}

	if !f(n.Value.(*loggregator_v2.Envelope)) {
		return
	}

	s.treeTraverse(n.Children[1], false, true, f)

	if !skipParent {
		s.treeTraverse(n.Parent, true, false, f)
	}
}

func (s *Store) checkEnvelopeType(e *loggregator_v2.Envelope, t EnvelopeType) bool {
	if t == nil {
		return true
	}

	switch t.(type) {
	case *loggregator_v2.Log:
		return e.GetLog() != nil
	case *loggregator_v2.Counter:
		return e.GetCounter() != nil
	case *loggregator_v2.Gauge:
		return e.GetGauge() != nil
	case *loggregator_v2.Timer:
		return e.GetTimer() != nil
	case *loggregator_v2.Event:
		return e.GetEvent() != nil
	default:
		// This should never happen. This implies the store is being used
		// poorly.
		panic("unknown type")
	}
}
