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
	mu           sync.RWMutex
	size         int
	maxPerSource int

	sourceIDs map[string]*avltree.Tree

	// oldestValueTree stores each tree's oldest value for pruning. As data is
	// added and needs to be pruned, it is done so from here.
	oldestValueTree *treeStorage

	// count is incremented each Put. It is used to determine when to prune. When
	// an envelope is pruned, it is decremented.
	count int
}

// NewStore creates a new store.
func NewStore(size, maxPerSource int) *Store {
	return &Store{
		size:            size,
		maxPerSource:    maxPerSource,
		sourceIDs:       make(map[string]*avltree.Tree),
		oldestValueTree: newTreeStorage(),
	}
}

// Put adds a batch of envelopes into the store.
func (s *Store) Put(envs []*loggregator_v2.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range envs {

		t, ok := s.sourceIDs[e.SourceId]
		if !ok {
			t = avltree.NewWith(utils.Int64Comparator)
			s.sourceIDs[e.SourceId] = t

			// Store the tree for pruning purposes.
			s.oldestValueTree.Put(e.Timestamp, t)
		}

		var (
			oldest    int64
			hasOldest bool
		)
		if t.Size() > 0 {
			oldest = t.Left().Key.(int64)
			hasOldest = true
		}

		preSize := t.Size()

		if preSize >= s.maxPerSource {
			// This sourceID has reached/exceeded its allowed quota. Truncate the
			// oldest before putting a new envelope in.
			t.Remove(oldest)
		}

		t.Put(e.Timestamp, e)

		// Only increment if we didn't overwrite.
		s.count += t.Size() - preSize

		newOldest := t.Left().Key.(int64)
		if oldest != newOldest && hasOldest {
			s.oldestValueTree.Remove(oldest, t)
			s.oldestValueTree.Put(newOldest, t)
		}

		s.truncate()
	}
}

// truncate removes the oldest envelope from the entire cache. It considers
// each source-id.
func (s *Store) truncate() {
	if s.count <= s.size {
		return
	}

	s.count--

	// dereference the node so that after we remove it, the pointer does not
	// get updated underneath us.
	key, oldTree := s.oldestValueTree.Left()
	s.oldestValueTree.Remove(key, oldTree)

	// Truncate the oldest envelope.
	left := oldTree.Left()

	sourceID := left.Value.(*loggregator_v2.Envelope).SourceId

	oldTree.Remove(key)

	if oldTree.Size() == 0 {
		// Remove the sourceID completely.
		delete(s.sourceIDs, sourceID)
		return
	}

	// Add tree back to oldestValueTree for future pruning.
	s.oldestValueTree.Put(oldTree.Left().Key.(int64), oldTree)
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

	t, ok := s.sourceIDs[sourceID]
	if !ok {
		return nil
	}

	endNs := end.UnixNano()
	startNode, _ := t.Ceiling(start.UnixNano())

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

// treeStorage stores the trees and sorts them with respect to time. It
// prevents overwrites for the same key.
type treeStorage struct {
	t *avltree.Tree
}

func newTreeStorage() *treeStorage {
	return &treeStorage{
		t: avltree.NewWith(utils.Int64Comparator),
	}
}

func (s *treeStorage) Put(key int64, t *avltree.Tree) {
	var values []*avltree.Tree
	if existing, found := s.t.Get(key); found {
		values = existing.([]*avltree.Tree)
	}

	s.t.Put(key, append(values, t))
}

func (s *treeStorage) Remove(key int64, t *avltree.Tree) {
	var values []*avltree.Tree
	if existing, found := s.t.Get(key); found {
		values = existing.([]*avltree.Tree)
	}

	for i, v := range values {
		if v == t {
			values = append(values[:i], values[i+1:]...)
			break
		}
	}

	if len(values) == 0 {
		s.t.Remove(key)
		return
	}

	s.t.Put(key, values)
}

func (s *treeStorage) Left() (int64, *avltree.Tree) {
	l := s.t.Left()
	return l.Key.(int64), l.Value.([]*avltree.Tree)[0]
}
