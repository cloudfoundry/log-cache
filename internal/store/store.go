package store

import (
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
)

// Metrics is the client used for initializing counter and gauge metrics.
type Metrics interface {
	//NewCounter initializes a new counter metric.
	NewCounter(name string) func(delta uint64)

	//NewGauge initializes a new gauge metric.
	NewGauge(name string) func(value float64)
}

// Pruner is used to determine if the store should prune.
type Pruner interface {
	// Prune returns true if the store should prune entries. The returned
	// value is the length of entries to prune.
	Prune() int
}

type LockableTree struct {
	*avltree.Tree
	meta logcache_v1.MetaInfo
	sync.RWMutex
}

// Store is an in memory data store for envelopes. It will keep a bounded
// number and drop older data once that threshold is exceeded. All functions
// are thread safe. The Pruner is used to know when entries should be
// pruned.
type Store struct {
	indexes map[string]*LockableTree

	// expirationIndex stores each tree's oldest value for pruning. As data is
	// added and needs to be pruned, it is done so from here.
	expirationIndex *treeIndex

	// count is incremented atomically each Put. It is used to determine when to prune. When
	// an envelope is pruned, it is decremented atomically.
	count                   int64
	maxPerSource            int
	minimumStoreSizeToPrune int

	// metrics
	incExpired     func(delta uint64)
	setCachePeriod func(value float64)
	incIngress     func(delta uint64)
	incEgress      func(delta uint64)
	setStoreSize   func(value float64)

	p Pruner
	sync.RWMutex
}

func NewStore(maxPerSource, minimumStoreSizeToPrune int, p Pruner, m Metrics) *Store {
	return &Store{
		maxPerSource:            maxPerSource,
		p:                       p,
		indexes:                 make(map[string]*LockableTree),
		expirationIndex:         newTreeIndex(),
		minimumStoreSizeToPrune: minimumStoreSizeToPrune,

		incExpired:     m.NewCounter("Expired"),
		setCachePeriod: m.NewGauge("CachePeriod"),
		incIngress:     m.NewCounter("Ingress"),
		incEgress:      m.NewCounter("Egress"),
		setStoreSize:   m.NewGauge("StoreSize"),
	}
}

func (store *Store) Put(envelope *loggregator_v2.Envelope, index string) {
	store.incIngress(1)

	tree, ok := store.retrieveTree(index)
	if !ok {
		tree = store.insertTree(index)
		store.expirationIndex.Put(envelope.Timestamp, tree)
	}

	tree.Lock()

	var oldestBeforeInsertion int64
	preexistingTree := tree.Size() > 0
	if preexistingTree {
		oldestBeforeInsertion = tree.Left().Key.(int64)
	}

	treeSizeBeforeInsertion := tree.Size()
	if treeSizeBeforeInsertion >= store.maxPerSource {
		tree.Remove(oldestBeforeInsertion)

		store.incExpired(1)
		tree.meta.Expired++
	}

	tree.Put(envelope.Timestamp, envelopeWrapper{e: envelope, index: index})

	// Only increment if we didn't overwrite.
	atomic.AddInt64(&store.count, int64(tree.Size()-treeSizeBeforeInsertion))

	oldestAfterInsertion := tree.Left().Key.(int64)
	if oldestBeforeInsertion != oldestAfterInsertion && preexistingTree {
		store.expirationIndex.Remove(oldestBeforeInsertion, tree)
		store.expirationIndex.Put(oldestAfterInsertion, tree)
	}

	tree.Unlock()

	store.truncate()
	store.setStoreSize(float64(atomic.LoadInt64(&store.count)))

	tree.Lock()
	if envelope.GetTimestamp() > tree.meta.NewestTimestamp {
		tree.meta.NewestTimestamp = envelope.GetTimestamp()
	}

	// TODO - this logic maybe belongs in truncate() - Put() seems like an odd place
	// 		to consider that the tree has been deleted during truncate()
	if tree.Size() > 0 {
		tree.meta.OldestTimestamp = tree.Left().Key.(int64)
	}
	tree.Unlock()

	// TODO - probably a helper
	// Set Cache Period
	oldestValue, _ := store.expirationIndex.Left()
	cachePeriod := (time.Now().UnixNano() - oldestValue) / int64(time.Millisecond)
	store.setCachePeriod(float64(cachePeriod))
}

func (store *Store) insertTree(key string) *LockableTree {
	store.Lock()
	defer store.Unlock()

	tree := &LockableTree{Tree: avltree.NewWith(utils.Int64Comparator)}
	store.indexes[key] = tree
	return tree
}

func (store *Store) retrieveTree(key string) (*LockableTree, bool) {
	store.RLock()
	defer store.RUnlock()

	tree, ok := store.indexes[key]
	return tree, ok
}

func (store *Store) removeTree(key string) {
	store.Lock()
	defer store.Unlock()

	delete(store.indexes, key)
}

// truncate removes the n oldest envelopes across all trees
func (store *Store) truncate() {
	numberToPrune := int64(store.p.Prune())

	// Prevent the whole cache from being pruned
	if atomic.LoadInt64(&store.count)-numberToPrune < int64(store.minimumStoreSizeToPrune) {
		numberToPrune = atomic.LoadInt64(&store.count) - int64(store.minimumStoreSizeToPrune)
	}

	for i := int64(0); i < numberToPrune; i++ {
		store.removeOldestEnvelope()
	}
}

func (store *Store) removeOldestEnvelope() {
	atomic.AddInt64(&store.count, -1)
	store.incExpired(1)

	// dereference the node so that after we remove it, the pointer does not
	// get updated underneath us.
	oldestTimestamp, treeToPrune := store.expirationIndex.Left()

	treeToPrune.Lock()
	defer treeToPrune.Unlock()

	store.expirationIndex.Remove(oldestTimestamp, treeToPrune)

	oldestEnvelope := treeToPrune.Left()
	treeIndex := oldestEnvelope.Value.(envelopeWrapper).index

	treeToPrune.Remove(oldestTimestamp)

	if treeToPrune.Size() == 0 {
		store.removeTree(treeIndex)
		return
	}

	// TODO - can we extract a function for 'update meta and expirationIndex?'
	oldestAfterRemoval := treeToPrune.Left().Key.(int64)

	treeToPrune.meta.Expired++
	treeToPrune.meta.OldestTimestamp = oldestAfterRemoval

	store.expirationIndex.Put(oldestAfterRemoval, treeToPrune)
}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (store *Store) Get(
	index string,
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	tree, ok := store.retrieveTree(index)
	if !ok {
		return nil
	}

	tree.RLock()
	defer tree.RUnlock()

	traverser := store.treeAscTraverse
	if descending {
		traverser = store.treeDescTraverse
	}

	var res []*loggregator_v2.Envelope
	traverser(tree.Root, start.UnixNano(), end.UnixNano(), func(e *loggregator_v2.Envelope, idx string) bool {
		if idx == index && store.validEnvelopeType(e, envelopeTypes) {
			res = append(res, e)
		}

		// Return true to stop traversing
		return len(res) >= limit
	})

	store.incEgress(uint64(len(res)))
	return res
}

func (s *Store) validEnvelopeType(e *loggregator_v2.Envelope, types []logcache_v1.EnvelopeType) bool {
	if types == nil {
		return true
	}
	for _, t := range types {
		if s.checkEnvelopeType(e, t) {
			return true
		}
	}
	return false
}

func (s *Store) treeAscTraverse(
	n *avltree.Node,
	start int64,
	end int64,
	f func(e *loggregator_v2.Envelope, index string) bool,
) bool {
	if n == nil {
		return false
	}

	t := n.Key.(int64)
	if t >= start {
		if s.treeAscTraverse(n.Children[0], start, end, f) {
			return true
		}

		w := n.Value.(envelopeWrapper)

		if t >= end || f(w.e, w.index) {
			return true
		}
	}

	return s.treeAscTraverse(n.Children[1], start, end, f)
}

func (s *Store) treeDescTraverse(
	n *avltree.Node,
	start int64,
	end int64,
	f func(e *loggregator_v2.Envelope, index string) bool,
) bool {
	if n == nil {
		return false
	}

	t := n.Key.(int64)
	if t < end {
		if s.treeDescTraverse(n.Children[1], start, end, f) {
			return true
		}

		w := n.Value.(envelopeWrapper)

		if t < start || f(w.e, w.index) {
			return true
		}
	}

	return s.treeDescTraverse(n.Children[0], start, end, f)
}

func (s *Store) checkEnvelopeType(e *loggregator_v2.Envelope, t logcache_v1.EnvelopeType) bool {
	if t == logcache_v1.EnvelopeType_ANY {
		return true
	}

	switch t {
	case logcache_v1.EnvelopeType_LOG:
		return e.GetLog() != nil
	case logcache_v1.EnvelopeType_COUNTER:
		return e.GetCounter() != nil
	case logcache_v1.EnvelopeType_GAUGE:
		return e.GetGauge() != nil
	case logcache_v1.EnvelopeType_TIMER:
		return e.GetTimer() != nil
	case logcache_v1.EnvelopeType_EVENT:
		return e.GetEvent() != nil
	default:
		// This should never happen. This implies the store is being used
		// poorly.
		panic("unknown type")
	}
}

// Meta returns each source ID tracked in the store.
func (store *Store) Meta() map[string]logcache_v1.MetaInfo {
	metaReport := make(map[string]logcache_v1.MetaInfo)

	// Copy the maps so that we don't leak the lock protected maps beyond the
	// locks.
	store.RLock()
	for index, tree := range store.indexes {
		tree.RLock()
		metaReport[index] = tree.meta
		tree.RUnlock()
	}
	store.RUnlock()

	// Range over our local copy of metaReport
	// TODO - shouldn't we just maintain Count on metaReport..?!
	for index, meta := range metaReport {
		tree, _ := store.retrieveTree(index)

		tree.RLock()
		meta.Count = int64(tree.Size())
		tree.RUnlock()
		metaReport[index] = meta
	}
	return metaReport
}

// treeIndex stores the trees and sorts them with respect to time. It
// prevents overwrites for the same key.
type treeIndex struct {
	t *avltree.Tree
	sync.RWMutex
}

func newTreeIndex() *treeIndex {
	return &treeIndex{
		t: avltree.NewWith(utils.Int64Comparator),
	}
}

func (s *treeIndex) Put(key int64, treeToIndex *LockableTree) {
	s.Lock()
	defer s.Unlock()
	indexTree := s.t

	var values []*LockableTree
	if existing, found := indexTree.Get(key); found {
		values = existing.([]*LockableTree)
	}

	indexTree.Put(key, append(values, treeToIndex))
}

func (s *treeIndex) Remove(key int64, treeToIndex *LockableTree) {
	s.Lock()
	defer s.Unlock()
	indexTree := s.t

	var values []*LockableTree
	if existing, found := indexTree.Get(key); found {
		values = existing.([]*LockableTree)
	}

	for i, v := range values {
		if v == treeToIndex {
			values = append(values[:i], values[i+1:]...)
			break
		}
	}

	if len(values) == 0 {
		indexTree.Remove(key)
		return
	}

	indexTree.Put(key, values)
}

func (s *treeIndex) Left() (int64, *LockableTree) {
	s.RLock()
	defer s.RUnlock()
	indexTree := s.t

	l := indexTree.Left()
	return l.Key.(int64), l.Value.([]*LockableTree)[0]
}

type envelopeWrapper struct {
	e     *loggregator_v2.Envelope
	index string
}
