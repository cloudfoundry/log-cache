package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
)

type MetricsInitializer interface {
	NewCounter(name string) func(delta uint64)
	NewGauge(name string) func(value float64)
}

// MemoryConsultant is used to determine if the store should prune.
type MemoryConsultant interface {
	// Prune returns the number of envelopes to prune.
	GetQuantityToPrune(int64) int
}

// Store is an in-memory data store for envelopes. It will store envelopes up
// to a per-source threshold and evict oldest data first, as instructed by the
// Pruner. All functions are thread safe.
type Store struct {
	storageIndex sync.Map

	initializationMutex sync.Mutex

	// count is incremented/decremented atomically during Put. Pruning occurs
	// only when count surpasses minimumStoreSizeToPrune
	count                   int64
	oldestTimestamp         int64
	minimumStoreSizeToPrune int

	maxPerSource int

	metrics Metrics
	mc      MemoryConsultant

	truncationCompleted chan bool
}

type Metrics struct {
	incExpired     func(delta uint64)
	setCachePeriod func(value float64)
	incIngress     func(delta uint64)
	incEgress      func(delta uint64)
	setStoreSize   func(value float64)
}

func NewStore(maxPerSource, minimumStoreSizeToPrune int, mc MemoryConsultant, m MetricsInitializer) *Store {
	store := &Store{
		minimumStoreSizeToPrune: minimumStoreSizeToPrune,
		maxPerSource:            maxPerSource,
		oldestTimestamp:         int64(^uint64(0) >> 1),

		metrics: Metrics{
			incExpired:     m.NewCounter("Expired"),
			setCachePeriod: m.NewGauge("CachePeriod"),
			incIngress:     m.NewCounter("Ingress"),
			incEgress:      m.NewCounter("Egress"),
			setStoreSize:   m.NewGauge("StoreSize"),
		},

		mc:                  mc,
		truncationCompleted: make(chan bool),
	}

	go store.truncationLoop(500 * time.Millisecond)

	return store
}

func (store *Store) getOrInitializeStorage(sourceId string) (*storage, bool) {
	var newStorage bool

	store.initializationMutex.Lock()
	defer store.initializationMutex.Unlock()

	envelopeStorage, existingSourceId := store.storageIndex.Load(sourceId)

	if !existingSourceId {
		envelopeStorage = &storage{
			sourceId: sourceId,
			Tree:     avltree.NewWith(utils.Int64Comparator),
		}
		store.storageIndex.Store(sourceId, envelopeStorage.(*storage))
		newStorage = true
	}

	return envelopeStorage.(*storage), newStorage
}

func (storage *storage) insertOrSwap(store *Store, ew envelopeWrapper) {
	storage.Lock()
	defer storage.Unlock()

	// If we're at our maximum capacity, remove an envelope before inserting
	if storage.Size() >= store.maxPerSource {
		oldestTimestamp := storage.Left().Key.(int64)
		storage.Remove(oldestTimestamp)
		storage.meta.Expired++
		store.metrics.incExpired(1)
	} else {
		atomic.AddInt64(&store.count, 1)
		store.metrics.setStoreSize(float64(atomic.LoadInt64(&store.count)))
	}

	storage.Put(ew.e.Timestamp, ew)

	if ew.e.Timestamp > storage.meta.NewestTimestamp {
		storage.meta.NewestTimestamp = ew.e.Timestamp
	}

	oldestTimestamp := storage.Left().Key.(int64)
	storage.meta.OldestTimestamp = oldestTimestamp

	storeOldestTimestamp := atomic.LoadInt64(&store.oldestTimestamp)
	if oldestTimestamp < storeOldestTimestamp {
		atomic.StoreInt64(&store.oldestTimestamp, oldestTimestamp)
		cachePeriod := calculateCachePeriod(oldestTimestamp)
		store.metrics.setCachePeriod(float64(cachePeriod))
	}
}

func (store *Store) WaitForTruncationToComplete() bool {
	return <-store.truncationCompleted
}

func (store *Store) sendTruncationCompleted(status bool) {
	select {
	case store.truncationCompleted <- status:
		// fmt.Println("Truncation ended with status", status)
	default:
		// Don't block if the channel has no receiver
	}
}

func (store *Store) truncationLoop(runInterval time.Duration) {

	t := time.NewTimer(runInterval)

	for {
		// Wait for our timer to go off
		<-t.C

		// startTime := time.Now()
		store.truncate()
		t.Reset(runInterval)
	}
}

func (store *Store) Put(envelope *loggregator_v2.Envelope, sourceId string) {
	store.metrics.incIngress(1)

	envelopeStorage, _ := store.getOrInitializeStorage(sourceId)

	ew := envelopeWrapper{e: envelope, index: sourceId}
	envelopeStorage.insertOrSwap(store, ew)
}

// truncate removes the n oldest envelopes across all trees
func (store *Store) truncate() {
	storeCount := atomic.LoadInt64(&store.count)
	minimumStoreSizeToPrune := int64(store.minimumStoreSizeToPrune)

	if storeCount < minimumStoreSizeToPrune {
		store.sendTruncationCompleted(false)
		return
	}

	numberToPrune := store.mc.GetQuantityToPrune(storeCount)

	if numberToPrune == 0 {
		store.sendTruncationCompleted(false)
		return
	}

	expirationsIndex := newIndexTree()

	// TODO - removing the single oldest envelope from each tree is vulnerable
	// to noisy neighbor - may need to tweak this algorithm if we see crowding
	store.storageIndex.Range(func(sourceId interface{}, tree interface{}) bool {
		tree.(*storage).RLock()
		oldestTimestamp := tree.(*storage).Left().Key.(int64)
		expirationsIndex.PutTree(oldestTimestamp, tree.(*storage))
		tree.(*storage).RUnlock()

		return true
	})

	// TODO - this is the limitation most likely to keep us from pruning fast
	// enough - may need to tweak
	if numberToPrune > expirationsIndex.TotalSize() {
		numberToPrune = expirationsIndex.TotalSize()
	}

	for i := 0; i < numberToPrune; i++ {
		_, oldestTree := expirationsIndex.RemoveLeftTree()
		store.removeOldestEnvelope(oldestTree)
	}

	store.metrics.setStoreSize(float64(atomic.LoadInt64(&store.count)))
	store.sendTruncationCompleted(true)
}

func (store *Store) removeOldestEnvelope(treeToPrune *storage) {
	treeToPrune.Lock()
	defer treeToPrune.Unlock()

	if treeToPrune.Size() == 0 {
		return
	}

	atomic.AddInt64(&store.count, -1)
	store.metrics.incExpired(1)

	oldestEnvelope := treeToPrune.Left()
	treeIndex := oldestEnvelope.Value.(envelopeWrapper).index

	treeToPrune.Remove(oldestEnvelope.Key.(int64))

	if treeToPrune.Size() == 0 {
		store.storageIndex.Delete(treeIndex)
		return
	}

	newOldestEnvelope := treeToPrune.Left()
	oldestAfterRemoval := newOldestEnvelope.Key.(int64)

	treeToPrune.meta.Expired++
	treeToPrune.meta.OldestTimestamp = oldestAfterRemoval
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
	tree, ok := store.storageIndex.Load(index)
	if !ok {
		return nil
	}

	tree.(*storage).RLock()
	defer tree.(*storage).RUnlock()

	traverser := store.treeAscTraverse
	if descending {
		traverser = store.treeDescTraverse
	}

	var res []*loggregator_v2.Envelope
	traverser(tree.(*storage).Root, start.UnixNano(), end.UnixNano(), func(e *loggregator_v2.Envelope, idx string) bool {
		if idx == index && store.validEnvelopeType(e, envelopeTypes) {
			res = append(res, e)
		}

		// Return true to stop traversing
		return len(res) >= limit
	})

	store.metrics.incEgress(uint64(len(res)))
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

	store.storageIndex.Range(func(sourceId interface{}, tree interface{}) bool {
		tree.(*storage).RLock()
		metaReport[sourceId.(string)] = tree.(*storage).meta
		tree.(*storage).RUnlock()

		return true
	})

	// Range over our local copy of metaReport
	// TODO - shouldn't we just maintain Count on metaReport..?!
	for sourceId, meta := range metaReport {
		tree, _ := store.storageIndex.Load(sourceId)

		tree.(*storage).RLock()
		meta.Count = int64(tree.(*storage).Size())
		tree.(*storage).RUnlock()
		metaReport[sourceId] = meta
	}
	return metaReport
}

type storage struct {
	sourceId string
	meta     logcache_v1.MetaInfo

	*avltree.Tree
	sync.RWMutex
}

type index struct {
	*avltree.Tree
	sync.RWMutex
}

func newIndexTree() *index {
	return &index{
		Tree: avltree.NewWith(utils.Int64Comparator),
	}
}

func calculateCachePeriod(oldestTimestamp int64) int64 {
	return (time.Now().UnixNano() - oldestTimestamp) / int64(time.Millisecond)
}

func (index *index) TotalSize() int {
	index.RLock()
	defer index.RUnlock()

	totalSize := 0
	for _, value := range index.Values() {
		totalSize += len(value.([]*storage))
	}

	return totalSize
}

func (index *index) EnsureOldestTimestamp(existingTimestamp, candidateTimestamp int64, envelopeStorage *storage) int64 {
	index.Lock()
	defer index.Unlock()

	if existingTimestamp > candidateTimestamp {
		index.RemoveTree(existingTimestamp, envelopeStorage)
		index.PutTree(candidateTimestamp, envelopeStorage)

		return calculateCachePeriod(candidateTimestamp)
	}

	return calculateCachePeriod(existingTimestamp)
}

// NOTE: PutTree should always have a Lock()
func (index *index) PutTree(key int64, treeToIndex *storage) {
	var values []*storage
	if existing, found := index.Get(key); found {
		values = existing.([]*storage)
	}

	index.Put(key, append(values, treeToIndex))
}

// NOTE: RemoveTree should always have a Lock()
func (index *index) RemoveTree(timestamp int64, treeToIndex *storage) bool {
	var found bool
	var values []*storage

	if existing, found := index.Get(timestamp); found {
		values = existing.([]*storage)
	}

	if !found {
		fmt.Println("Failed to find timestamp", timestamp)
	}

	for i, v := range values {
		if v == treeToIndex {
			values = append(values[:i], values[i+1:]...)
			break
		}
	}

	if len(values) == 0 {
		index.Remove(timestamp)
		return found
	}

	index.Put(timestamp, values)

	return found
}

// TODO: maybe perhaps consider wrtiting a test for this at some point. maybe.
// NOTE: RemoveLeftTree should always have a Lock()
func (index *index) RemoveLeftTree() (int64, *storage) {
	l := index.Left()
	timestamp, values := l.Key.(int64), l.Value.([]*storage)

	lt := values[0]
	values = values[1:]

	if len(values) == 0 {
		index.Remove(timestamp)
		return timestamp, lt
	}

	index.Put(timestamp, values)
	return timestamp, lt
}

func (index *index) LeftTree() (int64, *storage) {
	index.RLock()
	defer index.RUnlock()

	if index.Size() == 0 {
		return 0, nil
	}

	l := index.Left()
	return l.Key.(int64), l.Value.([]*storage)[0]
}

type envelopeWrapper struct {
	e     *loggregator_v2.Envelope
	index string
}
