package store

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/rpc/logcache_v1"
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
	// setMemoryReporter accepts a reporting function for Memory Utilization
	SetMemoryReporter(func(float64))
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

	maxPerSource      int
	maxTimestampFudge int64

	metrics Metrics
	mc      MemoryConsultant

	truncationCompleted chan bool
}

type Metrics struct {
	incExpired            func(delta uint64)
	setCachePeriod        func(value float64)
	incIngress            func(delta uint64)
	incEgress             func(delta uint64)
	setStoreSize          func(value float64)
	setTruncationDuration func(value float64)
	setMemoryUtilization  func(value float64)
}

func NewStore(maxPerSource, minimumStoreSizeToPrune int, mc MemoryConsultant, m MetricsInitializer) *Store {
	store := &Store{
		minimumStoreSizeToPrune: minimumStoreSizeToPrune,
		maxPerSource:            maxPerSource,
		maxTimestampFudge:       4000,
		oldestTimestamp:         int64(^uint64(0) >> 1),

		metrics: Metrics{
			incExpired:            m.NewCounter("Expired"),
			setCachePeriod:        m.NewGauge("CachePeriod"),
			incIngress:            m.NewCounter("Ingress"),
			incEgress:             m.NewCounter("Egress"),
			setStoreSize:          m.NewGauge("StoreSize"),
			setTruncationDuration: m.NewGauge("TruncationDuration"),
			setMemoryUtilization:  m.NewGauge("MemoryUtilization"),
		},

		mc:                  mc,
		truncationCompleted: make(chan bool),
	}

	store.mc.SetMemoryReporter(store.metrics.setMemoryUtilization)

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

func (storage *storage) insertOrSwap(store *Store, e *loggregator_v2.Envelope) {
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

	var timestampFudge int64
	for timestampFudge = 0; timestampFudge < store.maxTimestampFudge; timestampFudge++ {
		_, exists := storage.Get(e.Timestamp + timestampFudge)

		if !exists {
			break
		}
	}

	storage.Put(e.Timestamp+timestampFudge, e)

	if e.Timestamp > storage.meta.NewestTimestamp {
		storage.meta.NewestTimestamp = e.Timestamp
	}

	oldestTimestamp := storage.Left().Key.(int64)
	storage.meta.OldestTimestamp = oldestTimestamp
	storeOldestTimestamp := atomic.LoadInt64(&store.oldestTimestamp)

	if oldestTimestamp < storeOldestTimestamp {
		atomic.StoreInt64(&store.oldestTimestamp, oldestTimestamp)
		storeOldestTimestamp = oldestTimestamp
	}

	cachePeriod := calculateCachePeriod(storeOldestTimestamp)
	store.metrics.setCachePeriod(float64(cachePeriod))
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

		startTime := time.Now()
		store.truncate()
		t.Reset(runInterval)
		store.metrics.setTruncationDuration(float64(time.Since(startTime) / time.Millisecond))
	}
}

func (store *Store) Put(envelope *loggregator_v2.Envelope, sourceId string) {
	store.metrics.incIngress(1)

	envelopeStorage, _ := store.getOrInitializeStorage(sourceId)
	envelopeStorage.insertOrSwap(store, envelope)
}

func (store *Store) BuildExpirationHeap() *ExpirationHeap {
	expirationHeap := &ExpirationHeap{}
	heap.Init(expirationHeap)

	store.storageIndex.Range(func(sourceId interface{}, tree interface{}) bool {
		tree.(*storage).RLock()
		oldestTimestamp := tree.(*storage).Left().Key.(int64)
		heap.Push(expirationHeap, storageExpiration{timestamp: oldestTimestamp, sourceId: sourceId.(string), tree: tree.(*storage)})
		tree.(*storage).RUnlock()

		return true
	})

	return expirationHeap
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

	expirationHeap := store.BuildExpirationHeap()

	// Remove envelopes one at a time, popping state from the expirationHeap
	for i := 0; i < numberToPrune; i++ {
		oldest := heap.Pop(expirationHeap)
		newOldestTimestamp, valid := store.removeOldestEnvelope(oldest.(storageExpiration).tree, oldest.(storageExpiration).sourceId)
		if valid {
			heap.Push(expirationHeap, storageExpiration{timestamp: newOldestTimestamp, sourceId: oldest.(storageExpiration).sourceId, tree: oldest.(storageExpiration).tree})
		}
	}

	// Grab the next oldest timestamp and use it to update the cache period
	oldest := expirationHeap.Pop()
	if oldest.(storageExpiration).tree != nil {
		atomic.StoreInt64(&store.oldestTimestamp, oldest.(storageExpiration).timestamp)
		cachePeriod := calculateCachePeriod(oldest.(storageExpiration).timestamp)
		store.metrics.setCachePeriod(float64(cachePeriod))
	}

	store.metrics.setStoreSize(float64(atomic.LoadInt64(&store.count)))
	store.sendTruncationCompleted(true)
}

func (store *Store) removeOldestEnvelope(treeToPrune *storage, sourceId string) (int64, bool) {
	treeToPrune.Lock()
	defer treeToPrune.Unlock()

	if treeToPrune.Size() == 0 {
		return 0, false
	}

	atomic.AddInt64(&store.count, -1)
	store.metrics.incExpired(1)

	oldestEnvelope := treeToPrune.Left()

	treeToPrune.Remove(oldestEnvelope.Key.(int64))

	if treeToPrune.Size() == 0 {
		store.storageIndex.Delete(sourceId)
		return 0, false
	}

	newOldestEnvelope := treeToPrune.Left()
	oldestTimestampAfterRemoval := newOldestEnvelope.Key.(int64)

	treeToPrune.meta.Expired++
	treeToPrune.meta.OldestTimestamp = oldestTimestampAfterRemoval

	return oldestTimestampAfterRemoval, true
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
	traverser(tree.(*storage).Root, start.UnixNano(), end.UnixNano(), func(e *loggregator_v2.Envelope) bool {
		if store.validEnvelopeType(e, envelopeTypes) {
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
	f func(e *loggregator_v2.Envelope) bool,
) bool {
	if n == nil {
		return false
	}

	e := n.Value.(*loggregator_v2.Envelope)
	t := e.GetTimestamp()

	if t >= start {
		if s.treeAscTraverse(n.Children[0], start, end, f) {
			return true
		}

		if (t >= end || f(e)) && (t == n.Key.(int64)) {
			return true
		}
	}

	return s.treeAscTraverse(n.Children[1], start, end, f)
}

func (s *Store) treeDescTraverse(
	n *avltree.Node,
	start int64,
	end int64,
	f func(e *loggregator_v2.Envelope) bool,
) bool {
	if n == nil {
		return false
	}

	e := n.Value.(*loggregator_v2.Envelope)
	t := e.GetTimestamp()

	if t < end {
		if s.treeDescTraverse(n.Children[1], start, end, f) {
			return true
		}

		if (t < start || f(e)) && (t == n.Key.(int64)) {
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

type ExpirationHeap []storageExpiration

type storageExpiration struct {
	timestamp int64
	sourceId  string
	tree      *storage
}

func (h ExpirationHeap) Len() int           { return len(h) }
func (h ExpirationHeap) Less(i, j int) bool { return h[i].timestamp < h[j].timestamp }
func (h ExpirationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *ExpirationHeap) Push(x interface{}) {
	*h = append(*h, x.(storageExpiration))
}

func (h *ExpirationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]

	return x
}

func calculateCachePeriod(oldestTimestamp int64) int64 {
	return (time.Now().UnixNano() - oldestTimestamp) / int64(time.Millisecond)
}
