package store

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"github.com/emirpasic/gods/trees/avltree"
	"github.com/emirpasic/gods/utils"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
	_ "github.com/influxdata/influxdb/tsdb/engine"
	_ "github.com/influxdata/influxdb/tsdb/index"
	"github.com/influxdata/influxql"
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
	tsdbStore    *tsdb.Store

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

func NewStore(storagePath string, maxPerSource, minimumStoreSizeToPrune int, mc MemoryConsultant, m MetricsInitializer) *Store {
	baseDir := filepath.Join(storagePath, "influxdb")

	dataDir := filepath.Join(baseDir, "data")
	walDir := filepath.Join(baseDir, "wal")

	// shardDir := filepath.Join(baseDir, "shard")
	// seriesFileDir := filepath.Join(baseDir, "series-file")

	// seriesFile := tsdb.NewSeriesFile(seriesFileDir)
	// seriesFile.Logger = logger.New(os.Stdout)
	// if err := seriesFile.Open(); err != nil {
	// 	panic(err)
	// }

	// opts := tsdb.NewEngineOptions()

	tsdbStore := tsdb.NewStore(dataDir)
	tsdbStore.EngineOptions.WALEnabled = true
	tsdbStore.EngineOptions.Config.WALDir = walDir

	if err := tsdbStore.Open(); err != nil {
		panic(err)
	}

	if err := tsdbStore.CreateShard("db", "rp", 0, true); err != nil {
		panic(err)
	}

	// opts.InmemIndex = inmem.NewIndex(baseDir, seriesFile)
	// shard := tsdb.NewShard(0, shardDir, walDir, seriesFile, opts)

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

		tsdbStore: tsdbStore,
	}

	store.mc.SetMemoryReporter(store.metrics.setMemoryUtilization)

	go store.truncationLoop(500 * time.Millisecond)

	return store
}

func (store *Store) Close() {
	store.tsdbStore.Close()
}

func (store *Store) QueryTSDB(index string, start, end int64, limit int) ([]*query.FloatPoint, error) {
	shard := store.tsdbStore.Shard(0)
	m := &influxql.Measurement{
		Database:        "db",
		RetentionPolicy: "rp",
		Name:            "envelopes",
	}

	opts := query.IteratorOptions{
		Expr:      influxql.MustParseExpr(`value`),
		Aux:       []influxql.VarRef{{Val: "source_id"}},
		StartTime: start,
		EndTime:   end - 1,
		Condition: &influxql.BinaryExpr{
			LHS: &influxql.VarRef{Val: "source_id"},
			RHS: &influxql.StringLiteral{Val: index},
			Op:  influxql.EQ,
		},
		Ascending: true,
		Limit:     100,
	}

	iterator, err := shard.CreateIterator(context.Background(), m, opts)

	if err != nil {
		return nil, err
	}

	var points []*query.FloatPoint
	fitr := iterator.(query.FloatIterator)
	for {
		point, _ := fitr.Next()
		if point == nil {
			break
		}
		fmt.Printf("%#v\b", point)

		// if point.Aux[0] == index {
		points = append(points, point)
		// }
	}

	return points, nil
}

// Iterators is a test wrapper for iterators.
type Iterators []query.Iterator

// Next returns the next value from each iterator.
// Returns nil if any iterator returns a nil.
func (itrs Iterators) Next() ([]query.Point, error) {
	a := make([]query.Point, len(itrs))
	for i, itr := range itrs {
		switch itr := itr.(type) {
		case query.FloatIterator:
			fp, err := itr.Next()
			if fp == nil || err != nil {
				return nil, err
			}
			a[i] = fp
		case query.IntegerIterator:
			ip, err := itr.Next()
			if ip == nil || err != nil {
				return nil, err
			}
			a[i] = ip
		case query.UnsignedIterator:
			up, err := itr.Next()
			if up == nil || err != nil {
				return nil, err
			}
			a[i] = up
		case query.StringIterator:
			sp, err := itr.Next()
			if sp == nil || err != nil {
				return nil, err
			}
			a[i] = sp
		case query.BooleanIterator:
			bp, err := itr.Next()
			if bp == nil || err != nil {
				return nil, err
			}
			a[i] = bp
		default:
			panic(fmt.Sprintf("iterator type not supported: %T", itr))
		}
	}
	return a, nil
}

// ReadAll reads all points from all iterators.
func (itrs Iterators) ReadAll() ([][]query.Point, error) {
	var a [][]query.Point

	// Read from every iterator until a nil is encountered.
	for {
		points, err := itrs.Next()
		if err != nil {
			return nil, err
		} else if points == nil {
			break
		}
		a = append(a, query.Points(points).Clone())
	}

	// Close all iterators.
	query.Iterators(itrs).Close()

	return a, nil
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

	points, err := convertEnvelopeToPoints(envelope)
	if err == nil {
		err = store.tsdbStore.WriteToShard(0, points)
		if err != nil {
			panic(err)
		}
	}

	envelopeStorage, _ := store.getOrInitializeStorage(sourceId)
	envelopeStorage.insertOrSwap(store, envelope)
}

func convertEnvelopeToPoints(envelope *loggregator_v2.Envelope) ([]models.Point, error) {
	switch envelope.Message.(type) {
	case *loggregator_v2.Envelope_Gauge:
		var points []models.Point

		gauge := envelope.GetGauge()

		for name, metric := range gauge.GetMetrics() {
			fields := make(models.Fields)
			for tagName, tagValue := range envelope.GetTags() {
				fields[tagName] = tagValue
			}
			fields["name"] = name
			fields["unit"] = metric.GetUnit()
			fields["value"] = metric.GetValue()

			tags := models.Tags{{
				Key:   []byte("source_id"),
				Value: []byte(envelope.GetSourceId()),
			}}

			point, err := models.NewPoint("envelopes", tags, fields, time.Unix(0, envelope.GetTimestamp()))
			if err != nil {
				return nil, err
			}
			points = append(points, point)
		}

		return points, nil
	case *loggregator_v2.Envelope_Counter:
		counter := envelope.GetCounter()

		fields := make(models.Fields)
		for tagName, tagValue := range envelope.GetTags() {
			fields[tagName] = tagValue
		}
		fields["name"] = counter.GetName()
		fields["value"] = counter.GetTotal()
		point, err := models.NewPoint("envelopes", nil, fields, time.Unix(0, envelope.GetTimestamp()))
		if err != nil {
			return nil, err
		}

		return []models.Point{point}, nil
	case *loggregator_v2.Envelope_Timer:
		timer := envelope.GetTimer()

		fields := make(models.Fields)
		for tagName, tagValue := range envelope.GetTags() {
			fields[tagName] = tagValue
		}
		fields["name"] = timer.GetName()
		fields["value"] = timer.GetStop() - timer.GetStart()
		point, err := models.NewPoint("envelopes", nil, fields, time.Unix(0, envelope.GetTimestamp()))
		if err != nil {
			return nil, err
		}

		return []models.Point{point}, nil
	default:
		return nil, errors.New("unhandled message type")
	}
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
	var res []*loggregator_v2.Envelope
	points, err := store.QueryTSDB(index, start.UnixNano(), end.UnixNano(), limit)
	if err != nil {
		fmt.Println("Error fetching points from tsdb:", err)
		return nil
	}

	envelopes := convertPointsToEnvelopes(points)

	store.metrics.incEgress(uint64(len(res)))
	return envelopes
}

func convertPointsToEnvelopes(points []*query.FloatPoint) []*loggregator_v2.Envelope {
	var envelopes []*loggregator_v2.Envelope
	for _, point := range points {
		metric := make(map[string]*loggregator_v2.GaugeValue)
		metric[point.Name] = &loggregator_v2.GaugeValue{Unit: "ms", Value: 1}

		fmt.Printf("%#v", point.Tags)
		envelope := &loggregator_v2.Envelope{
			Timestamp: point.Time,
			SourceId:  point.Aux[0].(string),
			Message: &loggregator_v2.Envelope_Gauge{
				Gauge: &loggregator_v2.Gauge{
					Metrics: metric,
				},
			},
		}

		envelopes = append(envelopes, envelope)
	}

	return envelopes
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

	t := n.Key.(int64)
	if t >= start {
		if s.treeAscTraverse(n.Children[0], start, end, f) {
			return true
		}

		e := n.Value.(*loggregator_v2.Envelope)

		if t >= end || f(e) {
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

	t := n.Key.(int64)
	if t < end {
		if s.treeDescTraverse(n.Children[1], start, end, f) {
			return true
		}

		e := n.Value.(*loggregator_v2.Envelope)

		if t < start || f(e) {
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
