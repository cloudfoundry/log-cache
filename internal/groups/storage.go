package groups

import (
	"context"
	"log"
	"time"

	gologcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	streamaggregator "code.cloudfoundry.org/go-stream-aggregator"
	"code.cloudfoundry.org/log-cache/internal/store"
)

// Storage stores data for groups. It prunes data out when the data is read.
// It replenishes when the Replenish method is invoked.
type Storage struct {
	log *log.Logger
	r   Reader

	ctx    context.Context
	cancel func()

	// key=name
	m map[string]aggregator

	streamAgg *streamaggregator.StreamAggregator
	store     *store.Store
}

// Reader reads envelopes from LogCache.
type Reader func(
	sourceID string,
	start time.Time,
	opts ...gologcache.ReadOption,
) ([]*loggregator_v2.Envelope, error)

// Metrics is the client used for initializing counter and gauge metrics.
type Metrics interface {
	//NewCounter initializes a new counter metric.
	NewCounter(name string) func(delta uint64)

	//NewGauge initializes a new gauge metric.
	NewGauge(name string) func(value float64)
}

// NewStorage returns new Storage.
func NewStorage(
	size int,
	r Reader,
	m Metrics,
	log *log.Logger,
) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Storage{
		log:       log,
		r:         r,
		streamAgg: streamaggregator.New(),
		ctx:       ctx,
		cancel:    cancel,
		store:     store.NewStore(size, 1000, m),
		m:         make(map[string]aggregator),
	}

	return s
}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (s *Storage) Get(
	name string,
	start time.Time,
	end time.Time,
	envelopeType store.EnvelopeType,
	limit int,
) []*loggregator_v2.Envelope {
	return s.store.Get(name, start, end, envelopeType, limit)
}

// Add adds a SourceID for the storage to fetch.
func (s *Storage) Add(name, sourceID string) {
	agg, ok := s.m[name]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		agg = aggregator{
			agg:    streamaggregator.New(),
			ctx:    ctx,
			cancel: cancel,
		}

		go s.start(name, agg)
	}

	agg.sourceIDCount++
	s.m[name] = agg

	agg.agg.AddProducer(sourceID, streamaggregator.ProducerFunc(func(ctx context.Context, request interface{}, c chan<- interface{}) {
		gologcache.Walk(sourceID, func(e []*loggregator_v2.Envelope) bool {
			for _, ee := range e {
				select {
				case c <- ee:
				case <-ctx.Done():
					return false
				}
			}

			return true
		}, gologcache.Reader(s.r),
			gologcache.WithWalkBackoff(gologcache.NewAlwaysRetryBackoff(time.Second)),
		)
	}))
}

// Remove removes a SourceID from the store. The data will not be eagerly
// deleted but additional data will not be added.
func (s *Storage) Remove(name, sourceID string) {
	agg, ok := s.m[name]
	if !ok {
		return
	}

	agg.agg.RemoveProducer(sourceID)

	agg.sourceIDCount--
	if agg.sourceIDCount == 0 {
		delete(s.m, name)
		return
	}

	s.m[name] = agg
}

func (s *Storage) start(name string, agg aggregator) {
	for e := range agg.agg.Consume(agg.ctx, nil) {
		s.store.Put(e.(*loggregator_v2.Envelope), name)
	}
}

type aggregator struct {
	agg           *streamaggregator.StreamAggregator
	sourceIDCount int
	ctx           context.Context
	cancel        func()
}

type envelopes []*loggregator_v2.Envelope

func (i envelopes) Len() int {
	return len(i)
}

func (i envelopes) Less(a, b int) bool {
	return i[a].GetTimestamp() < i[b].GetTimestamp()
}

func (i envelopes) Swap(a, b int) {
	tmp := i[a]
	i[a] = i[b]
	i[b] = tmp
}
