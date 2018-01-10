package groups

import (
	"context"
	"fmt"
	"log"
	"time"

	gologcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	orchestrator "code.cloudfoundry.org/go-orchestrator"
	streamaggregator "code.cloudfoundry.org/go-stream-aggregator"
	"code.cloudfoundry.org/log-cache/internal/store"
)

// Storage stores data for groups. It prunes data out when the data is read.
// It replenishes when the Replenish method is invoked.
type Storage struct {
	log     *log.Logger
	r       Reader
	backoff time.Duration

	ctx    context.Context
	cancel func()

	// key=name
	m map[string]*aggregator

	streamAgg *streamaggregator.StreamAggregator
	store     *store.Store
}

// Reader reads envelopes from LogCache.
type Reader func(
	ctx context.Context,
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
	backoff time.Duration,
	m Metrics,
	log *log.Logger,
) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Storage{
		log:       log,
		r:         r,
		backoff:   backoff,
		streamAgg: streamaggregator.New(),
		ctx:       ctx,
		cancel:    cancel,
		store:     store.NewStore(size, 1000, m),
		m:         make(map[string]*aggregator),
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
	requesterID uint64,
) []*loggregator_v2.Envelope {
	encodedName := s.encodeName(name, requesterID)
	return s.store.Get(encodedName, start, end, envelopeType, limit)
}

// Add adds a SourceID for the storage to fetch.
func (s *Storage) Add(name, sourceID string) {
	agg := s.initAggregator(name)

	agg.sourceIDs = append(agg.sourceIDs, sourceID)

	agg.orch.AddTask(sourceID)
	agg.orch.NextTerm(context.Background())
}

// AddRequester adds a request ID for sharded reading.
func (s *Storage) AddRequester(name string, ID uint64) {
	agg := s.initAggregator(name)

	if _, ok := agg.requesterIDs[ID]; !ok {
		ctx, cancel := context.WithCancel(agg.ctx)
		req := &requester{
			agg:    streamaggregator.New(),
			ctx:    ctx,
			cancel: cancel,
		}
		go s.start(s.encodeName(name, ID), req)
		agg.requesterIDs[ID] = req
	}

	agg.orch.AddWorker(ID)
	agg.orch.NextTerm(context.Background())
}

// RemoveRequester removes a request ID for sharded reading.
func (s *Storage) RemoveRequester(name string, ID uint64) {
	agg := s.initAggregator(name)

	agg.orch.RemoveWorker(ID)
	agg.orch.NextTerm(context.Background())

	agg.requesterIDs[ID].cancel()
	delete(agg.requesterIDs, ID)
}

// Remove removes a SourceID from the store. The data will not be eagerly
// deleted but additional data will not be added.
func (s *Storage) Remove(name, sourceID string) {
	agg, ok := s.m[name]
	if !ok {
		return
	}

	agg.orch.RemoveTask(sourceID)
	agg.orch.NextTerm(context.Background())

	for i, id := range agg.sourceIDs {
		if id == sourceID {
			agg.sourceIDs = append(agg.sourceIDs[:i], agg.sourceIDs[i+1:]...)
		}
	}

	if len(agg.sourceIDs) == 0 {
		agg.cancel()
		delete(s.m, name)
		return
	}
}

func (s *Storage) initAggregator(name string) *aggregator {
	agg, ok := s.m[name]
	if !ok {
		ctx, cancel := context.WithCancel(context.Background())
		agg = &aggregator{
			r:            s.r,
			ctx:          ctx,
			cancel:       cancel,
			requesterIDs: make(map[uint64]*requester),
			backoff:      s.backoff,
		}

		agg.orch = orchestrator.New(agg)

		s.m[name] = agg
	}

	return agg
}

func (s *Storage) start(encodedName string, req *requester) {
	for e := range req.agg.Consume(req.ctx, nil) {
		s.store.Put(e.(*loggregator_v2.Envelope), encodedName)
	}
}

func (s *Storage) encodeName(name string, requesterID uint64) string {
	return fmt.Sprintf("%s-%d", name, requesterID)
}

type aggregator struct {
	orch         *orchestrator.Orchestrator
	r            Reader
	sourceIDs    []string
	requesterIDs map[uint64]*requester
	backoff      time.Duration

	ctx    context.Context
	cancel func()
}

type requester struct {
	sourceIDs []string
	agg       *streamaggregator.StreamAggregator

	ctx    context.Context
	cancel func()
}

// List implements orchestrator.Communicator.
func (a *aggregator) List(ctx context.Context, worker interface{}) ([]interface{}, error) {
	y, ok := a.requesterIDs[worker.(uint64)]
	if !ok {
		return nil, nil
	}

	var r []interface{}
	for _, x := range y.sourceIDs {
		r = append(r, x)
	}

	return r, nil
}

// Add implements orchestrator.Communicator.
func (a *aggregator) Add(ctx context.Context, worker interface{}, task interface{}) error {
	id := worker.(uint64)
	sourceID := task.(string)
	req := a.requesterIDs[id]

	// Ensure we aren't already doing this sourceID
	for _, sid := range req.sourceIDs {
		if sourceID == sid {
			return nil
		}
	}

	req.sourceIDs = append(req.sourceIDs, sourceID)

	req.agg.AddProducer(sourceID, streamaggregator.ProducerFunc(func(ctx context.Context, request interface{}, c chan<- interface{}) {
		gologcache.Walk(ctx, sourceID,
			func(e []*loggregator_v2.Envelope) bool {
				for _, ee := range e {
					select {
					case c <- ee:
					case <-ctx.Done():
						return false
					}
				}

				return true
			},
			gologcache.Reader(a.r),
			gologcache.WithWalkBackoff(gologcache.NewAlwaysRetryBackoff(a.backoff)),
		)
	}))

	return nil
}

// Remove implements orchestrator.Communicator.
func (a *aggregator) Remove(ctx context.Context, worker interface{}, task interface{}) error {
	id := worker.(uint64)
	sourceID := task.(string)
	req := a.requesterIDs[id]
	for i, x := range req.sourceIDs {
		if x == sourceID {
			req.sourceIDs = append(req.sourceIDs[:i], req.sourceIDs[i+1:]...)
		}
	}

	req.agg.RemoveProducer(sourceID)

	return nil
}
