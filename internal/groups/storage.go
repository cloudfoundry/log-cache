package groups

import (
	"context"
	"fmt"
	"log"
	"sort"
	"time"

	diodes "code.cloudfoundry.org/go-diodes"
	logcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	streamaggregator "code.cloudfoundry.org/go-stream-aggregator"
	"code.cloudfoundry.org/log-cache/internal/store"
)

// Storage stores data for groups. It prunes data out when the data is read.
// It replenishes when the Replenish method is invoked.
type Storage struct {
	log     *log.Logger
	r       Reader
	backoff time.Duration

	// key=name
	m map[string]*aggregator

	streamAgg *streamaggregator.StreamAggregator

	diode *diodes.Poller
	store *store.Store
}

// Reader reads envelopes from LogCache.
type Reader func(
	ctx context.Context,
	sourceID string,
	start time.Time,
	opts ...logcache.ReadOption,
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
	maxPerSource int,
	r Reader,
	backoff time.Duration,
	p store.Pruner,
	m Metrics,
	log *log.Logger,
) *Storage {
	s := &Storage{
		log:       log,
		r:         r,
		backoff:   backoff,
		streamAgg: streamaggregator.New(),
		store:     store.NewStore(maxPerSource, 1000, p, m),
		diode: diodes.NewPoller(diodes.NewManyToOne(10000, diodes.AlertFunc(func(dropped int) {
			log.Printf("dropped %d", dropped)
		}))),
		m: make(map[string]*aggregator),
	}

	go s.run()

	return s
}

// Get fetches envelopes from the store based on the source ID, start and end
// time. Start is inclusive while end is not: [start..end).
func (s *Storage) Get(
	name string,
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	limit int,
	descending bool,
	requesterID uint64,
) []*loggregator_v2.Envelope {
	encodedName := s.encodeName(name, requesterID)
	return s.store.Get(encodedName, start, end, envelopeTypes, limit, descending)
}

// Add adds a SourceID group for the storage to fetch.
func (s *Storage) Add(name string, sourceIDs []string) {
	agg := s.initAggregator(name)

	agg.sourceIDs = append(agg.sourceIDs, sourceIDs)
	sort.Sort(sourceIDGroups(agg.sourceIDs))
	s.assignSourceIDs(agg)
}

type sourceIDGroups [][]string

func (g sourceIDGroups) Len() int {
	return len(g)
}

func (g sourceIDGroups) Swap(i, j int) {
	t := g[i]
	g[i] = g[j]
	g[j] = t
}

func (g sourceIDGroups) Less(i, j int) bool {
	a := concat(g[i])
	b := concat(g[j])

	return a < b
}

// AddRequester adds a request ID for sharded reading.
func (s *Storage) AddRequester(name string, ID uint64, remoteOnly bool) {
	agg := s.initAggregator(name)

	if _, ok := agg.requesterIDs[ID]; !ok {
		ctx, cancel := context.WithCancel(agg.ctx)
		req := &requester{
			agg:    streamaggregator.New(),
			ctx:    ctx,
			cancel: cancel,
			ID:     ID,
		}

		if !remoteOnly {
			go s.start(s.encodeName(name, ID), req)
		}
		agg.requesterIDs[ID] = req
		agg.requesters = append(agg.requesters, req)
		sort.Sort(reqs(agg.requesters))
	}
	s.assignSourceIDs(agg)
}

// RemoveRequester removes a request ID for sharded reading.
func (s *Storage) RemoveRequester(name string, ID uint64) {
	agg := s.initAggregator(name)
	agg.requesterIDs[ID].cancel()
	delete(agg.requesterIDs, ID)

	for i, r := range agg.requesters {
		if r.ID == ID {
			agg.requesters = append(agg.requesters[:i], agg.requesters[i+1:]...)
			break
		}
	}

	s.assignSourceIDs(agg)
}

// Remove removes a SourceID group from the store. The data will not be
// eagerly deleted but additional data will not be added.
func (s *Storage) Remove(name string, sourceIDs []string) {
	agg, ok := s.m[name]
	if !ok {
		return
	}

	a := concat(sourceIDs)

	for i, id := range agg.sourceIDs {
		b := concat(id)
		if b == a {
			agg.sourceIDs = append(agg.sourceIDs[:i], agg.sourceIDs[i+1:]...)
		}
	}

	s.assignSourceIDs(agg)

	if len(agg.sourceIDs) == 0 {
		agg.cancel()
		delete(s.m, name)
		return
	}
}

func (s *Storage) assignSourceIDs(agg *aggregator) {
	if len(agg.requesters) == 0 {
		return
	}

	// Setup expected
	expected := make(map[uint64][][]string)
	for si, sourceID := range agg.sourceIDs {
		reqID := agg.requesters[si%len(agg.requesters)].ID
		expected[reqID] = append(expected[reqID], sourceID)
	}

	// Find Delta of sourceIds per Requester
	for _, req := range agg.requesters {
		// Add sourceIDs that are in expected, but not in req.sourceIDs
		for _, sid := range expected[req.ID] {
			if !containsSourceIDs(req.sourceIDs, sid) {
				agg.add(req.ID, sid)
			}
		}

		// Remove sourceIDs that aren't in expected, but are in req.sourceIDs
		for _, sid := range req.sourceIDs {
			if !containsSourceIDs(expected[req.ID], sid) {
				agg.remove(req.ID, sid)
			}
		}
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

		s.m[name] = agg
	}

	return agg
}

type envelopeWrapper struct {
	e           *loggregator_v2.Envelope
	encodedName string
}

func (s *Storage) run() {
	for {
		e := (*envelopeWrapper)(s.diode.Next())
		s.store.Put(e.e, e.encodedName)
	}
}

func (s *Storage) start(encodedName string, req *requester) {
	for e := range req.agg.Consume(req.ctx, nil) {
		s.diode.Set(diodes.GenericDataType(&envelopeWrapper{
			e:           e.(*loggregator_v2.Envelope),
			encodedName: encodedName,
		}))
	}
}

func (s *Storage) encodeName(name string, requesterID uint64) string {
	return fmt.Sprintf("%s-%d", name, requesterID)
}

type aggregator struct {
	r            Reader
	sourceIDs    [][]string
	requesterIDs map[uint64]*requester
	requesters   []*requester
	backoff      time.Duration

	ctx    context.Context
	cancel func()
}

type requester struct {
	sourceIDs [][]string
	agg       *streamaggregator.StreamAggregator
	ID        uint64

	ctx    context.Context
	cancel func()
}

func containsSourceIDs(ids [][]string, sid []string) bool {
	b := concat(sid)

	for _, s := range ids {
		// TODO: This could be more performant.
		a := concat(s)
		if a == b {
			return true
		}
	}
	return false
}

func (a *aggregator) add(id uint64, sourceIDs []string) error {
	req := a.requesterIDs[id]
	req.sourceIDs = append(req.sourceIDs, sourceIDs)

	for _, sourceID := range sourceIDs {
		// shadow to avoid closure problems
		sourceID := sourceID
		req.agg.AddProducer(sourceID, streamaggregator.ProducerFunc(func(ctx context.Context, request interface{}, c chan<- interface{}) {
			logcache.Walk(ctx, sourceID,
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
				logcache.Reader(a.r),
				logcache.WithWalkBackoff(logcache.NewAlwaysRetryBackoff(a.backoff)),
			)
		}))
	}

	return nil
}

func (a *aggregator) remove(id uint64, sourceIDs []string) error {
	req := a.requesterIDs[id]
	sid := concat(sourceIDs)
	for i, x := range req.sourceIDs {
		b := concat(x)
		if sid == b {
			req.sourceIDs = append(req.sourceIDs[:i], req.sourceIDs[i+1:]...)
		}
	}

	for _, sourceID := range sourceIDs {
		req.agg.RemoveProducer(sourceID)
	}

	return nil
}

type reqs []*requester

func (r reqs) Len() int {
	return len(r)
}

func (r reqs) Swap(i, j int) {
	t := r[i]
	r[i] = r[j]
	r[j] = t
}

func (r reqs) Less(i, j int) bool {
	return r[i].ID < r[j].ID
}
