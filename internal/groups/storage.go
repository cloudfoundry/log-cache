package groups

import (
	"context"
	"log"
	"sort"
	"time"

	gologcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/go-stream-aggregator"
)

// Storage stores data for groups. It prunes data out when the data is read.
// It replenishes when the Replenish method is invoked.
type Storage struct {
	log *log.Logger
	r   Reader

	ctx    context.Context
	cancel func()

	streamAgg     *streamaggregator.StreamAggregator
	store         chan *loggregator_v2.Envelope
	batchInterval time.Duration
}

// Reader reads envelopes from LogCache.
type Reader func(
	sourceID string,
	start time.Time,
	opts ...gologcache.ReadOption,
) ([]*loggregator_v2.Envelope, error)

// NewStorage returns new Storage.
func NewStorage(size int, batchInterval time.Duration, r Reader, log *log.Logger) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Storage{
		log:           log,
		r:             r,
		streamAgg:     streamaggregator.New(),
		ctx:           ctx,
		cancel:        cancel,
		store:         make(chan *loggregator_v2.Envelope, size),
		batchInterval: batchInterval,
	}

	go s.start()

	return s
}

// Next is a destructive method call. It returns and prunes data from the
// storage.
func (s *Storage) Next() (batch []*loggregator_v2.Envelope) {
	t := time.NewTimer(s.batchInterval)
	defer t.Stop()

	defer func() {
		sort.Sort(envelopes(batch))
	}()

	for i := 0; i < 100; i++ {
		select {
		case e := <-s.store:
			batch = append(batch, e)
		case <-t.C:
			return batch
		case <-s.ctx.Done():
			return nil
		}
	}
	return batch
}

// Close prevents Next from returning any data.
func (s *Storage) Close() error {
	s.cancel()
	return nil
}

// Add adds a SourceID for the storage to fetch.
func (s *Storage) Add(sourceID string) {
	s.streamAgg.AddProducer(sourceID, streamaggregator.ProducerFunc(func(ctx context.Context, request interface{}, c chan<- interface{}) {
		gologcache.Walk(sourceID, func(e []*loggregator_v2.Envelope) bool {
			for _, ee := range e {
				select {
				case s.store <- ee:
				case <-ctx.Done():
					return false
				}
			}

			return true
		}, gologcache.Reader(s.r))
	}))
}

// Remove removes a SourceID from the store. The data will not be eagerly
// deleted but additional data will not be added.
func (s *Storage) Remove(sourceID string) {
	s.streamAgg.RemoveProducer(sourceID)
}

func (s *Storage) start() {
	for e := range s.streamAgg.Consume(s.ctx, nil) {
		select {
		case s.store <- e.(*loggregator_v2.Envelope):
		case <-s.ctx.Done():
			return
		}
	}
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
