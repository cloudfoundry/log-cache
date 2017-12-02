package ingress

import (
	"sync"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
)

// Pubsub stores subscriptions and publishes envelopes based on what the
// lookup function returns.
type Pubsub struct {
	lookup      LookUp
	subscribers *sync.Map
}

// NewPubsub creates and returns a new Pubsub.
func NewPubsub(l LookUp) *Pubsub {
	return &Pubsub{
		lookup:      l,
		subscribers: &sync.Map{},
	}
}

// LookUp is used to convert an envelope into the routing index.
type LookUp func(sourceID string) int

// Publish writes an envelope to any interested subscribers.
func (s *Pubsub) Publish(e *loggregator_v2.Envelope) {
	v, ok := s.subscribers.Load(s.lookup(e.SourceId))
	if !ok {
		panic("Something has gone poorly. This map is populated ahead of time with known values and this should never happen")
	}
	v.(Subscription)(e)
}

// Subscription is used in Subscribe. It is written to for each coresponding
// envelope.
type Subscription func(*loggregator_v2.Envelope)

// Subscribe stores subscriptions and writes corresponding envelopes.
func (s *Pubsub) Subscribe(routeIndex int, sub Subscription) {
	s.subscribers.Store(routeIndex, sub)
}
