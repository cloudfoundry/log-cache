package ingress

import "code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"

// EnvelopeStream reads envelopes from a Stream and posts them to the given
// store.
type EnvelopeStream struct {
	rx           Stream
	store        Store
	incEnvelopes func(delta uint64)
}

// NewEnvelopeStream constructs and returns an EnvelopeStream.
func NewEnvelopeStream(rx Stream, st Store, m MetricClient) *EnvelopeStream {
	return &EnvelopeStream{
		rx:           rx,
		store:        st,
		incEnvelopes: m.NewCounter("Ingress"),
	}
}

// Stream returns blocks until a batch of envelopes is available.
type Stream func() []*loggregator_v2.Envelope

// Store takes and stores batches of envelopes.
type Store func([]*loggregator_v2.Envelope)

// MetricClient returns a method of emitting metrics.
type MetricClient interface {
	// NewCounter returns a counter metric.
	NewCounter(name string) func(delta uint64)
}

// Start reads from the stream and writes to the given store. It blocks
// indefinitely.
func (s *EnvelopeStream) Start() {
	for {
		e := s.rx()
		s.incEnvelopes(uint64(len(e)))
		s.store(e)
	}
}
