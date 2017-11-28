package ingress_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("EnvelopeStream", func() {
	var (
		s               *ingress.EnvelopeStream
		envelopes       chan []*loggregator_v2.Envelope
		storedEnvelopes chan []*loggregator_v2.Envelope
		spyMetrics      *spyMetrics
	)

	BeforeEach(func() {
		spyMetrics = newSpyMetrics()
		storedEnvelopes = make(chan []*loggregator_v2.Envelope, 100)
		envelopes = make(chan []*loggregator_v2.Envelope, 100)

		s = ingress.NewEnvelopeStream(
			func() []*loggregator_v2.Envelope {
				return <-envelopes
			},
			func(e []*loggregator_v2.Envelope) {
				storedEnvelopes <- e
			},
			spyMetrics,
		)
	})

	It("takes data from the receiver and stores it in the store", func() {
		e := []*loggregator_v2.Envelope{
			{Timestamp: 99},
			{Timestamp: 101},
		}
		envelopes <- e

		go s.Start()

		Eventually(storedEnvelopes).Should(Receive(Equal(e)))

		// NOTE: This is thread safe due to the storedEnvelopes channel.
		Expect(spyMetrics.values["ingress"]).To(Equal(uint64(2)))
	})
})

type spyMetrics struct {
	values map[string]uint64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		values: make(map[string]uint64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(delta uint64) {
	return func(d uint64) {
		s.values[name] += d
	}
}
