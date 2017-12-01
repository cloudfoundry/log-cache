package ingress_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pubsub", func() {
	var (
		r *ingress.Pubsub

		lookupEnvelope *loggregator_v2.Envelope
		lookupResult   uint64
	)

	BeforeEach(func() {
		lookup := func(e *loggregator_v2.Envelope) uint64 {
			lookupEnvelope = e
			return lookupResult
		}

		r = ingress.NewPubsub(lookup)
	})

	It("routes data via lookup function", func() {
		var (
			a, b   int
			actual *loggregator_v2.Envelope
		)
		r.Subscribe(0, func(_ *loggregator_v2.Envelope) {
			a++
		})

		r.Subscribe(1, func(e *loggregator_v2.Envelope) {
			actual = e
			b++
		})

		lookupResult = 1
		expected := &loggregator_v2.Envelope{Timestamp: 99}
		r.Publish(expected)

		Expect(a).To(Equal(0))
		Expect(b).To(Equal(1))
		Expect(actual).To(Equal(expected))
		Expect(lookupEnvelope).To(Equal(expected))
	})
})
