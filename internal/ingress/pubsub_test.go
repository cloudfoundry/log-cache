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

		lookupSourceID string
		lookupResult   int
	)

	BeforeEach(func() {
		lookup := func(sourceID string) int {
			lookupSourceID = sourceID
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
		expected := &loggregator_v2.Envelope{SourceId: "some-id"}
		r.Publish(expected)

		Expect(a).To(Equal(0))
		Expect(b).To(Equal(1))
		Expect(actual).To(Equal(expected))
		Expect(lookupSourceID).To(Equal("some-id"))
	})
})
