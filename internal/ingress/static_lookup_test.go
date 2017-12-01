package ingress_test

import (
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/ingress"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("StaticLookup", func() {
	var (
		l *ingress.StaticLookup

		sourceId string
		hash     uint64
	)

	BeforeEach(func() {
		l = ingress.NewStaticLookup(4, func(s string) uint64 {
			sourceId = s
			return hash
		})
	})

	It("associates indexes for each route", func() {
		// range #0 -> 0 - 4611686018427387902
		// range #1 -> 4611686018427387903 - 9223372036854775805
		// range #2 -> 9223372036854775806 - 13835058055282163708
		// range #3 -> 13835058055282163709 - 18446744073709551615

		// Range #0
		hash = 0
		i := l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(0)))

		hash = 4611686018427387902
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(0)))

		// Range #1
		hash = 4611686018427387903
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(1)))

		hash = 9223372036854775805
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(1)))

		// Range #2
		hash = 9223372036854775806
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(2)))

		hash = 13835058055282163708
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(2)))

		// Range #3
		hash = 13835058055282163709
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(3)))

		hash = 18446744073709551615
		i = l.Lookup(&loggregator_v2.Envelope{SourceId: "source-a"})
		Expect(i).To(Equal(uint64(3)))

		Expect(sourceId).To(Equal("source-a"))
	})
})
