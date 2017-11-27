package store_test

import (
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Store", func() {
	var (
		s *store.Store
	)

	BeforeEach(func() {
		s = store.NewStore(5, 5)
	})

	It("fetches data based on time and source ID", func() {
		e1 := buildEnvelope(1, "a")
		e2 := buildEnvelope(2, "b")
		e3 := buildEnvelope(3, "a")
		e4 := buildEnvelope(4, "a")

		s.Put([]*loggregator_v2.Envelope{e1, e2})
		s.Put([]*loggregator_v2.Envelope{e3, e4})

		start := time.Unix(0, 0)
		end := time.Unix(0, 4)
		envelopes := s.Get("a", start, end, nil, 10)
		Expect(envelopes).To(HaveLen(2))

		for _, e := range envelopes {
			Expect(e.SourceId).To(Equal("a"))
		}
	})

	It("returns a maximum number of envelopes", func() {
		e1 := buildEnvelope(1, "a")
		e2 := buildEnvelope(2, "a")
		e3 := buildEnvelope(3, "a")
		e4 := buildEnvelope(4, "a")

		s.Put([]*loggregator_v2.Envelope{e1, e2})
		s.Put([]*loggregator_v2.Envelope{e3, e4})

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 3)
		Expect(envelopes).To(HaveLen(3))
	})

	DescribeTable("fetches data based on envelope type",
		func(envelopeType, envelopeWrapper interface{}) {
			e1 := buildTypedEnvelope(0, "a", &loggregator_v2.Log{})
			e2 := buildTypedEnvelope(1, "a", &loggregator_v2.Counter{})
			e3 := buildTypedEnvelope(2, "a", &loggregator_v2.Gauge{})
			e4 := buildTypedEnvelope(3, "a", &loggregator_v2.Timer{})
			e5 := buildTypedEnvelope(4, "a", &loggregator_v2.Event{})
			s.Put([]*loggregator_v2.Envelope{e1, e2, e3, e4, e5})

			start := time.Unix(0, 0)
			end := time.Unix(0, 9999)
			envelopes := s.Get("a", start, end, envelopeType, 5)
			Expect(envelopes).To(HaveLen(1))
			Expect(envelopes[0].Message).To(BeAssignableToTypeOf(envelopeWrapper))

			// No Filter
			envelopes = s.Get("a", start, end, nil, 10)
			Expect(envelopes).To(HaveLen(5))
		},

		Entry("Log", &loggregator_v2.Log{}, &loggregator_v2.Envelope_Log{}),
		Entry("Counter", &loggregator_v2.Counter{}, &loggregator_v2.Envelope_Counter{}),
		Entry("Gauge", &loggregator_v2.Gauge{}, &loggregator_v2.Envelope_Gauge{}),
		Entry("Timer", &loggregator_v2.Timer{}, &loggregator_v2.Envelope_Timer{}),
		Entry("Event", &loggregator_v2.Event{}, &loggregator_v2.Envelope_Event{}),
	)

	It("is thread safe", func() {
		e1 := buildEnvelope(0, "a")
		go func() {
			s.Put([]*loggregator_v2.Envelope{e1})
		}()

		start := time.Unix(0, 0)
		end := time.Unix(9999, 0)

		Eventually(func() int { return len(s.Get("a", start, end, nil, 10)) }).Should(Equal(1))
	})

	It("truncates older envelopes when max size is reached", func() {
		s = store.NewStore(5, 10)
		// e1 should be truncated and sourceID "b" should be forgotten.
		e1 := buildTypedEnvelope(0, "b", &loggregator_v2.Log{})
		// e2 should be truncated.
		e2 := buildTypedEnvelope(1, "a", &loggregator_v2.Counter{})

		// e3-e7 should be available
		e3 := buildTypedEnvelope(2, "a", &loggregator_v2.Gauge{})
		e4 := buildTypedEnvelope(3, "a", &loggregator_v2.Timer{})
		e5 := buildTypedEnvelope(4, "a", &loggregator_v2.Event{})
		e6 := buildTypedEnvelope(5, "a", &loggregator_v2.Event{})
		e7 := buildTypedEnvelope(6, "a", &loggregator_v2.Event{})

		// e8 should be truncated even though it is late
		e8 := buildTypedEnvelope(0, "a", &loggregator_v2.Event{})

		s.Put([]*loggregator_v2.Envelope{e1, e2, e3, e4, e5, e6, e7, e8})

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 10)
		Expect(envelopes).To(HaveLen(5))

		for i, e := range envelopes {
			Expect(e.Timestamp).To(Equal(int64(i + 2)))
		}
	})

	It("truncates envelopes for a specific source-id if its max size is reached", func() {
		s = store.NewStore(5, 2)
		// e1 should not be truncated
		e1 := buildTypedEnvelope(0, "b", &loggregator_v2.Log{})
		// e2 should be truncated
		e2 := buildTypedEnvelope(1, "a", &loggregator_v2.Log{})
		e3 := buildTypedEnvelope(2, "a", &loggregator_v2.Log{})
		e4 := buildTypedEnvelope(3, "a", &loggregator_v2.Log{})

		s.Put([]*loggregator_v2.Envelope{e1, e2, e3, e4})

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 10)
		Expect(envelopes).To(HaveLen(2))
		Expect(envelopes[0].Timestamp).To(Equal(int64(2)))
		Expect(envelopes[1].Timestamp).To(Equal(int64(3)))

		envelopes = s.Get("b", start, end, nil, 10)
		Expect(envelopes).To(HaveLen(1))
	})
})

func buildEnvelope(timestamp int64, sourceID string) *loggregator_v2.Envelope {
	return &loggregator_v2.Envelope{
		Timestamp: timestamp,
		SourceId:  sourceID,
	}
}

func buildTypedEnvelope(timestamp int64, sourceID string, t interface{}) *loggregator_v2.Envelope {
	e := &loggregator_v2.Envelope{
		Timestamp: timestamp,
		SourceId:  sourceID,
	}

	switch t.(type) {
	case *loggregator_v2.Log:
		e.Message = &loggregator_v2.Envelope_Log{
			Log: &loggregator_v2.Log{},
		}
	case *loggregator_v2.Counter:
		e.Message = &loggregator_v2.Envelope_Counter{
			Counter: &loggregator_v2.Counter{},
		}
	case *loggregator_v2.Gauge:
		e.Message = &loggregator_v2.Envelope_Gauge{
			Gauge: &loggregator_v2.Gauge{},
		}
	case *loggregator_v2.Timer:
		e.Message = &loggregator_v2.Envelope_Timer{
			Timer: &loggregator_v2.Timer{},
		}
	case *loggregator_v2.Event:
		e.Message = &loggregator_v2.Envelope_Event{
			Event: &loggregator_v2.Event{},
		}
	default:
		panic("unexpected type")
	}

	return e
}
