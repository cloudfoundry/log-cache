package store_test

import (
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("Store", func() {
	var (
		s  *store.Store
		sm *spyMetrics
		sp *spyPruner
	)

	BeforeEach(func() {
		sm = newSpyMetrics()
		sp = newSpyPruner()
		s = store.NewStore(5, 10, sp, sm)
	})

	It("fetches data based on time and source ID", func() {
		e1 := buildEnvelope(1, "a")
		e2 := buildEnvelope(2, "b")
		e3 := buildEnvelope(3, "a")
		e4 := buildEnvelope(4, "a")

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())

		start := time.Unix(0, 0)
		end := time.Unix(0, 4)
		envelopes := s.Get("a", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(2))

		for _, e := range envelopes {
			Expect(e.SourceId).To(Equal("a"))
		}

		Expect(sm.values["Expired"]).To(Equal(0.0))
	})

	It("returns a maximum number of envelopes in ascending order", func() {
		e1 := buildEnvelope(1, "a")
		e2 := buildEnvelope(2, "a")
		e3 := buildEnvelope(3, "a")
		e4 := buildEnvelope(4, "a")

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 3, false)
		Expect(envelopes).To(HaveLen(3))
		Expect(envelopes[0].GetTimestamp()).To(Equal(int64(1)))
		Expect(envelopes[1].GetTimestamp()).To(Equal(int64(2)))
		Expect(envelopes[2].GetTimestamp()).To(Equal(int64(3)))
	})

	It("returns a maximum number of envelopes in descending order", func() {
		e1 := buildEnvelope(1, "a")
		e2 := buildEnvelope(2, "a")
		e3 := buildEnvelope(3, "a")
		e4 := buildEnvelope(4, "a")

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 3, true)
		Expect(envelopes).To(HaveLen(3))
		Expect(envelopes[0].GetTimestamp()).To(Equal(int64(4)))
		Expect(envelopes[1].GetTimestamp()).To(Equal(int64(3)))
		Expect(envelopes[2].GetTimestamp()).To(Equal(int64(2)))
	})

	DescribeTable("fetches data based on envelope type",
		func(envelopeType store.EnvelopeType, envelopeWrapper interface{}) {
			e1 := buildTypedEnvelope(0, "a", &loggregator_v2.Log{})
			e2 := buildTypedEnvelope(1, "a", &loggregator_v2.Counter{})
			e3 := buildTypedEnvelope(2, "a", &loggregator_v2.Gauge{})
			e4 := buildTypedEnvelope(3, "a", &loggregator_v2.Timer{})
			e5 := buildTypedEnvelope(4, "a", &loggregator_v2.Event{})

			s.Put(e1, e1.GetSourceId())
			s.Put(e2, e2.GetSourceId())
			s.Put(e3, e3.GetSourceId())
			s.Put(e4, e4.GetSourceId())
			s.Put(e5, e5.GetSourceId())

			start := time.Unix(0, 0)
			end := time.Unix(0, 9999)
			envelopes := s.Get("a", start, end, []store.EnvelopeType{envelopeType}, 5, false)
			Expect(envelopes).To(HaveLen(1))
			Expect(envelopes[0].Message).To(BeAssignableToTypeOf(envelopeWrapper))

			// No Filter
			envelopes = s.Get("a", start, end, nil, 10, false)
			Expect(envelopes).To(HaveLen(5))
		},

		Entry("Log", &loggregator_v2.Log{}, &loggregator_v2.Envelope_Log{}),
		Entry("Counter", &loggregator_v2.Counter{}, &loggregator_v2.Envelope_Counter{}),
		Entry("Gauge", &loggregator_v2.Gauge{}, &loggregator_v2.Envelope_Gauge{}),
		Entry("Timer", &loggregator_v2.Timer{}, &loggregator_v2.Envelope_Timer{}),
		Entry("Event", &loggregator_v2.Event{}, &loggregator_v2.Envelope_Event{}),
	)

	It("is thread safe", func() {
		var wg sync.WaitGroup
		wg.Add(2)
		defer wg.Wait()

		e1 := buildEnvelope(0, "a")
		go func() {
			defer wg.Done()
			s.Put(e1, e1.GetSourceId())
		}()

		go func() {
			defer wg.Done()
			s.Meta()
		}()

		start := time.Unix(0, 0)
		end := time.Unix(9999, 0)

		Eventually(func() int { return len(s.Get("a", start, end, nil, 10, false)) }).Should(Equal(1))
	})

	It("survives being over pruned", func() {
		s = store.NewStore(10, 10, sp, sm)
		sp.result = 1000
		e1 := buildTypedEnvelope(0, "b", &loggregator_v2.Log{})
		Expect(func() { s.Put(e1, e1.GetSourceId()) }).ToNot(Panic())
	})

	It("truncates older envelopes when max size is reached", func() {
		s = store.NewStore(10, 5, sp, sm)
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

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())
		s.Put(e5, e5.GetSourceId())
		s.Put(e6, e6.GetSourceId())
		s.Put(e7, e7.GetSourceId())

		// Next put should prune 3 envelopes
		sp.result = 3

		s.Put(e8, e8.GetSourceId())

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(5))

		for i, e := range envelopes {
			Expect(e.Timestamp).To(Equal(int64(i + 2)))
		}

		Expect(sm.values["Expired"]).To(Equal(3.0))
		Expect(sm.values["StoreSize"]).To(Equal(5.0))

		// Ensure b was removed fully
		for s := range s.Meta() {
			Expect(s).To(Equal("a"))
		}
	})

	It("truncates envelopes for a specific source-id if its max size is reached", func() {
		s = store.NewStore(2, 2, sp, sm)
		// e1 should not be truncated
		e1 := buildTypedEnvelope(0, "b", &loggregator_v2.Log{})
		// e2 should be truncated
		e2 := buildTypedEnvelope(1, "a", &loggregator_v2.Log{})
		e3 := buildTypedEnvelope(2, "a", &loggregator_v2.Log{})
		e4 := buildTypedEnvelope(3, "a", &loggregator_v2.Log{})

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(2))
		Expect(envelopes[0].Timestamp).To(Equal(int64(2)))
		Expect(envelopes[1].Timestamp).To(Equal(int64(3)))

		envelopes = s.Get("b", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(1))

		Expect(sm.values["Expired"]).To(Equal(1.0))
	})

	It("sets (via metrics) the store's period in milliseconds", func() {
		e := buildTypedEnvelope(time.Now().Add(-time.Minute).UnixNano(), "b", &loggregator_v2.Log{})
		s.Put(e, e.GetSourceId())

		Expect(sm.values["CachePeriod"]).To(BeNumerically("~", float64(time.Minute/time.Millisecond), 1000))
	})

	It("uses the given index", func() {
		s = store.NewStore(2, 2, sp, sm)
		e := buildTypedEnvelope(0, "a", &loggregator_v2.Log{})
		s.Put(e, "some-id")

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)

		envelopes := s.Get("some-id", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(1))
	})

	It("returns the indices in the store", func() {
		s = store.NewStore(2, 2, sp, sm)

		// Will be pruned by pruner
		s.Put(buildTypedEnvelope(0, "index-0", &loggregator_v2.Log{}), "index-0")
		s.Put(buildTypedEnvelope(1, "index-1", &loggregator_v2.Log{}), "index-1")

		// Timestamp 2 should be pruned as we exceed the max per source of 2.
		s.Put(buildTypedEnvelope(2, "index-2", &loggregator_v2.Log{}), "index-2")
		s.Put(buildTypedEnvelope(3, "index-2", &loggregator_v2.Log{}), "index-2")
		s.Put(buildTypedEnvelope(4, "index-2", &loggregator_v2.Log{}), "index-2")

		// Prune first 2 entries (timestamp 0 and 1)
		sp.result = 2
		s.Put(buildTypedEnvelope(5, "index-1", &loggregator_v2.Log{}), "index-1")

		meta := s.Meta()

		// Does not contain index-0
		Expect(meta).To(HaveLen(2))

		Expect(meta).To(HaveKeyWithValue("index-1", store.MetaInfo{
			Count:   1,
			Expired: 1,
			Oldest:  time.Unix(0, 5),
			Newest:  time.Unix(0, 5),
		}))

		Expect(meta).To(HaveKeyWithValue("index-2", store.MetaInfo{
			Count:   2,
			Expired: 1,
			Oldest:  time.Unix(0, 3),
			Newest:  time.Unix(0, 4),
		}))
	})

	It("survives the just added entry from being pruned", func() {
		s = store.NewStore(2, 2, sp, sm)

		s.Put(buildTypedEnvelope(0, "index-0", &loggregator_v2.Log{}), "index-0")
		s.Put(buildTypedEnvelope(1, "index-0", &loggregator_v2.Log{}), "index-0")

		sp.result = 1
		s.Put(buildTypedEnvelope(-1, "index-1", &loggregator_v2.Log{}), "index-1")
		Expect(s.Meta()).ToNot(HaveKey("index-1"))
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

type spyMetrics struct {
	values map[string]float64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		values: make(map[string]float64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(delta uint64) {
	return func(d uint64) {
		s.values[name] += float64(d)
	}
}

func (s *spyMetrics) NewGauge(name string) func(value float64) {
	return func(v float64) {
		s.values[name] = v
	}
}

type spyPruner struct {
	called int
	result int
}

func newSpyPruner() *spyPruner {
	return &spyPruner{}
}

func (s *spyPruner) Prune() int {
	s.called++
	return s.result
}
