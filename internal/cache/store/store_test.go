package store_test

import (
	"strconv"
	"sync"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/cache/store"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"

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
		s = store.NewStore(5, sp, sm)
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

		Expect(sm.GetValue("Expired")).To(Equal(0.0))
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

	Context("in ascending order", func() {
		It("respects timestamp fudging when checking the time boundaries", func() {
			s = store.NewStore(50, sp, sm)

			e0 := buildEnvelope(0, "a")
			e1 := buildEnvelope(1, "a")
			e2 := buildEnvelope(2, "a")
			e3 := buildEnvelope(3, "a")

			s.Put(e0, e0.GetSourceId())
			s.Put(e0, e0.GetSourceId())
			s.Put(e0, e0.GetSourceId())
			s.Put(e0, e0.GetSourceId())
			s.Put(e0, e0.GetSourceId())

			s.Put(e1, e1.GetSourceId())
			s.Put(e1, e1.GetSourceId())
			s.Put(e1, e1.GetSourceId())

			s.Put(e2, e2.GetSourceId())

			s.Put(e3, e3.GetSourceId())
			s.Put(e3, e3.GetSourceId())

			start := time.Unix(0, 1)
			end := time.Unix(0, 3)
			envelopes := s.Get("a", start, end, nil, 5, false)
			Expect(envelopes).To(HaveLen(4))
			Expect(envelopes[0].GetTimestamp()).To(Equal(int64(1)))
			Expect(envelopes[1].GetTimestamp()).To(Equal(int64(1)))
			Expect(envelopes[2].GetTimestamp()).To(Equal(int64(1)))
			Expect(envelopes[3].GetTimestamp()).To(Equal(int64(2)))
		})

		It("intentionally exceeds the limit when it would otherwise break up a group of fudged timestamps", func() {
			s = store.NewStore(50, sp, sm)

			e0 := buildEnvelope(0, "a")
			e1 := buildEnvelope(1, "a")

			s.Put(e0, e0.GetSourceId())
			s.Put(e1, e1.GetSourceId())
			s.Put(e1, e1.GetSourceId())

			start := time.Unix(0, 0)
			end := time.Unix(0, 2)
			envelopes := s.Get("a", start, end, nil, 2, false)
			Expect(envelopes).To(HaveLen(3))
			Expect(envelopes[0].GetTimestamp()).To(Equal(int64(0)))
			Expect(envelopes[1].GetTimestamp()).To(Equal(int64(1)))
			Expect(envelopes[2].GetTimestamp()).To(Equal(int64(1)))
		})
	})

	Context("in descending order", func() {
		It("respects timestamp fudging when checking the time boundaries", func() {
			s = store.NewStore(50, sp, sm)

			e0 := buildEnvelope(0, "a")
			e1 := buildEnvelope(1, "a")
			e2 := buildEnvelope(2, "a")
			e3 := buildEnvelope(3, "a")

			s.Put(e0, e0.GetSourceId())
			s.Put(e0, e0.GetSourceId())

			s.Put(e1, e1.GetSourceId())

			s.Put(e2, e2.GetSourceId())
			s.Put(e2, e2.GetSourceId())
			s.Put(e2, e2.GetSourceId())

			s.Put(e3, e3.GetSourceId())
			s.Put(e3, e3.GetSourceId())
			s.Put(e3, e3.GetSourceId())
			s.Put(e3, e3.GetSourceId())
			s.Put(e3, e3.GetSourceId())

			start := time.Unix(0, 1)
			end := time.Unix(0, 3)
			envelopes := s.Get("a", start, end, nil, 5, true)
			Expect(envelopes).To(HaveLen(4))
			Expect(envelopes[0].GetTimestamp()).To(Equal(int64(2)))
			Expect(envelopes[1].GetTimestamp()).To(Equal(int64(2)))
			Expect(envelopes[2].GetTimestamp()).To(Equal(int64(2)))
			Expect(envelopes[3].GetTimestamp()).To(Equal(int64(1)))
		})

		It("intentionally exceeds the limit when it would otherwise break up a group of fudged timestamps", func() {
			s = store.NewStore(50, sp, sm)

			e0 := buildEnvelope(0, "a")
			e1 := buildEnvelope(1, "a")

			s.Put(e0, e0.GetSourceId())
			s.Put(e0, e0.GetSourceId())
			s.Put(e1, e1.GetSourceId())

			start := time.Unix(0, 0)
			end := time.Unix(0, 2)
			envelopes := s.Get("a", start, end, nil, 2, true)
			Expect(envelopes).To(HaveLen(3))
			Expect(envelopes[0].GetTimestamp()).To(Equal(int64(1)))
			Expect(envelopes[1].GetTimestamp()).To(Equal(int64(0)))
			Expect(envelopes[2].GetTimestamp()).To(Equal(int64(0)))
		})
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

	It("increments the timestamp as necessary to prevent overwrites", func() {
		e1 := buildEnvelope(1, "a")
		e2 := buildEnvelope(1, "a")
		e3 := buildEnvelope(3, "a")
		e4 := buildEnvelope(4, "a")

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())

		m := s.Meta()["a"]
		Expect(m.Count).To(Equal(int64(4)))
	})

	DescribeTable("fetches data based on envelope type",
		func(envelopeType logcache_v1.EnvelopeType, envelopeWrapper interface{}) {
			e1 := buildTypedEnvelope(1, "a", &loggregator_v2.Log{})
			e2 := buildTypedEnvelope(2, "a", &loggregator_v2.Counter{})
			e3 := buildTypedEnvelope(3, "a", &loggregator_v2.Gauge{})
			e4 := buildTypedEnvelope(4, "a", &loggregator_v2.Timer{})
			e5 := buildTypedEnvelope(5, "a", &loggregator_v2.Event{})

			s.Put(e1, e1.GetSourceId())
			s.Put(e2, e2.GetSourceId())
			s.Put(e3, e3.GetSourceId())
			s.Put(e4, e4.GetSourceId())
			s.Put(e5, e5.GetSourceId())

			start := time.Unix(0, 0)
			end := time.Unix(0, 9999)
			envelopes := s.Get("a", start, end, []logcache_v1.EnvelopeType{envelopeType}, 5, false)
			Expect(envelopes).To(HaveLen(1))
			Expect(envelopes[0].Message).To(BeAssignableToTypeOf(envelopeWrapper))

			// No Filter
			envelopes = s.Get("a", start, end, nil, 10, false)
			Expect(envelopes).To(HaveLen(5))
		},

		Entry("Log", logcache_v1.EnvelopeType_LOG, &loggregator_v2.Envelope_Log{}),
		Entry("Counter", logcache_v1.EnvelopeType_COUNTER, &loggregator_v2.Envelope_Counter{}),
		Entry("Gauge", logcache_v1.EnvelopeType_GAUGE, &loggregator_v2.Envelope_Gauge{}),
		Entry("Timer", logcache_v1.EnvelopeType_TIMER, &loggregator_v2.Envelope_Timer{}),
		Entry("Event", logcache_v1.EnvelopeType_EVENT, &loggregator_v2.Envelope_Event{}),
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
		s = store.NewStore(10, sp, sm)
		e1 := buildTypedEnvelope(0, "b", &loggregator_v2.Log{})
		s.Put(e1, e1.GetSourceId())
		sp.SetNumberToPrune(1000)
		Expect(func() { s.Put(e1, e1.GetSourceId()) }).ToNot(Panic())
	})

	It("truncates older envelopes when max size is reached", func() {
		s = store.NewStore(10, sp, sm)
		// e1 should be truncated and sourceID "b" should be forgotten.
		e1 := buildTypedEnvelope(1, "b", &loggregator_v2.Log{})
		// e2 should be truncated.
		e2 := buildTypedEnvelope(2, "a", &loggregator_v2.Counter{})

		// e3-e7 should be available
		e3 := buildTypedEnvelope(3, "a", &loggregator_v2.Gauge{})
		e4 := buildTypedEnvelope(3, "a", &loggregator_v2.Timer{})
		e5 := buildTypedEnvelope(3, "a", &loggregator_v2.Event{})
		e6 := buildTypedEnvelope(3, "a", &loggregator_v2.Event{})
		e7 := buildTypedEnvelope(3, "a", &loggregator_v2.Event{})

		// e8 should be truncated even though it is late
		e8 := buildTypedEnvelope(1, "a", &loggregator_v2.Event{})

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())
		s.Put(e5, e5.GetSourceId())
		s.Put(e6, e6.GetSourceId())
		s.Put(e7, e7.GetSourceId())
		s.Put(e8, e8.GetSourceId())

		s.WaitForTruncationToComplete()
		// Tell the spyPruner to remove 3 envelopes
		sp.SetNumberToPrune(3)

		// Wait until the next truncation cycle is complete
		s.WaitForTruncationToComplete()

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(5))

		for _, e := range envelopes {
			Expect(e.Timestamp).To(Equal(int64(3)))
		}

		Expect(sm.GetValue("Expired")).To(Equal(3.0))
		Expect(sm.GetValue("StoreSize")).To(Equal(5.0))

		// Ensure b was removed fully
		for s := range s.Meta() {
			Expect(s).To(Equal("a"))
		}
	})

	It("truncates envelopes for a specific source-id if its max size is reached", func() {
		s = store.NewStore(2, sp, sm)
		// e1 should not be truncated
		e1 := buildTypedEnvelope(1, "b", &loggregator_v2.Log{})
		// e2 should be truncated
		e2 := buildTypedEnvelope(2, "a", &loggregator_v2.Log{})
		e3 := buildTypedEnvelope(3, "a", &loggregator_v2.Log{})
		e4 := buildTypedEnvelope(4, "a", &loggregator_v2.Log{})

		s.Put(e1, e1.GetSourceId())
		s.Put(e2, e2.GetSourceId())
		s.Put(e3, e3.GetSourceId())
		s.Put(e4, e4.GetSourceId())

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)
		envelopes := s.Get("a", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(2))
		Expect(envelopes[0].Timestamp).To(Equal(int64(3)))
		Expect(envelopes[1].Timestamp).To(Equal(int64(4)))

		envelopes = s.Get("b", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(1))

		Expect(sm.GetValue("Expired")).To(Equal(1.0))
	})

	It("sets (via metrics) the store's period in milliseconds", func() {
		e := buildTypedEnvelope(time.Now().Add(-time.Minute).UnixNano(), "b", &loggregator_v2.Log{})
		s.Put(e, e.GetSourceId())

		Expect(sm.GetValue("CachePeriod")).To(BeNumerically("~", float64(time.Minute/time.Millisecond), 1000))
	})

	It("uses the given index", func() {
		s = store.NewStore(2, sp, sm)
		e := buildTypedEnvelope(0, "a", &loggregator_v2.Log{})
		s.Put(e, "some-id")

		start := time.Unix(0, 0)
		end := time.Unix(0, 9999)

		envelopes := s.Get("some-id", start, end, nil, 10, false)
		Expect(envelopes).To(HaveLen(1))
	})

	It("returns the indices in the store", func() {
		s = store.NewStore(2, sp, sm)

		// Will be pruned by pruner
		s.Put(buildTypedEnvelope(1, "index-0", &loggregator_v2.Log{}), "index-0")
		s.Put(buildTypedEnvelope(2, "index-1", &loggregator_v2.Log{}), "index-1")

		// Timestamp 2 should be pruned as we exceed the max per source of 2.
		s.Put(buildTypedEnvelope(3, "index-2", &loggregator_v2.Log{}), "index-2")
		s.Put(buildTypedEnvelope(4, "index-2", &loggregator_v2.Log{}), "index-2")
		s.Put(buildTypedEnvelope(5, "index-2", &loggregator_v2.Log{}), "index-2")

		s.Put(buildTypedEnvelope(6, "index-1", &loggregator_v2.Log{}), "index-1")

		// This truncation cycle will not remove any entries
		s.WaitForTruncationToComplete()

		sp.SetNumberToPrune(2)

		// This truncation cycle will prune first 2 entries (timestamp 1 and 2)
		s.WaitForTruncationToComplete()

		meta := s.Meta()

		// Does not contain index-0
		Expect(meta).To(HaveLen(2))

		Expect(meta).To(HaveKeyWithValue("index-1", logcache_v1.MetaInfo{
			Count:           1,
			Expired:         1,
			OldestTimestamp: 6,
			NewestTimestamp: 6,
		}))

		Expect(meta).To(HaveKeyWithValue("index-2", logcache_v1.MetaInfo{
			Count:           2,
			Expired:         1,
			OldestTimestamp: 4,
			NewestTimestamp: 5,
		}))
	})

	It("survives the just added entry from being pruned", func() {
		s = store.NewStore(2, sp, sm)

		s.Put(buildTypedEnvelope(2, "index-0", &loggregator_v2.Log{}), "index-0")
		s.Put(buildTypedEnvelope(3, "index-0", &loggregator_v2.Log{}), "index-0")
		s.Put(buildTypedEnvelope(1, "index-1", &loggregator_v2.Log{}), "index-1")

		s.WaitForTruncationToComplete()
		sp.SetNumberToPrune(1)
		s.WaitForTruncationToComplete()

		Expect(s.Meta()).ToNot(HaveKey("index-1"))
	})

	// TODO: This is probably duplicated in the store_load_test, but it was
	// really useful in driving out races. We'd like to leave this in place
	// until we have a high level of confidence that we're catching races
	It("demonstrates thread safety under heavy concurrent load", func() {
		sp := newSpyPruner()
		sp.SetNumberToPrune(10)
		loadStore := store.NewStore(10000, sp, sm)
		start := time.Now()

		for i := 0; i < 10; i++ {
			for j := 0; j < 10; j++ {
				go func(sourceId string) {
					for i := 0; i < 10000; i++ {
						e := buildTypedEnvelope(time.Now().UnixNano(), sourceId, &loggregator_v2.Log{})
						loadStore.Put(e, sourceId)
						time.Sleep(time.Millisecond)
					}
				}(strconv.Itoa(j))
			}
		}

		Consistently(func() int64 {
			envelopes := loadStore.Get("9", start, time.Now(), nil, 100000, false)
			time.Sleep(500 * time.Millisecond)
			return int64(len(envelopes))
		}).Should(BeNumerically("<=", 10000))
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
	sync.Mutex
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		values: make(map[string]float64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(delta uint64) {
	return func(d uint64) {
		s.Lock()
		defer s.Unlock()
		s.values[name] += float64(d)
	}
}

func (s *spyMetrics) NewGauge(name string) func(value float64) {
	return func(v float64) {
		s.Lock()
		defer s.Unlock()

		s.values[name] = v
	}
}

func (s *spyMetrics) GetValue(name string) float64 {
	s.Lock()
	defer s.Unlock()

	return s.values[name]
}

type spyPruner struct {
	numberToPrune int
	sync.Mutex
}

func newSpyPruner() *spyPruner {
	return &spyPruner{}
}

func (sp *spyPruner) SetNumberToPrune(numberToPrune int) {
	sp.Lock()
	defer sp.Unlock()

	sp.numberToPrune = numberToPrune
}

func (sp *spyPruner) GetQuantityToPrune(int64) int {
	sp.Lock()
	defer sp.Unlock()

	return sp.numberToPrune
}

func (sp *spyPruner) SetMemoryReporter(_ func(float64)) {}
