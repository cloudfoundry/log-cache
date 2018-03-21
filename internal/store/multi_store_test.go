package store_test

import (
	"strconv"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("MultiStore", func() {
	var (
		spySubStores []*spySubStore

		s *store.MultiStore
	)

	BeforeEach(func() {
		spySubStores = nil

		creator := func() store.SubStore {
			spy := newSpySubStore()
			spySubStores = append(spySubStores, spy)
			return spy
		}

		hasher := func(s string) uint64 {
			x, err := strconv.Atoi(s)
			if err != nil {
				panic(err)
			}

			return uint64(x)
		}

		s = store.NewMultiStore(creator, hasher)

		s.SetRanges([]*logcache_v1.Range{
			{Start: 0, End: 10},
			{Start: 11, End: 20},
			{Start: 21, End: 30},
		})
	})

	It("creates a SubStore for each range and does not recreate for existing ranges", func() {
		s.SetRanges([]*logcache_v1.Range{
			{Start: 0, End: 10},
			{Start: 11, End: 20},
			{Start: 21, End: 30},
			{Start: 31, End: 40},
		})

		Expect(spySubStores).To(HaveLen(4))
	})

	It("closes a SubStore when the corresponding range is removed", func() {
		s.SetRanges([]*logcache_v1.Range{
			{Start: 11, End: 20},
			{Start: 21, End: 30},
		})

		Expect(spySubStores).To(HaveLen(3))
		Expect(spySubStores[0].closed).To(BeTrue())
	})

	It("puts data into the corresponding SubStore", func() {
		s.Put(&loggregator_v2.Envelope{Timestamp: 0}, "0")
		s.Put(&loggregator_v2.Envelope{Timestamp: 25}, "25")
		s.Put(&loggregator_v2.Envelope{Timestamp: 30}, "30")

		Expect(spySubStores[0].puts).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 0},
		))

		Expect(spySubStores[2].puts).To(ConsistOf(
			&loggregator_v2.Envelope{Timestamp: 25},
			&loggregator_v2.Envelope{Timestamp: 30},
		))
	})

	It("gets data from the corresponding SubStore", func() {
		spySubStores[0].getResults = []*loggregator_v2.Envelope{
			{Timestamp: 0},
		}
		spySubStores[2].getResults = []*loggregator_v2.Envelope{
			{Timestamp: 25},
			{Timestamp: 30},
		}

		r1 := s.Get("0", time.Unix(0, 1), time.Unix(0, 2), []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_EVENT}, 99, true)
		r2 := s.Get("25", time.Unix(0, 1), time.Unix(0, 2), []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_EVENT}, 99, true)
		r3 := s.Get("30", time.Unix(0, 1), time.Unix(0, 2), []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_EVENT}, 99, true)

		Expect(r1).To(ConsistOf(&loggregator_v2.Envelope{Timestamp: 0}))
		Expect(r2).To(ContainElement(&loggregator_v2.Envelope{Timestamp: 25}))
		Expect(r3).To(ContainElement(&loggregator_v2.Envelope{Timestamp: 30}))

		Expect(spySubStores[0].getIndices).To(ConsistOf("0"))
		Expect(spySubStores[0].getStarts).To(ConsistOf(time.Unix(0, 1)))
		Expect(spySubStores[0].getEnds).To(ConsistOf(time.Unix(0, 2)))
		Expect(spySubStores[0].getEnvelopeTypes[0]).To(ConsistOf(logcache_v1.EnvelopeType_EVENT))
		Expect(spySubStores[0].getLimits).To(ConsistOf(99))
		Expect(spySubStores[0].getDescending).To(ConsistOf(true))

		Expect(spySubStores[2].getIndices).To(ConsistOf("25", "30"))
	})

	It("does not fetch from a SubStore that does not have a corresponding range", func() {
		s.SetRanges([]*logcache_v1.Range{
			{Start: 11, End: 20},
			{Start: 21, End: 30},
		})

		spySubStores[0].getResults = []*loggregator_v2.Envelope{
			{Timestamp: 0},
		}

		r1 := s.Get("0", time.Unix(0, 1), time.Unix(0, 2), []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_EVENT}, 99, true)
		Expect(r1).To(HaveLen(0))
		Expect(spySubStores[0].getIndices).To(HaveLen(0))
	})

	It("fetches meta from all the stores", func() {
		spySubStores[0].metaResults = map[string]logcache_v1.MetaInfo{
			"a": logcache_v1.MetaInfo{Count: 99},
			"b": logcache_v1.MetaInfo{Count: 100},
		}
		spySubStores[1].metaResults = map[string]logcache_v1.MetaInfo{
			"c": logcache_v1.MetaInfo{Count: 101},
			"d": logcache_v1.MetaInfo{Count: 102},
		}

		m := s.Meta()

		Expect(m).To(HaveKeyWithValue("a", logcache_v1.MetaInfo{Count: 99}))
		Expect(m).To(HaveKeyWithValue("b", logcache_v1.MetaInfo{Count: 100}))
		Expect(m).To(HaveKeyWithValue("c", logcache_v1.MetaInfo{Count: 101}))
		Expect(m).To(HaveKeyWithValue("d", logcache_v1.MetaInfo{Count: 102}))
	})
})

type spySubStore struct {
	puts   []*loggregator_v2.Envelope
	closed bool

	getIndices       []string
	getStarts        []time.Time
	getEnds          []time.Time
	getEnvelopeTypes [][]logcache_v1.EnvelopeType
	getLimits        []int
	getDescending    []bool
	getResults       []*loggregator_v2.Envelope

	metaResults map[string]logcache_v1.MetaInfo
}

func newSpySubStore() *spySubStore {
	return &spySubStore{}
}

func (s *spySubStore) Get(index string, start time.Time, end time.Time, envelopeTypes []logcache_v1.EnvelopeType, limit int, descending bool) []*loggregator_v2.Envelope {
	s.getIndices = append(s.getIndices, index)
	s.getStarts = append(s.getStarts, start)
	s.getEnds = append(s.getEnds, end)
	s.getEnvelopeTypes = append(s.getEnvelopeTypes, envelopeTypes)
	s.getLimits = append(s.getLimits, limit)
	s.getDescending = append(s.getDescending, descending)
	return s.getResults
}

func (s *spySubStore) Meta() map[string]logcache_v1.MetaInfo {
	return s.metaResults
}

func (s *spySubStore) Put(e *loggregator_v2.Envelope, index string) {
	s.puts = append(s.puts, e)
}

func (s *spySubStore) Close() error {
	s.closed = true
	return nil
}
