package groups_test

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	gologcache "code.cloudfoundry.org/go-log-cache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/groups"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Storage", func() {
	var (
		s          *groups.Storage
		spyMetrics *spyMetrics
		reader     *spyReader
		spyPruner  *spyPruner
	)

	BeforeEach(func() {
		spyMetrics = newSpyMetrics()
		reader = newSpyReader()
		spyPruner = newSpyPruner()
		s = groups.NewStorage(reader.Read, time.Millisecond, spyPruner, spyMetrics, log.New(GinkgoWriter, "", 0))
	})

	It("returns data sorted by timestamp", func() {
		reader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: 99, SourceId: "a"},
			{Timestamp: 101, SourceId: "c"},
			{Timestamp: 103, SourceId: "a"},
			{Timestamp: 104, SourceId: "c"},
		})

		reader.addEnvelopes("b", []*loggregator_v2.Envelope{
			{Timestamp: 100, SourceId: "b"},
			{Timestamp: 102, SourceId: "b"},
			{Timestamp: 104, SourceId: "b"},
		})

		s.AddRequester("some-name", 0, false)
		s.Add("some-name", []string{"a", "c"})
		s.Add("some-name", []string{"b"})
		// Ensure we don't "clear" the existing state
		s.AddRequester("some-name", 0, false)

		var ts []int64

		Eventually(func() []string {
			var r []string
			ts = nil
			// [100, 104)
			for _, e := range s.Get("some-name", time.Unix(0, 100), time.Unix(0, 104), nil, 100, false, 0) {
				r = append(r, e.GetSourceId())
				ts = append(ts, e.GetTimestamp())
			}

			// [100, 104)
			if len(ts) != 4 {
				return nil
			}

			return r
		}).Should(And(ContainElement("a"), ContainElement("b"), ContainElement("c")))

		Expect(sort.IsSorted(int64s(ts))).To(BeTrue())
	})

	It("does not read for remoteOnly requesters", func() {
		reader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: 99, SourceId: "a"},
			{Timestamp: 101, SourceId: "a"},
			{Timestamp: 103, SourceId: "a"},
		})

		reader.addEnvelopes("b", []*loggregator_v2.Envelope{
			{Timestamp: 100, SourceId: "b"},
			{Timestamp: 102, SourceId: "b"},
			{Timestamp: 104, SourceId: "b"},
		})

		s.Add("some-name", []string{"a"})
		s.Add("some-name", []string{"b"})
		s.AddRequester("some-name", 0, true)
		s.AddRequester("some-name", 1, false)

		Eventually(reader.SourceIDs).ShouldNot(BeEmpty())
		Consistently(reader.SourceIDs).Should(
			Or(
				ContainElement("a"),
				ContainElement("b"),
			),
		)

		Expect(reader.SourceIDs()).ToNot(
			And(
				ContainElement("a"),
				ContainElement("b"),
			),
		)
	})

	It("removes producer", func() {
		reader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: 99, SourceId: "a"},
		})

		s.Add("some-name", []string{"a"})
		s.Remove("some-name", []string{"a"})
		s.AddRequester("some-name", 0, false)

		f := func() []*loggregator_v2.Envelope {
			return s.Get("some-name", time.Unix(0, 100), time.Unix(0, 101), nil, 100, false, 0)
		}()

		Eventually(f).Should(BeEmpty())
		Consistently(f).Should(BeEmpty())
	})

	It("shards data by source ID", func() {
		s.AddRequester("some-name", 0, false)
		s.AddRequester("some-name", 1, false)
		s.Add("some-name", []string{"a", "c"})
		s.Add("some-name", []string{"b"})

		go func(reader *spyReader) {
			for range time.Tick(time.Millisecond) {
				reader.addEnvelopes("a", []*loggregator_v2.Envelope{
					{Timestamp: 99, SourceId: "a"},
					{Timestamp: 101, SourceId: "c"},
					{Timestamp: 103, SourceId: "a"},
					{Timestamp: 104, SourceId: "c"},
				})

				reader.addEnvelopes("b", []*loggregator_v2.Envelope{
					{Timestamp: 100, SourceId: "b"},
					{Timestamp: 102, SourceId: "b"},
					{Timestamp: 104, SourceId: "b"},
				})
			}
		}(reader)

		Eventually(func() []string {
			var r []string
			// [100, 104)
			for _, e := range s.Get(
				"some-name",
				time.Unix(0, 100),
				time.Unix(0, 104),
				nil,   // any envelope type
				100,   // limit
				false, // ascending
				1,     // requesterID
			) {
				r = append(r, e.GetSourceId())
			}
			return r
		}).Should(
			And(ContainElement("b"), Not(ContainElement("a"))),
		)

		Eventually(func() []string {
			var r []string
			// [100, 104)
			for _, e := range s.Get(
				"some-name",
				time.Unix(0, 100),
				time.Unix(0, 104),
				nil,   // any envelope type
				100,   // limit
				false, // ascending
				0,     // requesterID
			) {
				r = append(r, e.GetSourceId())
			}
			return r
		}).Should(
			And(
				ContainElement("a"),
				Not(ContainElement("b")),
				ContainElement("c"),
			),
		)
	})

	It("ensures each source ID is serviced when a requester is removed", func() {
		s.Add("some-name", []string{"a"})
		s.Add("some-name", []string{"b"})
		s.AddRequester("some-name", 0, false)
		s.AddRequester("some-name", 1, false)
		s.RemoveRequester("some-name", 1)

		reader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: 99, SourceId: "a"},
			{Timestamp: 101, SourceId: "a"},
			{Timestamp: 103, SourceId: "a"},
		})

		reader.addEnvelopes("b", []*loggregator_v2.Envelope{
			{Timestamp: 100, SourceId: "b"},
			{Timestamp: 102, SourceId: "b"},
			{Timestamp: 104, SourceId: "b"},
		})

		Eventually(func() []string {
			// [100, 104)
			s.Get("some-name", time.Unix(0, 100), time.Unix(0, 104), nil, 100, false, 0)
			return reader.truncateSourceIDs()
		}).Should(
			And(ContainElement("a"), ContainElement("b")),
		)
	})
})

type spyReader struct {
	mu        sync.Mutex
	sourceIDs []string
	results   map[string][][]*loggregator_v2.Envelope
}

func newSpyReader() *spyReader {
	return &spyReader{
		results: make(map[string][][]*loggregator_v2.Envelope),
	}
}

func (s *spyReader) addEnvelopes(sourceID string, es []*loggregator_v2.Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.results[sourceID] = append(s.results[sourceID], es)
}

func (s *spyReader) Read(
	_ context.Context,
	sourceID string,
	start time.Time,
	opts ...gologcache.ReadOption,
) ([]*loggregator_v2.Envelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sourceIDs = append(s.sourceIDs, sourceID)

	r := s.results[sourceID]
	if len(r) == 0 {
		return nil, nil
	}

	s.results[sourceID] = r[1:]

	return r[0], nil
}

func (s *spyReader) SourceIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]string, len(s.sourceIDs))
	copy(r, s.sourceIDs)
	return r
}

func (s *spyReader) truncateSourceIDs() []string {
	result := s.SourceIDs()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sourceIDs = nil
	return result
}

type int64s []int64

func (i int64s) Len() int {
	return len(i)
}

func (i int64s) Less(a, b int) bool {
	return i[a] < i[b]
}

func (i int64s) Swap(a, b int) {
	tmp := i[a]
	i[a] = i[b]
	i[b] = tmp
}

type spyMetrics struct {
	mu     sync.Mutex
	values map[string]float64
}

func newSpyMetrics() *spyMetrics {
	return &spyMetrics{
		values: make(map[string]float64),
	}
}

func (s *spyMetrics) NewCounter(name string) func(delta uint64) {
	return func(d uint64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.values[name] += float64(d)
	}
}

func (s *spyMetrics) NewGauge(name string) func(value float64) {
	return func(v float64) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.values[name] = v
	}
}

type spyPruner struct {
	size int
}

func newSpyPruner() *spyPruner {
	return &spyPruner{}
}

func (s *spyPruner) Prune() int {
	s.size++
	if s.size > 5 {
		return s.size - 5
	}

	return 0
}
