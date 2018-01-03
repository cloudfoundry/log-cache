package groups_test

import (
	"log"
	"math/rand"
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
		s         *groups.Storage
		spyReader *spyReader
	)

	BeforeEach(func() {
		spyReader = newSpyReader()
		s = groups.NewStorage(5, 100*time.Millisecond, spyReader.Read, log.New(GinkgoWriter, "", 0))
	})

	It("returns data sorted by timestamp", func() {
		s.Add("a")
		s.Add("b")

		spyReader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: rand.Int63(), SourceId: "a"},
		})

		spyReader.addEnvelopes("b", []*loggregator_v2.Envelope{
			{Timestamp: rand.Int63(), SourceId: "b"},
		})

		var ts []int64

		Eventually(func() []string {
			var r []string
			for _, e := range s.Next() {
				r = append(r, e.GetSourceId())
				ts = append(ts, e.GetTimestamp())
			}
			return r
		}).Should(And(ContainElement("a"), ContainElement("b")))

		Expect(sort.IsSorted(int64s(ts))).To(BeTrue())
	})

	It("removes producer", func() {
		s.Add("a")
		s.Remove("a")

		spyReader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: rand.Int63(), SourceId: "a"},
		})

		Eventually(s.Next).Should(BeEmpty())
		Consistently(s.Next).Should(BeEmpty())
	})

	It("does not return any envelopes after being closed", func() {
		s.Add("a")
		s.Add("b")
		s.Close()

		spyReader.addEnvelopes("a", []*loggregator_v2.Envelope{
			{Timestamp: rand.Int63(), SourceId: "a"},
		})

		Eventually(s.Next).Should(BeEmpty())
		Consistently(s.Next).Should(BeEmpty())
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

func (s *spyReader) Read(sourceID string, start time.Time, opts ...gologcache.ReadOption) ([]*loggregator_v2.Envelope, error) {
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
