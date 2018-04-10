package replication_test

import (
	"log"
	"sync"
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store/replication"
	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Store", func() {
	var (
		spySubStore        *spySubStore
		spyApplier         *spyApplier
		spyMarshallerCache *spyMarshallerCache
		spyCloser          *spyCloser
		s                  *replication.Store
	)

	BeforeEach(func() {
		spySubStore = newSpySubStore()
		spyApplier = newSpyApplier()
		spyMarshallerCache = newSpyMarshallerCache()
		spyCloser = newSpyCloser()
		s = replication.NewStore(spySubStore, spyApplier, spyMarshallerCache, spyCloser.Close, log.New(GinkgoWriter, "", 0))
	})

	It("passes Get requests through to the SubStore", func() {
		expectedResults := []*loggregator_v2.Envelope{
			{Timestamp: 99},
			{Timestamp: 100},
		}

		spySubStore.getResults = [][]*loggregator_v2.Envelope{
			expectedResults,
		}
		es := s.Get(
			"some-index",
			time.Unix(0, 1),
			time.Unix(0, 2),
			[]logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_LOG},
			99,
			true,
		)

		Expect(es).To(Equal(expectedResults))

		Expect(spySubStore.getIndices).To(HaveLen(1))
		Expect(spySubStore.getIndices[0]).To(Equal("some-index"))
		Expect(spySubStore.getStarts[0]).To(Equal(time.Unix(0, 1)))
		Expect(spySubStore.getEnds[0]).To(Equal(time.Unix(0, 2)))
		Expect(spySubStore.getEnvelopeTypes[0]).To(Equal(
			[]logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_LOG},
		))
		Expect(spySubStore.getLimits[0]).To(Equal(99))
		Expect(spySubStore.getDescendings[0]).To(Equal(true))
	})

	It("passes Meta results from the SubStore", func() {
		spySubStore.metaResults = map[string]logcache_v1.MetaInfo{
			"a": logcache_v1.MetaInfo{Count: 99},
		}

		Expect(s.Meta()).To(Equal(spySubStore.metaResults))
	})

	It("puts data into the applier and the cache", func() {
		s.Put(&loggregator_v2.Envelope{Timestamp: 99, SourceId: "a"}, "some-index")

		Expect(spyApplier.cmds).To(HaveLen(1))
		Expect(spyApplier.timeouts).To(ConsistOf(time.Second))

		var e loggregator_v2.Envelope
		Expect(proto.Unmarshal(spyApplier.cmds[0], &e)).To(Succeed())
		Expect(e.Timestamp).To(Equal(int64(99)))

		Expect(proto.Unmarshal(spyMarshallerCache.putBytes[0], &e)).To(Succeed())
		Expect(spyMarshallerCache.putEnvelopes[0]).To(Equal(&e))

		Expect(spySubStore.putEnvelopes).To(ConsistOf(&loggregator_v2.Envelope{Timestamp: 99, SourceId: "a"}))
		Expect(spySubStore.putIndices).To(ConsistOf("a"))
	})

	It("closer should be invoked on close", func() {
		s.Close()
		Expect(spyCloser.called).To(BeTrue())
	})
})

type spySubStore struct {
	mu           sync.Mutex
	putEnvelopes []*loggregator_v2.Envelope
	putIndices   []string

	getIndices       []string
	getStarts        []time.Time
	getEnds          []time.Time
	getEnvelopeTypes [][]logcache_v1.EnvelopeType
	getLimits        []int
	getDescendings   []bool
	getResults       [][]*loggregator_v2.Envelope

	metaResults map[string]logcache_v1.MetaInfo
}

func newSpySubStore() *spySubStore {
	return &spySubStore{}
}

func (s *spySubStore) Put(e *loggregator_v2.Envelope, index string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.putEnvelopes = append(s.putEnvelopes, e)
	s.putIndices = append(s.putIndices, index)
}

func (s *spySubStore) PutEnvelopes() []*loggregator_v2.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*loggregator_v2.Envelope, len(s.putEnvelopes))
	copy(r, s.putEnvelopes)

	return r
}

func (s *spySubStore) PutIndices() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]string, len(s.putIndices))
	copy(r, s.putIndices)

	return r
}

func (s *spySubStore) Get(
	index string,
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	s.getIndices = append(s.getIndices, index)
	s.getStarts = append(s.getStarts, start)
	s.getEnds = append(s.getEnds, end)
	s.getEnvelopeTypes = append(s.getEnvelopeTypes, envelopeTypes)
	s.getLimits = append(s.getLimits, limit)
	s.getDescendings = append(s.getDescendings, descending)

	if len(s.getResults) == 0 {
		return nil
	}

	r := s.getResults[0]
	s.getResults = s.getResults[1:]
	return r
}

func (s *spySubStore) Meta() map[string]logcache_v1.MetaInfo {
	return s.metaResults
}

func (s *spySubStore) Close() error {
	return nil
}

type spyApplier struct {
	cmds     [][]byte
	timeouts []time.Duration
}

func newSpyApplier() *spyApplier {
	return &spyApplier{}
}

func (s *spyApplier) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	s.cmds = append(s.cmds, cmd)
	s.timeouts = append(s.timeouts, timeout)
	return nil
}

type spyMarshallerCache struct {
	putBytes     [][]byte
	putEnvelopes []*loggregator_v2.Envelope

	getBytes  [][]byte
	getResult *loggregator_v2.Envelope
}

func newSpyMarshallerCache() *spyMarshallerCache {
	return &spyMarshallerCache{}
}

func (s *spyMarshallerCache) Put(b []byte, e *loggregator_v2.Envelope) {
	s.putBytes = append(s.putBytes, b)
	s.putEnvelopes = append(s.putEnvelopes, e)
}

func (s *spyMarshallerCache) Get(b []byte) *loggregator_v2.Envelope {
	s.getBytes = append(s.getBytes, b)
	return s.getResult
}
