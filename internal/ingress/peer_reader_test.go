package ingress_test

import (
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/ingress"
	"code.cloudfoundry.org/log-cache/internal/store"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("PeerReader", func() {
	var (
		r                *ingress.PeerReader
		spyEnvelopeStore *spyEnvelopeStore
	)

	BeforeEach(func() {
		spyEnvelopeStore = newSpyEnvelopeStore()
		r = ingress.NewPeerReader(
			spyEnvelopeStore.Put,
			spyEnvelopeStore,
		)
	})

	It("writes the envelope to the store", func() {
		resp, err := r.Send(context.Background(), &logcache.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{Timestamp: 1},
					{Timestamp: 2},
				},
			},
		})

		Expect(resp).ToNot(BeNil())
		Expect(err).ToNot(HaveOccurred())
		Expect(spyEnvelopeStore.putEnvelopes).To(HaveLen(2))
		Expect(spyEnvelopeStore.putEnvelopes[0].Timestamp).To(Equal(int64(1)))
		Expect(spyEnvelopeStore.putEnvelopes[1].Timestamp).To(Equal(int64(2)))
	})

	It("reads envelopes from the store", func() {
		spyEnvelopeStore.getEnvelopes = []*loggregator_v2.Envelope{
			{Timestamp: 1},
			{Timestamp: 2},
		}
		resp, err := r.Read(context.Background(), &logcache.ReadRequest{
			SourceId:     "some-source",
			StartTime:    99,
			EndTime:      100,
			Limit:        101,
			EnvelopeType: logcache.EnvelopeTypes_LOG,
			Descending:   true,
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.Envelopes.Batch).To(HaveLen(2))
		Expect(spyEnvelopeStore.sourceID).To(Equal("some-source"))
		Expect(spyEnvelopeStore.start.UnixNano()).To(Equal(int64(99)))
		Expect(spyEnvelopeStore.end.UnixNano()).To(Equal(int64(100)))
		Expect(spyEnvelopeStore.envelopeType).To(Equal(&loggregator_v2.Log{}))
		Expect(spyEnvelopeStore.limit).To(Equal(101))
		Expect(spyEnvelopeStore.descending).To(BeTrue())
	})

	DescribeTable("envelope types", func(t logcache.EnvelopeTypes, expected store.EnvelopeType) {
		_, err := r.Read(context.Background(), &logcache.ReadRequest{
			SourceId:     "some-source",
			StartTime:    99,
			EndTime:      100,
			Limit:        101,
			EnvelopeType: t,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(spyEnvelopeStore.envelopeType).To(Equal(expected))
	},
		Entry("log", logcache.EnvelopeTypes_LOG, &loggregator_v2.Log{}),
		Entry("counter", logcache.EnvelopeTypes_COUNTER, &loggregator_v2.Counter{}),
		Entry("gauge", logcache.EnvelopeTypes_GAUGE, &loggregator_v2.Gauge{}),
		Entry("timer", logcache.EnvelopeTypes_TIMER, &loggregator_v2.Timer{}),
		Entry("event", logcache.EnvelopeTypes_EVENT, &loggregator_v2.Event{}))

	It("does not set the envelope type for an ANY", func() {
		_, err := r.Read(context.Background(), &logcache.ReadRequest{
			SourceId:     "some-source",
			StartTime:    99,
			EndTime:      100,
			Limit:        101,
			EnvelopeType: logcache.EnvelopeTypes_ANY,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(spyEnvelopeStore.envelopeType).To(BeNil())
	})

	It("defaults StartTime to 0, EndTime to now, limit to 100 and EnvelopeType to ANY", func() {
		_, err := r.Read(context.Background(), &logcache.ReadRequest{
			SourceId: "some-source",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyEnvelopeStore.sourceID).To(Equal("some-source"))
		Expect(spyEnvelopeStore.start.UnixNano()).To(Equal(int64(0)))
		Expect(spyEnvelopeStore.end.UnixNano()).To(BeNumerically("~", time.Now().UnixNano(), time.Second))
		Expect(spyEnvelopeStore.envelopeType).To(BeNil())
		Expect(spyEnvelopeStore.limit).To(Equal(100))
	})

	It("returns an error if the end time is before the start time", func() {
		_, err := r.Read(context.Background(), &logcache.ReadRequest{
			SourceId:     "some-source",
			StartTime:    100,
			EndTime:      99,
			Limit:        101,
			EnvelopeType: logcache.EnvelopeTypes_ANY,
		})
		Expect(err).To(HaveOccurred())

		// Don't return an error if end is left out
		_, err = r.Read(context.Background(), &logcache.ReadRequest{
			SourceId:     "some-source",
			StartTime:    100,
			Limit:        101,
			EnvelopeType: logcache.EnvelopeTypes_ANY,
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns an error if the limit is greater than 1000", func() {
		_, err := r.Read(context.Background(), &logcache.ReadRequest{
			SourceId:     "some-source",
			StartTime:    99,
			EndTime:      100,
			Limit:        1001,
			EnvelopeType: logcache.EnvelopeTypes_ANY,
		})
		Expect(err).To(HaveOccurred())
	})

	It("returns local source IDs from the store", func() {
		spyEnvelopeStore.metaResponse = []string{
			"source-1",
			"source-2",
		}

		metaInfo, err := r.Meta(context.Background(), &logcache.MetaRequest{
			LocalOnly: true,
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(spyEnvelopeStore.metaLocalOnly).To(BeTrue())

		Expect(metaInfo).To(Equal(&logcache.MetaResponse{
			Meta: map[string]*logcache.MetaInfo{
				"source-1": &logcache.MetaInfo{},
				"source-2": &logcache.MetaInfo{},
			},
		}))
	})
})

type spyEnvelopeStore struct {
	putEnvelopes []*loggregator_v2.Envelope
	getEnvelopes []*loggregator_v2.Envelope

	sourceID      string
	start         time.Time
	end           time.Time
	envelopeType  store.EnvelopeType
	limit         int
	descending    bool
	metaLocalOnly bool
	metaResponse  []string
}

func newSpyEnvelopeStore() *spyEnvelopeStore {
	return &spyEnvelopeStore{}
}

func (s *spyEnvelopeStore) Put(e *loggregator_v2.Envelope) {
	s.putEnvelopes = append(s.putEnvelopes, e)
}

func (s *spyEnvelopeStore) Get(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType store.EnvelopeType,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	s.sourceID = sourceID
	s.start = start
	s.end = end
	s.envelopeType = envelopeType
	s.limit = limit
	s.descending = descending

	return s.getEnvelopes
}

func (s *spyEnvelopeStore) Meta(localOnly bool) []string {
	s.metaLocalOnly = localOnly
	return s.metaResponse
}
