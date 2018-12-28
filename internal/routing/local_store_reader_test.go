package routing_test

import (
	"regexp"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"golang.org/x/net/context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LocalStoreReader", func() {
	var (
		r              *routing.LocalStoreReader
		spyStoreReader *spyStoreReader
	)

	BeforeEach(func() {
		spyStoreReader = newSpyStoreReader()
		r = routing.NewLocalStoreReader(
			spyStoreReader,
		)
	})

	// TODO: add nameFiltering tests
	It("reads envelopes from the store", func() {
		spyStoreReader.getEnvelopes = []*loggregator_v2.Envelope{
			{Timestamp: 1},
			{Timestamp: 2},
		}
		resp, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     99,
			EndTime:       100,
			Limit:         101,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_LOG},
			Descending:    true,
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(resp.Envelopes.Batch).To(HaveLen(2))
		Expect(spyStoreReader.sourceID).To(Equal("some-source"))
		Expect(spyStoreReader.start.UnixNano()).To(Equal(int64(99)))
		Expect(spyStoreReader.end.UnixNano()).To(Equal(int64(100)))
		Expect(spyStoreReader.envelopeTypes).To(ConsistOf(logcache_v1.EnvelopeType_LOG))
		Expect(spyStoreReader.limit).To(Equal(101))
		Expect(spyStoreReader.descending).To(BeTrue())
	})

	It("does not set the envelope type for an ANY", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     99,
			EndTime:       100,
			Limit:         101,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(spyStoreReader.envelopeTypes).To(BeNil())
	})

	It("defaults StartTime to 0, EndTime to now, limit to 100 and EnvelopeType to ANY", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId: "some-source",
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(spyStoreReader.sourceID).To(Equal("some-source"))
		Expect(spyStoreReader.start.UnixNano()).To(Equal(int64(0)))
		Expect(spyStoreReader.end.UnixNano()).To(BeNumerically("~", time.Now().UnixNano(), time.Second))
		Expect(spyStoreReader.envelopeTypes).To(BeNil())
		Expect(spyStoreReader.limit).To(Equal(100))
	})

	It("sets the limit to the default of 100 if a limit of zero is provided", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     99,
			EndTime:       100,
			Limit:         0,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(spyStoreReader.limit).To(Equal(100))
	})

	It("validates that the name_filter param is passed through to the store", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     99,
			EndTime:       100,
			NameFilter:    ".*foo.*",
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(spyStoreReader.nameFilter.String()).To(Equal(".*foo.*"))
	})

	It("returns an error if the end time is before the start time", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     100,
			EndTime:       99,
			Limit:         101,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).To(HaveOccurred())

		// Don't return an error if end is left out
		_, err = r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     100,
			Limit:         101,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).ToNot(HaveOccurred())
	})

	It("returns an error if the limit is greater than 1000", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     99,
			EndTime:       100,
			Limit:         1001,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).To(HaveOccurred())
	})

	It("returns an error if the limit is less than 0", func() {
		_, err := r.Read(context.Background(), &logcache_v1.ReadRequest{
			SourceId:      "some-source",
			StartTime:     99,
			EndTime:       100,
			Limit:         -1,
			EnvelopeTypes: []logcache_v1.EnvelopeType{logcache_v1.EnvelopeType_ANY},
		})
		Expect(err).To(HaveOccurred())
	})

	It("returns local source IDs from the store", func() {
		spyStoreReader.metaResponse = map[string]logcache_v1.MetaInfo{
			"source-1": {
				Count:           1,
				Expired:         2,
				OldestTimestamp: 3,
				NewestTimestamp: 4,
			},
			"source-2": {
				Count:           5,
				Expired:         6,
				OldestTimestamp: 7,
				NewestTimestamp: 8,
			},
		}

		metaInfo, err := r.Meta(context.Background(), &logcache_v1.MetaRequest{
			LocalOnly: true,
		})
		Expect(err).ToNot(HaveOccurred())

		Expect(metaInfo).To(Equal(&logcache_v1.MetaResponse{
			Meta: map[string]*logcache_v1.MetaInfo{
				"source-1": {
					Count:           1,
					Expired:         2,
					OldestTimestamp: 3,
					NewestTimestamp: 4,
				},
				"source-2": {
					Count:           5,
					Expired:         6,
					OldestTimestamp: 7,
					NewestTimestamp: 8,
				},
			},
		}))
	})
})

type spyStoreReader struct {
	getEnvelopes []*loggregator_v2.Envelope

	sourceID      string
	start         time.Time
	end           time.Time
	envelopeTypes []logcache_v1.EnvelopeType
	limit         int
	descending    bool
	nameFilter    *regexp.Regexp
	metaResponse  map[string]logcache_v1.MetaInfo
}

func newSpyStoreReader() *spyStoreReader {
	return &spyStoreReader{}
}

func (s *spyStoreReader) Get(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeTypes []logcache_v1.EnvelopeType,
	nameFilter *regexp.Regexp,
	limit int,
	descending bool,
) []*loggregator_v2.Envelope {
	s.sourceID = sourceID
	s.start = start
	s.end = end
	s.envelopeTypes = envelopeTypes
	s.nameFilter = nameFilter
	s.limit = limit
	s.descending = descending

	return s.getEnvelopes
}

func (s *spyStoreReader) Meta() map[string]logcache_v1.MetaInfo {
	return s.metaResponse
}
