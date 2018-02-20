package store_test

import (
	"errors"
	"time"

	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("ProxyStore", func() {
	var (
		remotes      map[int]rpc.EgressClient
		lookup       *spyLookup
		local        *spyLocalStore
		egressClient *spyEgressClient

		remoteIndex = 0
		localIndex  = 1

		proxy *store.ProxyStore
	)

	BeforeEach(func() {
		localIndex := 1
		local = newSpyGetter()
		lookup = newSpyLookup()
		egressClient = newSpyEgressClient()
		remotes = map[int]rpc.EgressClient{
			remoteIndex: egressClient,
			localIndex:  nil,
		}

		proxy = store.NewProxyStore(local, 1, remotes, lookup.Lookup)
	})

	It("gets data from the local store for its index", func() {
		lookup.result = localIndex
		local.result = []*loggregator_v2.Envelope{
			{Timestamp: 1},
		}

		result := proxy.Get("some-source", time.Unix(0, 99), time.Unix(0, 100), "envelope-type", 101, true)

		Expect(result).To(Equal(local.result))

		Expect(local.sourceID).To(Equal("some-source"))
		Expect(local.start).To(Equal(time.Unix(0, 99)))
		Expect(local.end).To(Equal(time.Unix(0, 100)))
		Expect(local.envelopeType).To(Equal("envelope-type"))
		Expect(local.limit).To(Equal(101))
		Expect(local.descending).To(BeTrue())

		Expect(lookup.sourceID).To(Equal("some-source"))
	})

	It("gets data from the remote store for its index", func() {
		lookup.result = remoteIndex
		egressClient.results = []*loggregator_v2.Envelope{
			{Timestamp: 1},
		}

		result := proxy.Get("some-source", time.Unix(0, 99), time.Unix(0, 100), nil, 101, true)

		Expect(result).To(Equal(egressClient.results))
		Expect(egressClient.requests).To(HaveLen(1))
		req := egressClient.requests[0]

		Expect(req.SourceId).To(Equal("some-source"))
		Expect(req.StartTime).To(Equal(int64(99)))
		Expect(req.EndTime).To(Equal(int64(100)))
		Expect(req.EnvelopeType).To(Equal(rpc.EnvelopeTypes_ANY))
		Expect(req.Limit).To(Equal(int64(101)))
		Expect(req.Descending).To(BeTrue())

		Expect(lookup.sourceID).To(Equal("some-source"))
	})

	It("gets sourceIds from the local store", func() {
		lookup.result = localIndex
		local.metaResult = map[string]store.MetaInfo{
			"source-1": {
				Count:   1,
				Expired: 2,
				Oldest:  time.Unix(0, 3),
				Newest:  time.Unix(0, 4),
			},
			"source-2": {
				Count:   5,
				Expired: 6,
				Oldest:  time.Unix(0, 7),
				Newest:  time.Unix(0, 8),
			},
		}

		sourceIds := proxy.Meta(true)
		Expect(sourceIds).To(HaveKeyWithValue("source-1", store.MetaInfo{
			Count:   1,
			Expired: 2,
			Oldest:  time.Unix(0, 3),
			Newest:  time.Unix(0, 4),
		}))
		Expect(sourceIds).To(HaveKeyWithValue("source-2", store.MetaInfo{
			Count:   5,
			Expired: 6,
			Oldest:  time.Unix(0, 7),
			Newest:  time.Unix(0, 8),
		}))

		Expect(egressClient.metaRequests).To(BeEmpty())
	})

	It("gets sourceIds from the remote store and the local store", func() {
		local.metaResult = map[string]store.MetaInfo{
			"source-1": {
				Count:   1,
				Expired: 2,
				Oldest:  time.Unix(0, 3),
				Newest:  time.Unix(0, 4),
			},
			"source-2": {
				Count:   5,
				Expired: 6,
				Oldest:  time.Unix(0, 7),
				Newest:  time.Unix(0, 8),
			},
		}

		lookup.result = remoteIndex
		egressClient.metaResults = map[string]*rpc.MetaInfo{
			"source-3": {
				Count:           9,
				Expired:         10,
				OldestTimestamp: 11,
				NewestTimestamp: 12,
			},
		}

		sourceIds := proxy.Meta(false)
		Expect(sourceIds).To(HaveKeyWithValue("source-1", store.MetaInfo{
			Count:   1,
			Expired: 2,
			Oldest:  time.Unix(0, 3),
			Newest:  time.Unix(0, 4),
		}))
		Expect(sourceIds).To(HaveKeyWithValue("source-2", store.MetaInfo{
			Count:   5,
			Expired: 6,
			Oldest:  time.Unix(0, 7),
			Newest:  time.Unix(0, 8),
		}))
		Expect(sourceIds).To(HaveKeyWithValue("source-3", store.MetaInfo{
			Count:   9,
			Expired: 10,
			Oldest:  time.Unix(0, 11),
			Newest:  time.Unix(0, 12),
		}))

		Expect(egressClient.metaRequests).To(HaveLen(1))
		Expect(egressClient.metaRequests[0].LocalOnly).To(BeTrue())
	})

	It("gets sourceIds as empty list the remotes have an error", func() {
		lookup.result = remoteIndex
		egressClient.metaErr = errors.New("errors")

		sourceIds := proxy.Meta(false)
		Expect(sourceIds).To(BeEmpty())
	})

	DescribeTable("envelope types", func(t store.EnvelopeType, expected rpc.EnvelopeTypes) {
		lookup.result = remoteIndex
		egressClient.results = []*loggregator_v2.Envelope{
			{Timestamp: 1},
		}

		proxy.Get("some-source", time.Unix(0, 99), time.Unix(0, 100), t, 101, false)

		req := egressClient.requests[0]
		Expect(req.EnvelopeType).To(Equal(expected))
	},
		Entry("log", &loggregator_v2.Log{}, rpc.EnvelopeTypes_LOG),
		Entry("counter", &loggregator_v2.Counter{}, rpc.EnvelopeTypes_COUNTER),
		Entry("gauge", &loggregator_v2.Gauge{}, rpc.EnvelopeTypes_GAUGE),
		Entry("timer", &loggregator_v2.Timer{}, rpc.EnvelopeTypes_TIMER),
		Entry("event", &loggregator_v2.Event{}, rpc.EnvelopeTypes_EVENT))

})

type spyLocalStore struct {
	sourceID     string
	start        time.Time
	end          time.Time
	envelopeType store.EnvelopeType
	limit        int
	descending   bool
	result       []*loggregator_v2.Envelope
	metaResult   map[string]store.MetaInfo
}

func newSpyGetter() *spyLocalStore {
	return &spyLocalStore{}
}

func (s *spyLocalStore) Get(
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
	return s.result
}

func (s *spyLocalStore) Meta() map[string]store.MetaInfo {
	return s.metaResult
}

type spyEgressClient struct {
	requests     []*rpc.ReadRequest
	results      []*loggregator_v2.Envelope
	metaRequests []*rpc.MetaRequest
	metaResults  map[string]*rpc.MetaInfo
	metaErr      error
}

func newSpyEgressClient() *spyEgressClient {
	return &spyEgressClient{}
}

func (s *spyEgressClient) Read(ctx context.Context, in *rpc.ReadRequest, opts ...grpc.CallOption) (*rpc.ReadResponse, error) {
	s.requests = append(s.requests, in)
	return &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: s.results,
		},
	}, nil
}

func (s *spyEgressClient) Meta(ctx context.Context, r *rpc.MetaRequest, opts ...grpc.CallOption) (*rpc.MetaResponse, error) {
	s.metaRequests = append(s.metaRequests, r)
	metaInfo := make(map[string]*rpc.MetaInfo)
	for id, m := range s.metaResults {
		metaInfo[id] = m
	}

	return &rpc.MetaResponse{
		Meta: metaInfo,
	}, s.metaErr
}

type spyLookup struct {
	sourceID string
	result   int
}

func newSpyLookup() *spyLookup {
	return &spyLookup{}
}

func (s *spyLookup) Lookup(sourceID string) int {
	s.sourceID = sourceID
	return s.result
}
