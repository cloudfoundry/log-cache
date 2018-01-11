package store_test

import (
	"time"

	"code.cloudfoundry.org/go-log-cache/rpc/logcache"
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
		remotes      map[int]logcache.EgressClient
		lookup       *spyLookup
		getter       *spyGetter
		egressClient *spyEgressClient

		proxy *store.ProxyStore
	)

	BeforeEach(func() {
		getter = newSpyGetter()
		lookup = newSpyLookup()
		egressClient = newSpyEgressClient()
		remotes = map[int]logcache.EgressClient{
			0: egressClient,
			1: nil, // Actually local
		}

		proxy = store.NewProxyStore(getter.Get, 1, remotes, lookup.Lookup)
	})

	It("gets data from the local store for its index", func() {
		// 1 is the local index
		lookup.result = 1
		getter.result = []*loggregator_v2.Envelope{
			{Timestamp: 1},
		}

		result := proxy.Get("some-source", time.Unix(0, 99), time.Unix(0, 100), "envelope-type", 101)

		Expect(result).To(Equal(getter.result))

		Expect(getter.sourceID).To(Equal("some-source"))
		Expect(getter.start).To(Equal(time.Unix(0, 99)))
		Expect(getter.end).To(Equal(time.Unix(0, 100)))
		Expect(getter.envelopeType).To(Equal("envelope-type"))
		Expect(getter.limit).To(Equal(101))

		Expect(lookup.sourceID).To(Equal("some-source"))
	})

	It("gets data from the remote store for its index", func() {
		// 0 is the remote index
		lookup.result = 0
		egressClient.results = []*loggregator_v2.Envelope{
			{Timestamp: 1},
		}

		result := proxy.Get("some-source", time.Unix(0, 99), time.Unix(0, 100), nil, 101)

		Expect(result).To(Equal(egressClient.results))
		Expect(egressClient.requests).To(HaveLen(1))
		req := egressClient.requests[0]

		Expect(req.SourceId).To(Equal("some-source"))
		Expect(req.StartTime).To(Equal(int64(99)))
		Expect(req.EndTime).To(Equal(int64(100)))
		Expect(req.EnvelopeType).To(Equal(logcache.EnvelopeTypes_ANY))
		Expect(req.Limit).To(Equal(int64(101)))

		Expect(lookup.sourceID).To(Equal("some-source"))
	})

	DescribeTable("envelope types", func(t store.EnvelopeType, expected logcache.EnvelopeTypes) {
		// 0 is the remote index
		lookup.result = 0
		egressClient.results = []*loggregator_v2.Envelope{
			{Timestamp: 1},
		}

		proxy.Get("some-source", time.Unix(0, 99), time.Unix(0, 100), t, 101)

		req := egressClient.requests[0]
		Expect(req.EnvelopeType).To(Equal(expected))
	},
		Entry("log", &loggregator_v2.Log{}, logcache.EnvelopeTypes_LOG),
		Entry("counter", &loggregator_v2.Counter{}, logcache.EnvelopeTypes_COUNTER),
		Entry("gauge", &loggregator_v2.Gauge{}, logcache.EnvelopeTypes_GAUGE),
		Entry("timer", &loggregator_v2.Timer{}, logcache.EnvelopeTypes_TIMER),
		Entry("event", &loggregator_v2.Event{}, logcache.EnvelopeTypes_EVENT))

})

type spyGetter struct {
	sourceID     string
	start        time.Time
	end          time.Time
	envelopeType store.EnvelopeType
	limit        int
	result       []*loggregator_v2.Envelope
}

func newSpyGetter() *spyGetter {
	return &spyGetter{}
}

func (s *spyGetter) Get(
	sourceID string,
	start time.Time,
	end time.Time,
	envelopeType store.EnvelopeType,
	limit int,
) []*loggregator_v2.Envelope {
	s.sourceID = sourceID
	s.start = start
	s.end = end
	s.envelopeType = envelopeType
	s.limit = limit
	return s.result
}

type spyEgressClient struct {
	requests []*logcache.ReadRequest
	results  []*loggregator_v2.Envelope
}

func newSpyEgressClient() *spyEgressClient {
	return &spyEgressClient{}
}

func (s *spyEgressClient) Read(ctx context.Context, in *logcache.ReadRequest, opts ...grpc.CallOption) (*logcache.ReadResponse, error) {
	s.requests = append(s.requests, in)
	return &logcache.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: s.results,
		},
	}, nil
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
