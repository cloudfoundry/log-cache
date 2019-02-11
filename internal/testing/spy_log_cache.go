package testing

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
)

type SpyAgent struct {
	mu        sync.Mutex
	envelopes []*loggregator_v2.Envelope
	tlsConfig *tls.Config
}

func NewSpyAgent(tlsConfig *tls.Config) *SpyAgent {
	return &SpyAgent{
		tlsConfig: tlsConfig,
	}
}

func (s *SpyAgent) Start() string {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(s.tlsConfig)),
	)
	loggregator_v2.RegisterIngressServer(srv, s)
	go srv.Serve(lis)

	return lis.Addr().String()

}

func (s *SpyAgent) Sender(_ loggregator_v2.Ingress_SenderServer) error {
	return nil
}

func (s *SpyAgent) BatchSender(_ loggregator_v2.Ingress_BatchSenderServer) error {
	return nil
}

func (s *SpyAgent) Send(ctx context.Context, batch *loggregator_v2.EnvelopeBatch) (*loggregator_v2.SendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.envelopes = append(s.envelopes, batch.GetBatch()...)

	return &loggregator_v2.SendResponse{}, nil
}

func (s *SpyAgent) GetEnvelopes() []*loggregator_v2.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*loggregator_v2.Envelope, len(s.envelopes))
	copy(r, s.envelopes)
	return r
}

type SpyLogCache struct {
	mu                 sync.Mutex
	localOnlyValues    []bool
	envelopes          []*loggregator_v2.Envelope
	readRequests       []*rpc.ReadRequest
	queryRequests      []*rpc.PromQL_InstantQueryRequest
	QueryError         error
	rangeQueryRequests []*rpc.PromQL_RangeQueryRequest
	ReadEnvelopes      map[string]func() []*loggregator_v2.Envelope
	MetaResponses      map[string]*rpc.MetaInfo
	tlsConfig          *tls.Config
	value              float64
}

func (s *SpyLogCache) SetValue(value float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value = value
}

func NewSpyLogCache(tlsConfig *tls.Config) *SpyLogCache {
	return &SpyLogCache{
		ReadEnvelopes: make(map[string]func() []*loggregator_v2.Envelope),
		tlsConfig:     tlsConfig,
		value:         101,
	}
}

func (s *SpyLogCache) Start() string {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(s.tlsConfig)),
	)
	rpc.RegisterIngressServer(srv, s)
	rpc.RegisterEgressServer(srv, s)
	rpc.RegisterPromQLQuerierServer(srv, s)
	go srv.Serve(lis)

	return lis.Addr().String()
}

func (s *SpyLogCache) GetEnvelopes() []*loggregator_v2.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*loggregator_v2.Envelope, len(s.envelopes))
	copy(r, s.envelopes)
	return r
}

func (s *SpyLogCache) GetLocalOnlyValues() []bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]bool, len(s.localOnlyValues))
	copy(r, s.localOnlyValues)
	return r
}

func (s *SpyLogCache) GetReadRequests() []*rpc.ReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := make([]*rpc.ReadRequest, len(s.readRequests))
	copy(r, s.readRequests)
	return r
}

func (s *SpyLogCache) Send(ctx context.Context, r *rpc.SendRequest) (*rpc.SendResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.localOnlyValues = append(s.localOnlyValues, r.LocalOnly)

	for _, e := range r.Envelopes.Batch {
		s.envelopes = append(s.envelopes, e)
	}

	return &rpc.SendResponse{}, nil
}

func (s *SpyLogCache) Read(ctx context.Context, r *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.readRequests = append(s.readRequests, r)

	b := s.ReadEnvelopes[r.GetSourceId()]

	var batch []*loggregator_v2.Envelope
	if b != nil {
		batch = b()
	}

	return &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	}, nil
}

func (s *SpyLogCache) Meta(ctx context.Context, r *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	return &rpc.MetaResponse{
		Meta: s.MetaResponses,
	}, nil
}

func (s *SpyLogCache) InstantQuery(ctx context.Context, r *rpc.PromQL_InstantQueryRequest) (*rpc.PromQL_InstantQueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.queryRequests = append(s.queryRequests, r)

	return &rpc.PromQL_InstantQueryResult{
		Result: &rpc.PromQL_InstantQueryResult_Scalar{
			Scalar: &rpc.PromQL_Scalar{
				Time:  "99.000",
				Value: s.value,
			},
		},
	}, s.QueryError
}

func (s *SpyLogCache) GetQueryRequests() []*rpc.PromQL_InstantQueryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.PromQL_InstantQueryRequest, len(s.queryRequests))
	copy(r, s.queryRequests)

	return r
}

func (s *SpyLogCache) RangeQuery(ctx context.Context, r *rpc.PromQL_RangeQueryRequest) (*rpc.PromQL_RangeQueryResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.rangeQueryRequests = append(s.rangeQueryRequests, r)

	return &rpc.PromQL_RangeQueryResult{
		Result: &rpc.PromQL_RangeQueryResult_Matrix{
			Matrix: &rpc.PromQL_Matrix{
				Series: []*rpc.PromQL_Series{
					{
						Metric: map[string]string{
							"__name__": "test",
						},
						Points: []*rpc.PromQL_Point{
							{
								Time:  "99.000",
								Value: s.value,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (s *SpyLogCache) GetRangeQueryRequests() []*rpc.PromQL_RangeQueryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.PromQL_RangeQueryRequest, len(s.rangeQueryRequests))
	copy(r, s.rangeQueryRequests)

	return r
}

func StubUptimeFn() int64 {
	return 789
}
