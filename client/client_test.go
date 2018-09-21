package logcache_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/client"
	rpc "code.cloudfoundry.org/log-cache/rpc/logcache_v1"
	"google.golang.org/grpc"
)

// Assert that logcache.Reader is fulfilled by Client.Read
var _ logcache.Reader = logcache.Reader(logcache.NewClient("").Read)

func TestClientRead(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	client := logcache.NewClient(logCache.addr())

	envelopes, err := client.Read(context.Background(), "some-id", time.Unix(0, 99))

	if err != nil {
		t.Fatal(err.Error())
	}

	if len(envelopes) != 2 {
		t.Fatalf("expected to receive 2 envelopes, got %d", len(envelopes))
	}

	if envelopes[0].Timestamp != 99 || envelopes[1].Timestamp != 100 {
		t.Fatal("wrong envelopes")
	}

	if len(logCache.reqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.reqs))
	}

	if logCache.reqs[0].URL.Path != "/api/v1/read/some-id" {
		t.Fatalf("expected Path '/api/v1/read/some-id' but got '%s'", logCache.reqs[0].URL.Path)
	}

	assertQueryParam(t, logCache.reqs[0].URL, "start_time", "99")

	if len(logCache.reqs[0].URL.Query()) != 1 {
		t.Fatalf("expected only a single query parameter, but got %d", len(logCache.reqs[0].URL.Query()))
	}
}

func TestGrpcClientRead(t *testing.T) {
	t.Parallel()
	logCache := newStubGrpcLogCache()
	client := logcache.NewClient(logCache.addr(), logcache.WithViaGRPC(grpc.WithInsecure()))

	endTime := time.Now()

	envelopes, err := client.Read(context.Background(), "some-id", time.Unix(0, 99),
		logcache.WithLimit(10),
		logcache.WithEndTime(endTime),
		logcache.WithEnvelopeTypes(rpc.EnvelopeType_LOG, rpc.EnvelopeType_GAUGE),
		logcache.WithDescending(),
	)

	if err != nil {
		t.Fatal(err.Error())
	}

	if len(envelopes) != 2 {
		t.Fatalf("expected to receive 2 envelopes, got %d", len(envelopes))
	}

	if envelopes[0].Timestamp != 99 || envelopes[1].Timestamp != 100 {
		t.Fatal("wrong envelopes")
	}

	if len(logCache.reqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.reqs))
	}

	if logCache.reqs[0].SourceId != "some-id" {
		t.Fatalf("expected SourceId (%s) to equal %s", logCache.reqs[0].SourceId, "some-id")
	}

	if logCache.reqs[0].StartTime != 99 {
		t.Fatalf("expected StartTime (%d) to equal %d", logCache.reqs[0].StartTime, 99)
	}

	if logCache.reqs[0].EndTime != endTime.UnixNano() {
		t.Fatalf("expected EndTime (%d) to equal %d", logCache.reqs[0].EndTime, endTime.UnixNano())
	}

	if logCache.reqs[0].Limit != 10 {
		t.Fatalf("expected Limit (%d) to equal %d", logCache.reqs[0].Limit, 10)
	}

	if len(logCache.reqs[0].EnvelopeTypes) != 2 {
		t.Fatalf("expected to have 2 EnvelopeTypes")
	}

	if logCache.reqs[0].EnvelopeTypes[0] != rpc.EnvelopeType_LOG {
		t.Fatalf("expected EnvelopeType (%v) to equal %v", logCache.reqs[0].EnvelopeTypes[0], rpc.EnvelopeType_LOG)
	}

	if logCache.reqs[0].EnvelopeTypes[1] != rpc.EnvelopeType_GAUGE {
		t.Fatalf("expected EnvelopeType (%v) to equal %v", logCache.reqs[0].EnvelopeTypes[1], rpc.EnvelopeType_GAUGE)
	}

	if !logCache.reqs[0].Descending {
		t.Fatalf("expected Descending to be set")
	}
}

func TestClientReadWithOptions(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	client := logcache.NewClient(logCache.addr())

	_, err := client.Read(
		context.Background(),
		"some-id",
		time.Unix(0, 99),
		logcache.WithEndTime(time.Unix(0, 101)),
		logcache.WithLimit(103),
		logcache.WithEnvelopeTypes(rpc.EnvelopeType_LOG, rpc.EnvelopeType_GAUGE),
		logcache.WithDescending(),
	)

	if err != nil {
		t.Fatal(err.Error())
	}

	if len(logCache.reqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.reqs))
	}

	if logCache.reqs[0].URL.Path != "/api/v1/read/some-id" {
		t.Fatalf("expected Path '/api/v1/read/some-id' but got '%s'", logCache.reqs[0].URL.Path)
	}

	assertQueryParam(t, logCache.reqs[0].URL, "start_time", "99")
	assertQueryParam(t, logCache.reqs[0].URL, "end_time", "101")
	assertQueryParam(t, logCache.reqs[0].URL, "limit", "103")
	assertQueryParam(t, logCache.reqs[0].URL, "envelope_types", "LOG", "GAUGE")
	assertQueryParam(t, logCache.reqs[0].URL, "descending", "true")

	if len(logCache.reqs[0].URL.Query()) != 5 {
		t.Fatalf("expected 5 query parameters, but got %d", len(logCache.reqs[0].URL.Query()))
	}
}

func TestClientReadNon200(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.statusCode = 500
	client := logcache.NewClient(logCache.addr())

	_, err := client.Read(context.Background(), "some-id", time.Unix(0, 99))

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientReadInvalidResponse(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.result["GET/api/v1/read/some-id"] = []byte("invalid")
	client := logcache.NewClient(logCache.addr())

	_, err := client.Read(context.Background(), "some-id", time.Unix(0, 99))

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientReadEmptyJsonResponse(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.result["GET/api/v1/read/some-id"] = []byte("{}")
	client := logcache.NewClient(logCache.addr())

	_, err := client.Read(context.Background(), "some-id", time.Unix(0, 99))

	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestClientReadUnknownAddr(t *testing.T) {
	t.Parallel()
	client := logcache.NewClient("http://invalid.url")

	_, err := client.Read(context.Background(), "some-id", time.Unix(0, 99))

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientReadInvalidAddr(t *testing.T) {
	t.Parallel()
	client := logcache.NewClient("-:-invalid")

	_, err := client.Read(context.Background(), "some-id", time.Unix(0, 99))

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientReadCancelling(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.block = true
	client := logcache.NewClient(logCache.addr())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Read(
		ctx,
		"some-id",
		time.Unix(0, 99),
		logcache.WithEndTime(time.Unix(0, 101)),
		logcache.WithLimit(103),
		logcache.WithEnvelopeTypes(rpc.EnvelopeType_LOG),
	)

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestGrpcClientReadCancelling(t *testing.T) {
	t.Parallel()
	logCache := newStubGrpcLogCache()
	logCache.block = true
	client := logcache.NewClient(logCache.addr(), logcache.WithViaGRPC(grpc.WithInsecure()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Read(
		ctx,
		"some-id",
		time.Unix(0, 99),
		logcache.WithEndTime(time.Unix(0, 101)),
		logcache.WithLimit(103),
		logcache.WithEnvelopeTypes(rpc.EnvelopeType_LOG),
	)

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientMeta(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	client := logcache.NewClient(logCache.addr())

	logCache.result["GET/api/v1/meta"] = []byte(`{
		"meta": {
			"source-0": {},
			"source-1": {}
		}
	}`)

	meta, err := client.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(meta) != 2 {
		t.Fatalf("expected 2 sourceIDs: %d", len(meta))
	}

	if _, ok := meta["source-0"]; !ok {
		t.Fatal("did not find source-0")
	}

	if _, ok := meta["source-1"]; !ok {
		t.Fatal("did not find source-1")
	}
}

func TestClientMetaReturnsErrorWhenRequestFails(t *testing.T) {
	t.Parallel()

	client := logcache.NewClient("https://some-bad-addr")
	if _, err := client.Meta(context.Background()); err == nil {
		t.Fatal("did not error out on bad address")
	}
}

func TestClientMetaFailsOnNon200(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.statusCode = http.StatusNotFound
	client := logcache.NewClient(logCache.addr())

	_, err := client.Meta(context.Background())
	if err == nil {
		t.Fatal("did not error out on bad status code")
	}
}

func TestClientMetaFailsOnInvalidResponseBody(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.result["GET/api/v1/meta"] = []byte("not-real-result")
	client := logcache.NewClient(logCache.addr())

	_, err := client.Meta(context.Background())
	if err == nil {
		t.Fatal("did not error out on bad response body")
	}
}

func TestClientMetaCancelling(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.block = true
	client := logcache.NewClient(logCache.addr())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.Meta(ctx)

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestGrpcClientMeta(t *testing.T) {
	t.Parallel()
	logCache := newStubGrpcLogCache()
	client := logcache.NewClient(logCache.addr(), logcache.WithViaGRPC(grpc.WithInsecure()))

	meta, err := client.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(meta) != 2 {
		t.Fatalf("expected 2 sourceIDs: %d", len(meta))
	}

	if _, ok := meta["source-0"]; !ok {
		t.Fatal("did not find source-0")
	}

	if _, ok := meta["source-1"]; !ok {
		t.Fatal("did not find source-1")
	}
}

func TestGrpcClientMetaCancelling(t *testing.T) {
	t.Parallel()
	logCache := newStubGrpcLogCache()
	client := logcache.NewClient(logCache.addr(), logcache.WithViaGRPC(grpc.WithInsecure()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := client.Meta(ctx); err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientPromQLRange(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	client := logcache.NewClient(logCache.addr())
	start := time.Unix(time.Now().Unix(), 123000000)
	end := start.Add(time.Minute)

	result, err := client.PromQLRange(
		context.Background(),
		`some-query`,
		logcache.WithPromQLStart(start),
		logcache.WithPromQLEnd(end),
		logcache.WithPromQLStep("5m"),
	)

	if err != nil {
		t.Fatal(err.Error())
	}

	series := result.GetMatrix().GetSeries()
	if len(series) != 1 {
		t.Fatalf("expected to receive 1 series, got %d", len(series))
	}

	if series[0].GetPoints()[0].Value != 99 || series[0].GetPoints()[0].Time != "1234.000" {
		t.Fatal("point[0] is incorrect")
	}

	if series[0].GetPoints()[1].Value != 100 || series[0].GetPoints()[1].Time != "5678.000" {
		t.Fatal("point[1] is incorrect")
	}
	if len(logCache.reqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.reqs))
	}

	if logCache.reqs[0].URL.Path != "/api/v1/query_range" {
		t.Fatalf("expected Path '/api/v1/query_range' but got '%s'", logCache.reqs[0].URL.Path)
	}

	assertQueryParam(t, logCache.reqs[0].URL, "query", "some-query")
	assertQueryParam(t, logCache.reqs[0].URL, "start", fmt.Sprintf("%.3f", float64(start.UnixNano())/1e9))
	assertQueryParam(t, logCache.reqs[0].URL, "end", fmt.Sprintf("%.3f", float64(end.UnixNano())/1e9))
	assertQueryParam(t, logCache.reqs[0].URL, "step", "5m")

	if len(logCache.reqs[0].URL.Query()) != 4 {
		t.Fatalf("expected only a single query parameter, but got %d", len(logCache.reqs[0].URL.Query()))
	}
}

func TestClientPromQL(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	client := logcache.NewClient(logCache.addr())

	result, err := client.PromQL(
		context.Background(),
		`some-query`,
	)

	if err != nil {
		t.Fatal(err.Error())
	}

	samples := result.GetVector().GetSamples()
	if len(samples) != 1 {
		t.Fatalf("expected to receive 1 sample, got %d", len(samples))
	}

	if samples[0].Point.Value != 99 || samples[0].Point.Time != "1234.000" {
		t.Fatal("wrong samples")
	}

	if len(logCache.reqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.reqs))
	}

	if logCache.reqs[0].URL.Path != "/api/v1/query" {
		t.Fatalf("expected Path '/api/v1/query' but got '%s'", logCache.reqs[0].URL.Path)
	}

	assertQueryParam(t, logCache.reqs[0].URL, "query", "some-query")

	if len(logCache.reqs[0].URL.Query()) != 1 {
		t.Fatalf("expected only a single query parameter, but got %d", len(logCache.reqs[0].URL.Query()))
	}
}

func TestClientPromQLWithOptions(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	client := logcache.NewClient(logCache.addr())

	_, err := client.PromQL(
		context.Background(),
		"some-query",
		logcache.WithPromQLTime(time.Unix(101, 455700000)),
	)

	if err != nil {
		t.Fatal(err.Error())
	}

	if len(logCache.reqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.reqs))
	}

	if logCache.reqs[0].URL.Path != "/api/v1/query" {
		t.Fatalf("expected Path '/api/v1/query' but got '%s'", logCache.reqs[0].URL.Path)
	}

	assertQueryParam(t, logCache.reqs[0].URL, "query", "some-query")
	assertQueryParam(t, logCache.reqs[0].URL, "time", "101.456")

	if len(logCache.reqs[0].URL.Query()) != 2 {
		t.Fatalf("expected 2 query parameters, but got %d", len(logCache.reqs[0].URL.Query()))
	}
}

func TestClientPromQLNon200(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.statusCode = 500
	client := logcache.NewClient(logCache.addr())

	_, err := client.PromQL(context.Background(), "some-query")

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientPromQLInvalidResponse(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.result["GET/api/v1/query"] = []byte("invalid")
	client := logcache.NewClient(logCache.addr())

	_, err := client.PromQL(context.Background(), "some-query")

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientPromQLUnknownAddr(t *testing.T) {
	t.Parallel()
	client := logcache.NewClient("http://invalid.url")

	_, err := client.PromQL(context.Background(), "some-query")

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientPromQLInvalidAddr(t *testing.T) {
	t.Parallel()
	client := logcache.NewClient("-:-invalid")

	_, err := client.PromQL(context.Background(), "some-query")

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientPromQLCancelling(t *testing.T) {
	t.Parallel()
	logCache := newStubLogCache()
	logCache.block = true
	client := logcache.NewClient(logCache.addr())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.PromQL(
		ctx,
		"some-query",
	)

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestGrpcClientPromQL(t *testing.T) {
	t.Parallel()
	logCache := newStubGrpcLogCache()
	client := logcache.NewClient(logCache.addr(), logcache.WithViaGRPC(grpc.WithInsecure()))

	result, err := client.PromQL(context.Background(), "some-query",
		logcache.WithPromQLTime(time.Unix(99, 0)),
	)

	if err != nil {
		t.Fatal(err.Error())
	}

	scalar := result.GetScalar()
	if scalar.Time != "99.000" || scalar.Value != 101 {
		t.Fatalf("wrong scalar")
	}

	if len(logCache.promInstantReqs) != 1 {
		t.Fatalf("expected have 1 request, have %d", len(logCache.promInstantReqs))
	}

	if logCache.promInstantReqs[0].Query != "some-query" {
		t.Fatalf("expected Query (%s) to equal %s", logCache.promInstantReqs[0].Query, "some-query")
	}

	if logCache.promInstantReqs[0].Time != "99.000" {
		t.Fatalf("expected Time (%s) to equal %s", logCache.promInstantReqs[0].Time, "99.000")
	}
}

func TestGrpcClientPromQLCancelling(t *testing.T) {
	t.Parallel()
	logCache := newStubGrpcLogCache()
	logCache.block = true
	client := logcache.NewClient(logCache.addr(), logcache.WithViaGRPC(grpc.WithInsecure()))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.PromQL(
		ctx,
		"some-query",
	)

	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientAlwaysClosesBody(t *testing.T) {
	t.Parallel()

	// Read
	spyHTTPClient := newSpyHTTPClient()
	client := logcache.NewClient("", logcache.WithHTTPClient(spyHTTPClient))
	client.Read(context.Background(), "some-name", time.Now())

	if !spyHTTPClient.body.closed {
		t.Fatal("expected body to be closed")
	}

	// Meta
	spyHTTPClient = newSpyHTTPClient()
	client = logcache.NewClient("", logcache.WithHTTPClient(spyHTTPClient))
	client.Meta(context.Background())

	if !spyHTTPClient.body.closed {
		t.Fatal("expected body to be closed")
	}

	// PromQL
	spyHTTPClient = newSpyHTTPClient()
	client = logcache.NewClient("", logcache.WithHTTPClient(spyHTTPClient))
	client.PromQL(context.Background(), "some-query")

	if !spyHTTPClient.body.closed {
		t.Fatal("expected body to be closed")
	}
}

type stubLogCache struct {
	statusCode int
	server     *httptest.Server
	reqs       []*http.Request
	bodies     [][]byte
	result     map[string][]byte
	block      bool
}

func newStubLogCache() *stubLogCache {
	s := &stubLogCache{
		statusCode: http.StatusOK,
		result: map[string][]byte{
			"GET/api/v1/read/some-id": []byte(`{
		"envelopes": {
			"batch": [
			    {
					"timestamp": 99,
					"source_id": "some-id"
				},
			    {
					"timestamp": 100,
					"source_id": "some-id"
				}
			]
		}
	}`),
			"GET/api/v1/query": []byte(`
    {
	  "status": "success",
	  "data": {
		"resultType": "vector",
		"result": [
          {
            "metric": {
              "deployment": "cf"
            },
            "value": [ 1234, "99" ]
          }
        ]
      }
    }
			`),
			"GET/api/v1/query_range": []byte(`
    {
	  "status": "success",
	  "data": {
		"resultType": "matrix",
        "result": [
          {
            "metric": {
              "deployment": "cf"
            },
            "values": [
              [ 1234, "99" ],
              [ 5678, "100" ]
            ]
          }
        ]
      }
    }
			`),
		},
	}
	s.server = httptest.NewServer(s)
	return s
}

func (s *stubLogCache) addr() string {
	return s.server.URL
}

func (s *stubLogCache) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.block {
		var block chan struct{}
		<-block
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	s.bodies = append(s.bodies, body)
	s.reqs = append(s.reqs, r)
	w.WriteHeader(s.statusCode)
	w.Write(s.result[r.Method+r.URL.Path])
}

func assertQueryParam(t *testing.T, u *url.URL, name string, values ...string) {
	t.Helper()
	for _, value := range values {
		var found bool
		for _, actual := range u.Query()[name] {
			if actual == value {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("expected query parameter '%s' to contain '%s', but got '%v'", name, value, u.Query()[name])
		}
	}
}

type stubGrpcLogCache struct {
	mu              sync.Mutex
	reqs            []*rpc.ReadRequest
	promInstantReqs []*rpc.PromQL_InstantQueryRequest
	promRangeReqs   []*rpc.PromQL_RangeQueryRequest
	lis             net.Listener
	block           bool
}

func newStubGrpcLogCache() *stubGrpcLogCache {
	s := &stubGrpcLogCache{}
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	s.lis = lis
	srv := grpc.NewServer()
	rpc.RegisterEgressServer(srv, s)
	rpc.RegisterPromQLQuerierServer(srv, s)
	go srv.Serve(lis)

	return s
}

func (s *stubGrpcLogCache) addr() string {
	return s.lis.Addr().String()
}

func (s *stubGrpcLogCache) Read(c context.Context, r *rpc.ReadRequest) (*rpc.ReadResponse, error) {
	if s.block {
		var block chan struct{}
		<-block
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.reqs = append(s.reqs, r)

	return &rpc.ReadResponse{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: []*loggregator_v2.Envelope{
				{Timestamp: 99, SourceId: "some-id"},
				{Timestamp: 100, SourceId: "some-id"},
			},
		},
	}, nil
}

func (s *stubGrpcLogCache) InstantQuery(c context.Context, r *rpc.PromQL_InstantQueryRequest) (*rpc.PromQL_InstantQueryResult, error) {
	if s.block {
		var block chan struct{}
		<-block
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.promInstantReqs = append(s.promInstantReqs, r)

	return &rpc.PromQL_InstantQueryResult{
		Result: &rpc.PromQL_InstantQueryResult_Scalar{
			Scalar: &rpc.PromQL_Scalar{
				Time:  "99.000",
				Value: 101,
			},
		},
	}, nil
}

func (s *stubGrpcLogCache) RangeQuery(c context.Context, r *rpc.PromQL_RangeQueryRequest) (*rpc.PromQL_RangeQueryResult, error) {
	if s.block {
		var block chan struct{}
		<-block
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.promRangeReqs = append(s.promRangeReqs, r)

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
								Value: 101,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (s *stubGrpcLogCache) Meta(context.Context, *rpc.MetaRequest) (*rpc.MetaResponse, error) {
	return &rpc.MetaResponse{
		Meta: map[string]*rpc.MetaInfo{
			"source-0": {},
			"source-1": {},
		},
	}, nil
}

func (s *stubGrpcLogCache) requests() []*rpc.ReadRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.ReadRequest, len(s.reqs))
	copy(r, s.reqs)
	return r
}

func (s *stubGrpcLogCache) promQLRequests() []*rpc.PromQL_InstantQueryRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	r := make([]*rpc.PromQL_InstantQueryRequest, len(s.promInstantReqs))
	copy(r, s.promInstantReqs)
	return r
}

type stubBufferCloser struct {
	*bytes.Buffer
	closed bool
}

func newStubBufferCloser() *stubBufferCloser {
	return &stubBufferCloser{}
}

func (s *stubBufferCloser) Close() error {
	s.closed = true
	return nil
}

type spyHTTPClient struct {
	body *stubBufferCloser
}

func newSpyHTTPClient() *spyHTTPClient {
	return &spyHTTPClient{
		body: newStubBufferCloser(),
	}
}

func (s *spyHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Body: s.body,
	}, nil
}
