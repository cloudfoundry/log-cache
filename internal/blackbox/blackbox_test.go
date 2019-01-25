package blackbox_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"code.cloudfoundry.org/log-cache/internal/blackbox"
	logcache_client "code.cloudfoundry.org/log-cache/pkg/client"
	rpc "code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"

	"code.cloudfoundry.org/log-cache/internal/testing"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Log Cache Blackbox™", func() {
	It("periodically emits test metrics to the log-cache", func() {
		tc := setup()
		defer tc.teardown()

		ingressClient := blackbox.NewIngressClient(tc.logCache.url(), grpc.WithInsecure())

		startTime := time.Now().UnixNano()
		go blackbox.StartEmittingTestMetrics("source-1", 10*time.Millisecond, ingressClient)

		Eventually(tc.logCache.numEmittedRequests).Should(BeNumerically(">", 1))

		batch := tc.logCache.requests[0].GetEnvelopes().GetBatch()[0]
		Expect(batch.GetTimestamp()).To(BeNumerically("~", startTime, int64(time.Second)))
		Expect(batch.GetSourceId()).To(Equal("source-1"))

		metrics := batch.GetGauge().GetMetrics()

		for _, expected_metric_name := range blackbox.MagicMetricNames() {
			gaugeValue := metrics[expected_metric_name]

			Expect(gaugeValue.GetValue()).To(Equal(10.0))
			Expect(gaugeValue.GetUnit()).To(Equal("ms"))
		}
	})

	It("re-emits calculated metrics to the log-cache", func() {
		tc := setup()
		defer tc.teardown()

		ingressClient := blackbox.NewIngressClient(tc.logCache.url(), grpc.WithInsecure())
		startTime := time.Now().UnixNano()

		calculatedMetrics := map[string]float64{
			"blackbox_grpc_reliability": 99.11,
			"blackbox_http_reliability": 88.22,
		}

		blackbox.EmitMeasuredMetrics("source-2", ingressClient, calculatedMetrics)

		Eventually(tc.logCache.numEmittedRequests).Should(BeNumerically("==", 1))

		batch := tc.logCache.requests[0].GetEnvelopes().GetBatch()[0]
		Expect(batch.GetTimestamp()).To(BeNumerically("~", startTime, int64(time.Second)))
		Expect(batch.GetSourceId()).To(Equal("source-2"))

		metrics := batch.GetGauge().GetMetrics()
		Expect(metrics["blackbox_grpc_reliability"].GetValue()).To(Equal(99.11))
		Expect(metrics["blackbox_http_reliability"].GetValue()).To(Equal(88.22))
	})

	Context("ReliabilityCalculator", func() {
		It("calculates reliabilities real good", func() {
			rc := blackbox.ReliabilityCalculator{
				EmissionInterval: time.Second,
				WindowInterval:   10 * time.Minute,
				WindowLag:        15 * time.Minute,
				SourceId:         "source-1",
				InfoLogger:       nullLogger(),
				ErrorLogger:      nullLogger(),
			}
			mc := &mockClient{
				responseCount: 597,
			}

			Expect(rc.Calculate(mc)).To(BeNumerically("==", 0.9950))
		})

		It("returns an error when log-cache is unresponsive", func() {
			rc := blackbox.ReliabilityCalculator{
				EmissionInterval: time.Second,
				WindowInterval:   10 * time.Minute,
				WindowLag:        15 * time.Minute,
				SourceId:         "source-1",
				InfoLogger:       nullLogger(),
				ErrorLogger:      nullLogger(),
			}
			unresponsiveClient := &mockUnresponsiveClient{}

			_, err := rc.Calculate(unresponsiveClient)
			Expect(err).NotTo(BeNil())
		})
	})
})

func setup() testContext {
	lc := mockLogCache{
		port: 8080,
	}

	lc.Start()

	time.Sleep(10 * time.Millisecond)

	return testContext{
		logCache: &lc,
	}
}

func (tc testContext) teardown() {
	tc.logCache.lis.Close()
}

type testContext struct {
	logCache *mockLogCache
}

type mockLogCache struct {
	requests []*rpc.SendRequest

	lis  net.Listener
	port int

	sync.Mutex
}

func (lc *mockLogCache) Start() {
	freePort := testing.GetFreePort()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", freePort))
	if err != nil {
		panic(err)
	}
	lc.port = freePort
	lc.lis = lis
	srv := grpc.NewServer()
	rpc.RegisterIngressServer(srv, lc)
	go srv.Serve(lis)
}

func (lc *mockLogCache) url() string {
	return fmt.Sprintf("localhost:%d", lc.port)
}

func (lc *mockLogCache) numEmittedRequests() int {
	lc.Lock()
	defer lc.Unlock()

	return len(lc.requests)
}

func (lc *mockLogCache) Send(ctx context.Context, sendRequest *rpc.SendRequest) (*rpc.SendResponse, error) {
	lc.Lock()
	defer lc.Unlock()

	lc.requests = append(lc.requests, sendRequest)

	return &rpc.SendResponse{}, nil
}

type mockClient struct {
	responseCount int
}

func (c *mockClient) PromQL(_ context.Context, _ string, _ ...logcache_client.PromQLOption) (*rpc.PromQL_InstantQueryResult, error) {
	var points []*rpc.PromQL_Point

	ts := 0
	for i := 0; i <= 200; i++ {
		points = append(points, &rpc.PromQL_Point{
			Time:  strconv.FormatInt(int64(ts+i*1000), 10),
			Value: 10.0,
		})
	}

	for i := 202; i < (c.responseCount + 1); i++ {
		points = append(points, &rpc.PromQL_Point{
			Time:  strconv.FormatInt(int64(ts+i*1000), 10),
			Value: 10.0,
		})
	}

	return &rpc.PromQL_InstantQueryResult{
		Result: &rpc.PromQL_InstantQueryResult_Matrix{
			Matrix: &rpc.PromQL_Matrix{
				Series: []*rpc.PromQL_Series{
					{
						Points: points,
					},
				},
			},
		},
	}, nil
}

type mockUnresponsiveClient struct {
}

func (c *mockUnresponsiveClient) PromQL(_ context.Context, _ string, _ ...logcache_client.PromQLOption) (*rpc.PromQL_InstantQueryResult, error) {
	return nil, fmt.Errorf("unexpected status code 500")
}

func nullLogger() *log.Logger {
	return log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
}
