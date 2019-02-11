package blackbox

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	logcache_client "code.cloudfoundry.org/log-cache/pkg/client"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"google.golang.org/grpc"
)

const (
	MAGIC_METRIC_NAME = "blackbox_test_metric"
	TEN_MINUTES       = int64(600)
)

func MagicMetricNames() []string {
	return []string{
		MAGIC_METRIC_NAME,
	}
}

func NewIngressClient(grpcAddr string, opts ...grpc.DialOption) logcache_v1.IngressClient {
	var conn *grpc.ClientConn
	var err error

	for retries := 5; retries > 0; retries-- {
		conn, err = grpc.Dial(grpcAddr, opts...)
		if err == nil {
			break
		}

		log.Println("Failed to call grpc.Dial for ingress, retrying...")
		time.Sleep(10 * time.Millisecond)
	}

	if err != nil {
		log.Fatalf("failed to dial %s: %s", grpcAddr, err)
	}

	return logcache_v1.NewIngressClient(conn)
}

func NewGrpcEgressClient(grpcAddr string, opts ...grpc.DialOption) QueryableClient {
	return logcache_client.NewClient(
		grpcAddr,
		logcache_client.WithViaGRPC(opts...),
	)
}

func NewHttpEgressClient(httpAddr, uaaAddr, uaaClientId, uaaClientSecret string, skipTLSVerify bool) QueryableClient {
	return logcache_client.NewClient(
		httpAddr,
		logcache_client.WithHTTPClient(
			logcache_client.NewOauth2HTTPClient(
				uaaAddr,
				uaaClientId,
				uaaClientSecret,
				logcache_client.WithOauth2HTTPClient(buildHttpClient(skipTLSVerify)),
			),
		),
	)
}

func buildHttpClient(skipTLSVerify bool) *http.Client {
	client := http.DefaultClient
	client.Timeout = 10 * time.Second

	if skipTLSVerify {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	return client
}

func StartEmittingTestMetrics(sourceId string, emissionInterval time.Duration, ingressClient logcache_v1.IngressClient) {
	var lastTimestamp time.Time
	var expectedTimestamp time.Time
	var timestamp time.Time

	for range time.NewTicker(emissionInterval).C {
		expectedTimestamp = lastTimestamp.Add(emissionInterval)
		timestamp = time.Now().Truncate(emissionInterval)

		if !lastTimestamp.IsZero() && expectedTimestamp != timestamp {
			log.Printf("WARNING: an expected emission was missed at %s, and was sent at %s\n", expectedTimestamp.String(), timestamp.String())
		}

		emitTestMetrics(sourceId, ingressClient, timestamp)
		lastTimestamp = timestamp
	}
}

func emitTestMetrics(sourceId string, client logcache_v1.IngressClient, timestamp time.Time) {
	var batch []*loggregator_v2.Envelope

	for _, metricName := range MagicMetricNames() {
		batch = append(batch, &loggregator_v2.Envelope{
			Timestamp: timestamp.UnixNano(),
			SourceId:  sourceId,
			Message: &loggregator_v2.Envelope_Gauge{
				Gauge: &loggregator_v2.Gauge{
					Metrics: map[string]*loggregator_v2.GaugeValue{
						metricName: {
							Value: 10.0,
							Unit:  "ms",
						},
					},
				},
			},
		})
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	var err error
	for retries := 5; retries > 0; retries-- {
		_, err := client.Send(ctx, &logcache_v1.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: batch,
			},
		})

		if err == nil {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}

	if err != nil {
		log.Printf("failed to write test metric envelope: %s\n", err)
	}
}

func EmitMeasuredMetrics(sourceId string, ingressClient logcache_v1.IngressClient, logCache QueryableClient, metrics map[string]float64) {
	envelopeMetrics := make(map[string]*loggregator_v2.GaugeValue)

	if len(metrics) == 0 {
		log.Printf("no points to emit")
		return
	}

	up, err := logCache.LogCacheVMUptime(context.TODO())
	if err != nil {
		log.Printf("Couldn't fetch vm uptime, ignoring it: %s", err.Error())
	}

	if up >= 0 && up < TEN_MINUTES {
		log.Printf("vm uptime less than 10 minutes: %d seconds", up)
		return
	}

	for metricName, value := range metrics {
		envelopeMetrics[metricName] = &loggregator_v2.GaugeValue{
			Value: value,
			Unit:  "%",
		}
	}

	batch := []*loggregator_v2.Envelope{
		{
			Timestamp: time.Now().UnixNano(),
			SourceId:  sourceId,
			Message: &loggregator_v2.Envelope_Gauge{
				Gauge: &loggregator_v2.Gauge{
					Metrics: envelopeMetrics,
				},
			},
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err = ingressClient.Send(ctx, &logcache_v1.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	})

	if err != nil {
		log.Printf("failed to write measured metric points: %s\n", err)
	}
}

type QueryableClient interface {
	PromQL(context.Context, string, ...logcache_client.PromQLOption) (*logcache_v1.PromQL_InstantQueryResult, error)
	LogCacheVMUptime(ctx context.Context) (int64, error)
}

type ReliabilityCalculator struct {
	SampleInterval   time.Duration
	WindowInterval   time.Duration
	WindowLag        time.Duration
	EmissionInterval time.Duration
	SourceId         string
	InfoLogger       *log.Logger
	ErrorLogger      *log.Logger
}

func (rc ReliabilityCalculator) Calculate(client QueryableClient) (float64, error) {
	var totalReceivedCount uint64
	expectedEmissionCount := rc.ExpectedSamples()
	magicMetricNames := MagicMetricNames()

	for _, metricName := range magicMetricNames {
		rc.InfoLogger.Println(metricName, "expectedEmissionCount =", expectedEmissionCount)

		receivedCount, err := rc.CountMetricPoints(metricName, client)
		if err != nil {
			return 0, err
		}

		rc.InfoLogger.Println(metricName, "receivedCount =", receivedCount)
		totalReceivedCount += receivedCount
	}

	return float64(totalReceivedCount) / float64(int(expectedEmissionCount)*len(magicMetricNames)), nil
}

func (rc ReliabilityCalculator) ExpectedSamples() float64 {
	return rc.WindowInterval.Seconds() / rc.EmissionInterval.Seconds()
}

func (rc ReliabilityCalculator) printMissingSamples(points []*logcache_v1.PromQL_Point, queryTimestamp time.Time) {
	expectedTimestampsMap := make(map[int64]bool, int64(rc.ExpectedSamples()))
	queryTimestampInMillis := queryTimestamp.UnixNano() / int64(time.Millisecond)
	intervalInMillis := rc.EmissionInterval.Nanoseconds() / int64(time.Millisecond)

	for i := queryTimestampInMillis; i < int64(rc.ExpectedSamples()); i += intervalInMillis {
		expectedTimestampsMap[i] = false
	}

	for _, point := range points {
		timestamp, _ := strconv.ParseInt(point.Time, 10, 64)
		expectedTimestampsMap[timestamp] = true
	}

	var missingTimestamps []int64
	for expectedTimestamp, found := range expectedTimestampsMap {
		if !found {
			missingTimestamps = append(missingTimestamps, expectedTimestamp)
		}
	}

	if len(missingTimestamps) > 0 {
		rc.InfoLogger.Printf("WARNING: Missing %d points\n", len(missingTimestamps))
		rc.InfoLogger.Printf(" -- query start time was %d\n", queryTimestampInMillis)
		for _, missingTimestamp := range missingTimestamps {
			rc.InfoLogger.Printf("%d, ", missingTimestamp)
		}
	}
}

func (rc ReliabilityCalculator) CountMetricPoints(metricName string, client QueryableClient) (uint64, error) {
	queryString := fmt.Sprintf(`%s{source_id="%s"}[%.0fs]`, metricName, rc.SourceId, rc.WindowInterval.Seconds())
	rc.InfoLogger.Println("Issuing query:", queryString)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	queryTimestamp := time.Now().Add(-rc.WindowLag).Truncate(rc.SampleInterval)
	queryResult, err := client.PromQL(ctx, queryString, logcache_client.WithPromQLTime(queryTimestamp))
	if err != nil {
		rc.ErrorLogger.Printf("failed to count test metrics: %s\n", err)
		return 0, err
	}

	series := queryResult.GetMatrix().GetSeries()
	if len(series) == 0 {
		return 0, fmt.Errorf("couldn't find series for %s\n", queryString)
	}

	points := series[0].Points
	if len(points) == 0 {
		rc.InfoLogger.Printf("Response: %+v\n", queryResult)
	}

	if len(points) < int(rc.ExpectedSamples()) {
		rc.printMissingSamples(points, queryTimestamp)
	}

	return uint64(len(points)), nil
}
