package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	logcache "code.cloudfoundry.org/go-log-cache"
	rpc "code.cloudfoundry.org/go-log-cache/rpc/logcache_v1"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	uuid "github.com/nu7hatch/gouuid"
	datadog "github.com/zorkian/go-datadog-api"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load configuration: %s", err)
	}

	ddc := datadog.NewClient(cfg.DatadogAPIKey, "")

	t := time.NewTicker(cfg.Interval)
	for range t.C {
		log.Println("Emitting logs...")
		emitCount := 10000
		sourceID := newSourceID()
		start, end := emitEnvelopes(cfg, emitCount, sourceID)
		time.Sleep(10 * time.Second)

		log.Println("Counting emitted logs...")
		receivedCount := countEnvelopes(cfg, start, end, sourceID, emitCount)
		sendToDatadog(cfg, ddc, emitCount, receivedCount)
	}
}

func newSourceID() string {
	id, err := uuid.NewV4()
	if err != nil {
		log.Fatalf("couldn't generate a UUID for source id: %s", err)
	}

	return id.String()
}

func emitEnvelopes(cfg *Config, emitCount int, sourceID string) (time.Time, time.Time) {
	conn, err := grpc.Dial(
		cfg.LogCacheAddr,
		grpc.WithTransportCredentials(
			cfg.TLS.Credentials("log-cache"),
		),
	)
	if err != nil {
		log.Fatalf("failed to dial %s: %s", cfg.LogCacheAddr, err)
	}

	client := rpc.NewIngressClient(conn)

	start := time.Now()
	for n := 0; n < emitCount; n++ {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = client.Send(ctx, &rpc.SendRequest{
			Envelopes: &loggregator_v2.EnvelopeBatch{
				Batch: []*loggregator_v2.Envelope{
					{
						Timestamp: time.Now().UnixNano(),
						SourceId:  sourceID,
					},
				},
			},
		})
		if err != nil {
			log.Printf("Error writing envelope to log cache: %s", err)
		}

		time.Sleep(time.Millisecond)
	}

	return start, time.Now()
}

func countEnvelopes(cfg *Config, start, end time.Time, sourceID string, emitCount int) int {
	client := logcache.NewClient(
		cfg.LogCacheAddr,
		logcache.WithViaGRPC(
			grpc.WithTransportCredentials(
				cfg.TLS.Credentials("log-cache"),
			),
		),
	)

	var receivedCount int
	logcache.Walk(
		context.Background(),
		sourceID,
		func(envelopes []*loggregator_v2.Envelope) bool {
			receivedCount += len(envelopes)
			return receivedCount < emitCount
		},
		client.Read,
		logcache.WithWalkStartTime(start),
		logcache.WithWalkEndTime(end),
		logcache.WithWalkBackoff(logcache.NewRetryBackoff(50*time.Millisecond, 100)),
	)

	return receivedCount
}

func sendToDatadog(cfg *Config, ddc *datadog.Client, emitCount, receivedCount int) {
	var metrics []datadog.Metric
	mType := "gauge"
	name := "log_cache_grpc_blackbox.logs_sent"

	timestamp := time.Now().UnixNano()
	metrics = append(metrics, datadog.Metric{
		Metric: &name,
		Points: toDataPoint(timestamp, float64(emitCount)),
		Type:   &mType,
		Host:   &cfg.DatadogOriginHost,
		Tags:   cfg.DatadogTags,
	})

	name = "log_cache_grpc_blackbox.logs_received"
	metrics = append(metrics, datadog.Metric{
		Metric: &name,
		Points: toDataPoint(timestamp, float64(receivedCount)),
		Type:   &mType,
		Host:   &cfg.DatadogOriginHost,
		Tags:   cfg.DatadogTags,
	})

	err := ddc.PostMetrics(metrics)
	if err != nil {
		log.Printf("failed to write metrics to DataDog: %s", err)
	}

	log.Printf("posted %d metrics:", len(metrics))
	b, _ := json.MarshalIndent(metrics, "", "  ")
	log.Println(string(b))

}

func toDataPoint(x int64, y float64) []datadog.DataPoint {
	t := time.Unix(0, x)
	tf := float64(t.Unix())
	return []datadog.DataPoint{
		[2]*float64{&tf, &y},
	}
}
