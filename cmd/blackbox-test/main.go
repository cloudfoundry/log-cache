package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"time"

	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	logcache "code.cloudfoundry.org/log-cache/client"
	"code.cloudfoundry.org/log-cache/rpc/logcache_v1"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load configuration: %s", err)
	}

	ingressClient := buildIngressClient(cfg)
	go startEmittingTestMetrics(cfg, ingressClient)

	grpcEgressClient := buildGrpcEgressClient(cfg)

	var httpEgressClient *logcache.Client

	if cfg.CfBlackboxEnabled {
		httpEgressClient = buildHttpEgressClient(cfg)
	}

	t := time.NewTicker(cfg.SampleInterval)
	for range t.C {
		expectedEmissionCount := cfg.WindowInterval.Seconds() / cfg.EmissionInterval.Seconds()

		// TODO - this will get pretty noisy if we do it every minute
		log.Println("Counting emitted metrics...")

		reliabilityMetrics := make(map[string]float64)

		grpcReceivedCount := countMetricPoints(cfg, grpcEgressClient, cfg.SourceID)
		reliabilityMetrics["blackbox.grpc_reliability"] = float64(grpcReceivedCount) / expectedEmissionCount

		if cfg.CfBlackboxEnabled {
			httpReceivedCount := countMetricPoints(cfg, httpEgressClient, cfg.SourceID)
			reliabilityMetrics["blackbox.http_reliability"] = float64(httpReceivedCount) / expectedEmissionCount
		}

		log.Println("Emitting measured metrics...")
		emitMeasuredMetrics(cfg, ingressClient, reliabilityMetrics)
	}
}

func buildIngressClient(cfg *Config) logcache_v1.IngressClient {
	conn, err := grpc.Dial(cfg.DataSourceGrpcAddr, grpc.WithTransportCredentials(
		cfg.TLS.Credentials("log-cache"),
	))

	if err != nil {
		log.Fatalf("failed to dial %s: %s", cfg.DataSourceGrpcAddr, err)
	}

	return logcache_v1.NewIngressClient(conn)
}

func buildGrpcEgressClient(cfg *Config) *logcache.Client {
	return logcache.NewClient(
		cfg.DataSourceGrpcAddr,
		logcache.WithViaGRPC(
			grpc.WithTransportCredentials(
				cfg.TLS.Credentials("log-cache"),
			),
		),
	)
}

func buildHttpEgressClient(cfg *Config) *logcache.Client {
	return logcache.NewClient(
		cfg.DataSourceHttpAddr,
		logcache.WithHTTPClient(
			logcache.NewOauth2HTTPClient(
				cfg.UaaAddr,
				cfg.ClientID,
				cfg.ClientSecret,
				logcache.WithOauth2HTTPClient(buildHttpClient(cfg)),
			),
		),
	)
}

func buildHttpClient(cfg *Config) *http.Client {
	client := http.DefaultClient
	client.Timeout = 10 * time.Second

	if cfg.SkipTLSVerify {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	return client
}

func startEmittingTestMetrics(cfg *Config, ingressClient logcache_v1.IngressClient) {
	for range time.NewTicker(cfg.EmissionInterval).C {
		emitTestMetrics(cfg, ingressClient)
	}
}

func emitTestMetrics(cfg *Config, client logcache_v1.IngressClient) {
	batch := []*loggregator_v2.Envelope{
		{
			Timestamp: time.Now().UnixNano(),
			SourceId:  cfg.SourceID,
			Message: &loggregator_v2.Envelope_Gauge{
				Gauge: &loggregator_v2.Gauge{
					Metrics: map[string]*loggregator_v2.GaugeValue{
						"blackbox.test_metric": &loggregator_v2.GaugeValue{
							Value: 10.0,
							Unit:  "ms",
						},
					},
				},
			},
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := client.Send(ctx, &logcache_v1.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	})

	if err != nil {
		log.Printf("failed to write test metric envelope: %s\n", err)
	}
}

func emitMeasuredMetrics(cfg *Config, client logcache_v1.IngressClient, metrics map[string]float64) {
	envelopeMetrics := make(map[string]*loggregator_v2.GaugeValue)

	for metricName, value := range metrics {
		envelopeMetrics[metricName] = &loggregator_v2.GaugeValue{
			Value: value,
			Unit:  "%",
		}
	}

	batch := []*loggregator_v2.Envelope{
		{
			Timestamp: time.Now().UnixNano(),
			SourceId:  cfg.SourceID,
			Message: &loggregator_v2.Envelope_Gauge{
				Gauge: &loggregator_v2.Gauge{
					Metrics: envelopeMetrics,
				},
			},
		},
	}

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := client.Send(ctx, &logcache_v1.SendRequest{
		Envelopes: &loggregator_v2.EnvelopeBatch{
			Batch: batch,
		},
	})

	if err != nil {
		log.Printf("failed to write measured metrics envelope: %s\n", err)
	}
}

func countMetricPoints(cfg *Config, client *logcache.Client, sourceID string) uint64 {
	queryString := fmt.Sprintf(`count_over_time(blackbox_test_metric{source_id="%s"}[%.0fs])`, sourceID, cfg.WindowInterval.Seconds())

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	queryResult, err := client.PromQL(ctx, queryString)
	if err != nil {
		log.Printf("failed to count test metrics: %s\n", err)
		return 0
	}

	samples := queryResult.GetVector().GetSamples()

	if len(samples) < 1 {
		return 0
	}

	return uint64(samples[0].GetPoint().GetValue())
}
