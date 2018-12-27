package main

import (
	"log"
	"os"
	"time"

	"code.cloudfoundry.org/log-cache/internal/blackbox"
	"google.golang.org/grpc"
)

func main() {
	infoLogger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	errorLogger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load configuration: %s", err)
	}

	ingressClientBuilder := blackbox.NewIngressClientBuilder(
		cfg.DataSourceGrpcAddr,
		grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
	)

	go blackbox.StartEmittingTestMetrics(cfg.SourceId, cfg.EmissionInterval, ingressClientBuilder)

	grpcEgressClientBuilder := blackbox.NewGrpcEgressClientBuilder(
		cfg.DataSourceGrpcAddr,
		grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
	)

	var httpEgressClientBuilder func() blackbox.QueryableClient

	if cfg.CfBlackboxEnabled {
		httpEgressClientBuilder = blackbox.NewHttpEgressClientBuilder(cfg.DataSourceHTTPAddr, cfg.UaaAddr, cfg.ClientID, cfg.ClientSecret, cfg.SkipTLSVerify)
	}

	t := time.NewTicker(cfg.SampleInterval)
	rc := blackbox.ReliabilityCalculator{
		SampleInterval:   cfg.SampleInterval,
		WindowInterval:   cfg.WindowInterval,
		WindowLag:        cfg.WindowLag,
		EmissionInterval: cfg.EmissionInterval,
		SourceId:         cfg.SourceId,
		InfoLogger:       infoLogger,
		ErrorLogger:      errorLogger,
	}

	for range t.C {
		reliabilityMetrics := make(map[string]float64)

		infoLogger.Println("Querying for gRPC reliability metric...")
		grpcReliability, err := rc.Calculate(grpcEgressClientBuilder)
		if err == nil {
			reliabilityMetrics["blackbox.grpc_reliability"] = grpcReliability
		}

		if cfg.CfBlackboxEnabled {
			infoLogger.Println("Querying for HTTP reliability metric...")
			httpReliability, err := rc.Calculate(httpEgressClientBuilder)
			if err == nil {
				reliabilityMetrics["blackbox.http_reliability"] = httpReliability
			}
		}

		blackbox.EmitMeasuredMetrics(cfg.SourceId, ingressClientBuilder, reliabilityMetrics)
	}
}
