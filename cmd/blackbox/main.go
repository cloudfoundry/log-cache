package main

import (
	"log"
	"os"
	"time"

	"code.cloudfoundry.org/log-cache/internal/blackbox"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

func main() {
	infoLogger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	errorLogger := log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load configuration: %s", err)
	}

	resolver, _ := naming.NewDNSResolverWithFreq(1 * time.Minute)

	ingressClient := blackbox.NewIngressClient(
		cfg.DataSourceGrpcAddr,
		grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		grpc.WithBalancer(
			grpc.RoundRobin(resolver),
		),
	)

	go blackbox.StartEmittingTestMetrics(cfg.SourceId, cfg.EmissionInterval, ingressClient)

	grpcEgressClient := blackbox.NewGrpcEgressClient(
		cfg.DataSourceGrpcAddr,
		grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		grpc.WithBalancer(
			grpc.RoundRobin(resolver),
		),
	)

	var httpEgressClient blackbox.QueryableClient

	if cfg.CfBlackboxEnabled {
		httpEgressClient = blackbox.NewHttpEgressClient(
			cfg.DataSourceHTTPAddr,
			cfg.UaaAddr,
			cfg.ClientID,
			cfg.ClientSecret,
			cfg.SkipTLSVerify,
		)
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
		grpcReliability, err := rc.Calculate(grpcEgressClient)
		if err == nil {
			reliabilityMetrics["blackbox.grpc_reliability"] = grpcReliability
		}

		if cfg.CfBlackboxEnabled {
			infoLogger.Println("Querying for HTTP reliability metric...")
			httpReliability, err := rc.Calculate(httpEgressClient)
			if err == nil {
				reliabilityMetrics["blackbox.http_reliability"] = httpReliability
			}
		}

		blackbox.EmitMeasuredMetrics(cfg.SourceId, ingressClient, httpEgressClient, reliabilityMetrics)
	}
}
