package main

import (
	"context"
	"fmt"
	"log"
	"time"

	logcache "code.cloudfoundry.org/log-cache/client"
	"google.golang.org/grpc"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load configuration: %s", err)
	}

	grpcEgressClient := buildGrpcEgressClient(cfg)

	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	result, err := grpcEgressClient.PromQL(ctx, `metricName{source_id="value"}`)
	if err != nil {
		fmt.Println("grpc client error:", err)
	}
	fmt.Println(result)
}

func buildGrpcEgressClient(cfg *Config) *logcache.Client {
	return logcache.NewClient(
		cfg.DataSourceGrpcAddr,
		logcache.WithViaGRPC(
			grpc.WithTransportCredentials(
				cfg.TLS.ExperimentalCredentials("log-cache"),
			),
		),
	)
}
