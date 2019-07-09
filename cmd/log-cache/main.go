package main

import (
	"code.cloudfoundry.org/go-loggregator/metrics"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	. "code.cloudfoundry.org/log-cache/internal/cache"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Print("Starting Log Cache...")
	defer log.Print("Closing Log Cache.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	logger := log.New(os.Stderr, "", log.LstdFlags)

	m := metrics.NewRegistry(logger)
	uptimeFn := m.NewGauge(
		"log_cache_uptime",
		metrics.WithMetricTags(map[string]string{
			"unit": "seconds",
		}),
	)

	t := time.NewTicker(time.Second)
	go func(start time.Time) {
		for range t.C {
			uptimeFn.Set(float64(time.Since(start) / time.Second))
		}
	}(time.Now())

	cache := New(
		m,
		logger,
		WithAddr(cfg.Addr),
		WithMemoryLimit(float64(cfg.MemoryLimit)),
		WithMaxPerSource(cfg.MaxPerSource),
		WithQueryTimeout(cfg.QueryTimeout),
		WithClustered(
			cfg.NodeIndex,
			cfg.NodeAddrs,
			grpc.WithTransportCredentials(
				cfg.TLS.Credentials("log-cache"),
			),
		),
		WithServerOpts(grpc.Creds(cfg.TLS.Credentials("log-cache"))),
	)

	cache.Start()

	// health endpoints (pprof and prometheus)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}
