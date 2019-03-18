package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	. "code.cloudfoundry.org/log-cache/internal/cache"
	"code.cloudfoundry.org/log-cache/internal/metrics"
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

	m := metrics.New()
	uptimeFn := m.NewGauge("log_cache_uptime", "seconds")

	t := time.NewTicker(time.Second)
	go func(start time.Time) {
		for range t.C {
			uptimeFn(float64(time.Since(start) / time.Second))
		}
	}(time.Now())

	cache := New(
		WithLogger(log.New(os.Stderr, "", log.LstdFlags)),
		WithMetrics(m),
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

	// Register prometheus-compatible metric endpoint
	http.Handle("/metrics", m)

	// health endpoints (pprof and prometheus)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
