package main

import (
	"expvar"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"google.golang.org/grpc"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/log-cache"
)

func main() {
	log.Print("Starting Log Cache...")
	defer log.Print("Closing Log Cache.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	tlsCfg, err := loggregator.NewEgressTLSConfig(
		cfg.TLS.LogProviderCA,
		cfg.TLS.LogProviderCert,
		cfg.TLS.LogProviderKey,
	)
	if err != nil {
		log.Fatalf("invalid TLS configuration: %s", err)
	}

	streamConnector := loggregator.NewEnvelopeStreamConnector(
		cfg.LogProviderAddr,
		tlsCfg,
		loggregator.WithEnvelopeStreamLogger(log.New(os.Stderr, "[LOGGR] ", log.LstdFlags)),
	)

	opts := []logcache.LogCacheOption{
		logcache.WithEgressAddr(cfg.EgressAddr),
		logcache.WithStoreSize(cfg.StoreSize),
		logcache.WithLogger(log.New(os.Stderr, "", log.LstdFlags)),
		logcache.WithMetrics(expvar.NewMap("LogCache")),
	}

	if len(cfg.NodeAddrs) > 1 {
		opts = append(opts, logcache.WithClustered(cfg.NodeIndex, cfg.NodeAddrs, logcache.ClusterGrpc{
			Addr: cfg.IngressAddr,
			DialOptions: []grpc.DialOption{
				grpc.WithInsecure(),
			},
		}))
	}

	cache := logcache.New(
		streamConnector,
		opts...,
	)
	cache.Start()

	// health endpoints (pprof and expvar)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}
