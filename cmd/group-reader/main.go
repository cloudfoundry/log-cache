package main

import (
	"expvar"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"google.golang.org/grpc"
)

func main() {
	log.Print("Starting Log Cache Group Reader...")
	defer log.Print("Closing Log Cache Group Reader.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	// GroupReader uses the slice to figure out its address. We want to bind
	// to the given one.
	extAddr := cfg.NodeAddrs[cfg.NodeIndex]
	cfg.NodeAddrs[cfg.NodeIndex] = cfg.Addr

	reader := logcache.NewGroupReader(cfg.LogCacheAddr, cfg.NodeAddrs, cfg.NodeIndex,
		logcache.WithGroupReaderLogger(log.New(os.Stderr, "[GROUP-READER] ", log.LstdFlags)),
		logcache.WithGroupReaderMetrics(metrics.New(expvar.NewMap("GroupReader"))),
		logcache.WithGroupReaderExternalAddr(extAddr),
		logcache.WithGroupReaderMaxPerSource(cfg.MaxPerSource),
		logcache.WithGroupReaderServerOpts(
			grpc.Creds(cfg.LogCacheTLS.Credentials("log-cache")),
		),
		logcache.WithGroupReaderDialOpts(
			grpc.WithTransportCredentials(
				cfg.LogCacheTLS.Credentials("log-cache"),
			),
		),
	)

	reader.Start()

	// health endpoints (pprof and expvar)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
