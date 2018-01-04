package main

import (
	"expvar"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/metrics"
)

func main() {
	log.Print("Starting Log Cache Group Reader...")
	defer log.Print("Closing Log Cache Group Reader.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	reader := logcache.NewGroupReader(cfg.LogCacheAddr, cfg.NodeAddrs, cfg.NodeIndex,
		logcache.WithGroupReaderLogger(log.New(os.Stderr, "[GROUP-READER] ", log.LstdFlags)),
		logcache.WithGroupReaderMetrics(metrics.New(expvar.NewMap("GroupReader"))),
	)

	reader.Start()

	// health endpoints (pprof and expvar)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}
