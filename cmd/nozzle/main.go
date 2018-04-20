package main

import (
	"expvar"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	envstruct "code.cloudfoundry.org/go-envstruct"
	logcache "code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"google.golang.org/grpc"

	loggregator "code.cloudfoundry.org/go-loggregator"
)

func main() {
	log.Print("Starting LogCache Nozzle...")
	defer log.Print("Closing LogCache Nozzle.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	tlsCfg, err := loggregator.NewEgressTLSConfig(
		cfg.LogsProviderTLS.LogProviderCA,
		cfg.LogsProviderTLS.LogProviderCert,
		cfg.LogsProviderTLS.LogProviderKey,
	)
	if err != nil {
		log.Fatalf("invalid LogsProviderTLS configuration: %s", err)
	}

	loggr := log.New(os.Stderr, "[LOGGR] ", log.LstdFlags)
	streamConnector := loggregator.NewEnvelopeStreamConnector(
		cfg.LogProviderAddr,
		tlsCfg,
		loggregator.WithEnvelopeStreamLogger(loggr),
		loggregator.WithEnvelopeStreamBuffer(10000, func(missed int) {
			loggr.Printf("dropped %d envelope batches", missed)
		}),
	)

	nozzle := logcache.NewNozzle(
		streamConnector,
		cfg.LogCacheAddr,
		logcache.WithNozzleLogger(log.New(os.Stderr, "", log.LstdFlags)),
		logcache.WithNozzleMetrics(metrics.New(expvar.NewMap("Nozzle"))),
		logcache.WithNozzleDialOpts(
			grpc.WithTransportCredentials(
				cfg.LogCacheTLS.Credentials("log-cache"),
			),
		),
	)

	go nozzle.Start()

	// health endpoints (pprof and expvar)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
