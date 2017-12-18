package main

import (
	"expvar"
	"log"
	_ "net/http/pprof"
	"os"

	logcache "code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/metrics"

	loggregator "code.cloudfoundry.org/go-loggregator"
)

func main() {
	log.Print("Starting LogCache Nozzle...")
	defer log.Print("Closing LogCache Nozzle.")

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
	)

	nozzle.Start()
}
