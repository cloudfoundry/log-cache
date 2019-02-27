package main

import (
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	. "code.cloudfoundry.org/log-cache/internal/nozzle"
	"google.golang.org/grpc"

	loggregator "code.cloudfoundry.org/go-loggregator"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

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

	m := metrics.New()
	loggr := log.New(os.Stderr, "[LOGGR] ", log.LstdFlags)

	dropped := m.NewCounter("Dropped")

	streamConnector := loggregator.NewEnvelopeStreamConnector(
		cfg.LogProviderAddr,
		tlsCfg,
		loggregator.WithEnvelopeStreamLogger(loggr),
		loggregator.WithEnvelopeStreamBuffer(10000, func(missed int) {
			loggr.Printf("dropped %d envelope batches", missed)
			dropped(uint64(missed))
		}),
	)

	nozzle := NewNozzle(
		streamConnector,
		cfg.LogCacheAddr,
		cfg.ShardId,
		WithLogger(log.New(os.Stderr, "", log.LstdFlags)),
		WithMetrics(m),
		WithDialOpts(
			grpc.WithTransportCredentials(
				cfg.LogCacheTLS.Credentials("log-cache"),
			),
		),
		WithSelectors(cfg.Selectors...),
	)

	go nozzle.Start()

	// health endpoints (pprof and prometheus)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
