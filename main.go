package main

import (
	"log"
	"os"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/log-cache/app"
)

func main() {
	log.Print("Starting Log Cache...")
	defer log.Print("Closing Log Cache.")

	cfg, err := app.LoadConfig()
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

	cache := app.NewLogCache(streamConnector)
	cache.Start()
}
