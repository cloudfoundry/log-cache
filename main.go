package main

import (
	"fmt"
	"log"
	"os"

	"net/http"
	_ "net/http/pprof"

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

	cache := app.NewLogCache(
		streamConnector,
		app.WithEgressAddr(cfg.EgressAddr),
		app.WithStoreSize(cfg.StoreSize),
		app.WithLogger(log.New(os.Stderr, "", log.LstdFlags)),
	)
	cache.Start()

	// pprof
	log.Printf("PProf: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.PProfPort), nil))
}
