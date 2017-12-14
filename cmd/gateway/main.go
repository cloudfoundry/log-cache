package main

import (
	"log"
	"os"

	"code.cloudfoundry.org/log-cache"
)

func main() {
	log.Print("Starting Log Cache Gateway...")
	defer log.Print("Closing Log Cache Gateway.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	gateway := logcache.NewGateway(cfg.LogCacheAddr, cfg.Addr,
		logcache.WithGatewayLogger(log.New(os.Stderr, "[GATEWAY] ", log.LstdFlags)),
		logcache.WithGatewayBlock(),
	)

	gateway.Start()
}
