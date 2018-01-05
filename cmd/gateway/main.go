package main

import (
	"log"
	"os"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache"
)

func main() {
	log.Print("Starting Log Cache Gateway...")
	defer log.Print("Closing Log Cache Gateway.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	gateway := logcache.NewGateway(cfg.LogCacheAddr, cfg.GroupReaderAddr, cfg.Addr,
		logcache.WithGatewayLogger(log.New(os.Stderr, "[GATEWAY] ", log.LstdFlags)),
		logcache.WithGatewayBlock(),
	)

	gateway.Start()
}
