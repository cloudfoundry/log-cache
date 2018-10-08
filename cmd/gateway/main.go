package main

import (
	"log"
	"os"

	"net/http"
	_ "net/http/pprof"

	"code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Print("Starting Log Cache Gateway...")
	defer log.Print("Closing Log Cache Gateway.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	gateway := logcache.NewGateway(cfg.LogCacheAddr, cfg.Addr,
		logcache.WithGatewayLogger(log.New(os.Stderr, "[GATEWAY] ", log.LstdFlags)),
		logcache.WithGatewayLogCacheDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
		logcache.WithGatewayVersion(cfg.Version),
	)

	gateway.Start()

	// health endpoints (pprof)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
