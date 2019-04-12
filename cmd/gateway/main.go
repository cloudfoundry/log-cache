package main

import (
	"log"
	"os"

	"net/http"
	_ "net/http/pprof"

	. "code.cloudfoundry.org/log-cache/internal/gateway"
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

	gateway := NewGateway(cfg.LogCacheAddr, cfg.Addr, cfg.ProxyCertPath, cfg.ProxyKeyPath,
		WithGatewayLogger(log.New(os.Stderr, "[GATEWAY] ", log.LstdFlags)),
		WithGatewayLogCacheDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
		WithGatewayVersion(cfg.Version),
	)

	gateway.Start()

	// health endpoints (pprof)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
