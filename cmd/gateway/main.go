package main

import (
	"log"
	"os"

	"net/http"
	_ "net/http/pprof"

	envstruct "code.cloudfoundry.org/go-envstruct"
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

	envstruct.WriteReport(cfg)

	gateway := logcache.NewGateway(cfg.LogCacheAddr, cfg.GroupReaderAddr, cfg.Addr,
		logcache.WithGatewayLogger(log.New(os.Stderr, "[GATEWAY] ", log.LstdFlags)),
		logcache.WithGatewayLogCacheDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
		logcache.WithGatewayGroupReaderDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
	)

	gateway.Start()

	// health endpoints (pprof)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
