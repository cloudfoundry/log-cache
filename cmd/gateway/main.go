package main

import (
	"log"
	"os"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
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
		logcache.WithGatewayLogCacheDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
		logcache.WithGatewayGroupReaderDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache-group-reader")),
		),
	)

	gateway.Start()
}
