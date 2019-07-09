package main

import (
	"code.cloudfoundry.org/go-loggregator/metrics"
	"code.cloudfoundry.org/log-cache/internal/routing"
	"code.cloudfoundry.org/log-cache/internal/syslog"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"code.cloudfoundry.org/go-envstruct"
	"google.golang.org/grpc"
)

const (
	BATCH_FLUSH_INTERVAL = 500 * time.Millisecond
	BATCH_CHANNEL_SIZE   = 512
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Print("Starting Syslog Server...")
	defer log.Print("Closing Syslog Server.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	loggr := log.New(os.Stderr, "[LOGGR] ", log.LstdFlags)
	m := metrics.NewRegistry(loggr)

	egressDropped := m.NewCounter("egress_dropped")
	conn, err := grpc.Dial(
		cfg.LogCacheAddr,
		grpc.WithTransportCredentials(
			cfg.LogCacheTLS.Credentials("log-cache"),
		),
	)

	client := logcache_v1.NewIngressClient(conn)

	server := syslog.NewServer(
		loggr,
		routing.NewBatchedIngressClient(BATCH_CHANNEL_SIZE, BATCH_FLUSH_INTERVAL, client, egressDropped, loggr),
		m,
		cfg.SyslogTLSCertPath,
		cfg.SyslogTLSKeyPath,
		syslog.WithServerPort(cfg.SyslogPort),
		syslog.WithIdleTimeout(cfg.SyslogIdleTimeout),
	)

	go server.Start()

	// health endpoints (pprof and prometheus)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}
