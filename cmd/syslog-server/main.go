package main

import (
	"code.cloudfoundry.org/log-cache/internal/routing"
	"code.cloudfoundry.org/log-cache/internal/syslog"
	"code.cloudfoundry.org/log-cache/pkg/rpc/logcache_v1"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"google.golang.org/grpc"
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

	m := metrics.New()
	loggr := log.New(os.Stderr, "[LOGGR] ", log.LstdFlags)

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
		routing.NewBatchedIngressClient(cfg.BatchSize, cfg.BatchInterval, client, egressDropped, loggr),
		m,
		cfg.SyslogTLSCertPath,
		cfg.SyslogTLSKeyPath,
		syslog.WithServerPort(cfg.SyslogPort),
		syslog.WithIdleTimeout(cfg.SyslogIdleTimeout),
	)

	go server.Start()

	// Register prometheus-compatible metric endpoint
	http.Handle("/metrics", m)

	// health endpoints (pprof and prometheus)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}
