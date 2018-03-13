package main

import (
	"expvar"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"

	"code.cloudfoundry.org/go-envstruct"
	logcache "code.cloudfoundry.org/log-cache"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"google.golang.org/grpc"
)

func main() {
	log.Print("Starting Log Cache Scheduler...")
	defer log.Print("Closing Log Cache Scheduler.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	envstruct.WriteReport(cfg)

	sched := logcache.NewScheduler(
		nil,
		cfg.NodeAddrs,
		logcache.WithSchedulerLogger(log.New(os.Stderr, "", log.LstdFlags)),
		logcache.WithSchedulerMetrics(metrics.New(expvar.NewMap("Scheduler"))),
		logcache.WithSchedulerInterval(cfg.Interval),
		logcache.WithSchedulerCount(cfg.Count),
		logcache.WithSchedulerDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
	)

	sched.Start()

	// health endpoints (pprof and expvar)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}
