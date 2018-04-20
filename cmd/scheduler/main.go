package main

import (
	"expvar"
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

	opts := []logcache.SchedulerOption{
		logcache.WithSchedulerLogger(log.New(os.Stderr, "", log.LstdFlags)),
		logcache.WithSchedulerMetrics(metrics.New(expvar.NewMap("Scheduler"))),
		logcache.WithSchedulerInterval(cfg.Interval),
		logcache.WithSchedulerCount(cfg.Count),
		logcache.WithSchedulerReplicationFactor(cfg.ReplicationFactor),
		logcache.WithSchedulerDialOpts(
			grpc.WithTransportCredentials(cfg.TLS.Credentials("log-cache")),
		),
	}

	if cfg.LeaderElectionEndpoint != "" {
		opts = append(opts, logcache.WithSchedulerLeadership(func() bool {
			resp, err := http.Get(cfg.LeaderElectionEndpoint)
			if err != nil {
				log.Printf("failed to read from leaderhip endpoint: %s", err)
				return false
			}

			return resp.StatusCode == http.StatusOK
		}))
	}

	sched := logcache.NewScheduler(
		cfg.GroupReaderNodeAddrs,
		cfg.NodeAddrs,
		opts...,
	)

	sched.Start()

	// health endpoints (pprof and expvar)
	log.Printf("Health: %s", http.ListenAndServe(cfg.HealthAddr, nil))
}
