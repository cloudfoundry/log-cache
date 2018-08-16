package main

import (
	"log"
	_ "net/http/pprof"
	"os"

	logcache "code.cloudfoundry.org/log-cache"
	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.Print("Starting LogCache ExpvarForwarder...")
	defer log.Print("Closing LogCache ExpvarForwarder.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	opts := []logcache.ExpvarForwarderOption{
		logcache.WithExpvarLogger(log.New(os.Stderr, "", log.LstdFlags)),
		logcache.WithExpvarDialOpts(grpc.WithTransportCredentials(cfg.LogCacheTLS.Credentials("log-cache"))),
		logcache.WithGlobalTag("host", cfg.InstanceAddr),
	}

	if cfg.StructuredLogging {
		opts = append(opts, logcache.WithExpvarStructuredLogger(log.New(os.Stdout, "", 0)))
	}

	for _, c := range cfg.Counters.Descriptions {
		opts = append(opts, logcache.AddExpvarCounterTemplate(
			c.Addr,
			c.Name,
			c.SourceID,
			c.Template,
			c.Tags,
		))
	}

	for _, g := range cfg.Gauges.Descriptions {
		opts = append(opts, logcache.AddExpvarGaugeTemplate(
			g.Addr,
			g.Name,
			g.Unit,
			g.SourceID,
			g.Template,
			g.Tags,
		))
	}

	for _, m := range cfg.Maps.Descriptions {
		opts = append(opts, logcache.AddExpvarMapTemplate(
			m.Addr,
			m.Name,
			m.SourceID,
			m.Template,
			m.Tags,
		))
	}

	forwarder := logcache.NewExpvarForwarder(
		cfg.LogCacheAddr,
		opts...,
	)

	forwarder.Start()
}
