package main

import (
	"log"
	_ "net/http/pprof"
	"os"

	logcache "code.cloudfoundry.org/log-cache"
)

func main() {
	log.Print("Starting LogCache ExpvarForwarder...")
	defer log.Print("Closing LogCache ExpvarForwarder.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %s", err)
	}

	opts := []logcache.ExpvarForwarderOption{
		logcache.WithExpvarLogger(log.New(os.Stderr, "", log.LstdFlags)),
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

	forwarder := logcache.NewExpvarForwarder(
		cfg.LogCacheAddr,
		opts...,
	)

	forwarder.Start()
}
