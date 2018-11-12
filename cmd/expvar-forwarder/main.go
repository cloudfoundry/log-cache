package main

import (
	"log"
	_ "net/http/pprof"
	"os"

	. "code.cloudfoundry.org/log-cache/internal/expvar"
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

	opts := []ExpvarForwarderOption{
		WithExpvarLogger(log.New(os.Stderr, "", log.LstdFlags)),
		WithAgentDialOpts(grpc.WithTransportCredentials(cfg.AgentTLS.Credentials("loggregator-agent"))),
		WithExpvarGlobalTag("host", cfg.MetricHost),
		WithExpvarGlobalTag("addr", cfg.InstanceAddr),
		WithExpvarGlobalTag("id", cfg.InstanceId),
		WithExpvarGlobalTag("instance-id", cfg.InstanceCid),
		WithExpvarDefaultSourceId(cfg.DefaultSourceId),
		WithExpvarVersion(cfg.Version),
	}

	if cfg.StructuredLogging {
		opts = append(opts, WithExpvarStructuredLogger(log.New(os.Stdout, "", 0)))
	}

	for _, c := range cfg.Counters.Descriptions {
		opts = append(opts, AddExpvarCounterTemplate(
			c.Addr,
			c.Name,
			c.SourceId,
			c.Template,
			c.Tags,
		))
	}

	for _, g := range cfg.Gauges.Descriptions {
		opts = append(opts, AddExpvarGaugeTemplate(
			g.Addr,
			g.Name,
			g.Unit,
			g.SourceId,
			g.Template,
			g.Tags,
		))
	}

	for _, m := range cfg.Maps.Descriptions {
		opts = append(opts, AddExpvarMapTemplate(
			m.Addr,
			m.Name,
			m.SourceId,
			m.Template,
			m.Tags,
		))
	}

	forwarder := NewExpvarForwarder(
		cfg.AgentAddr,
		opts...,
	)

	forwarder.Start()
}
