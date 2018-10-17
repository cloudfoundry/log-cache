package main

import (
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/pkg/tls"
)

// Config is the configuration for a Scheduler.
type Config struct {
	HealthAddr string `env:"HEALTH_ADDR, report"`

	Interval          time.Duration `env:"INTERVAL, report"`
	Count             int           `env:"COUNT, report"`
	ReplicationFactor int           `env:"REPLICATION_FACTOR, report"`

	// NodeAddrs are all the LogCache addresses. They are in order according
	// to their NodeIndex.
	NodeAddrs []string `env:"NODE_ADDRS, report"`

	// If empty, then the scheduler assumes it is always the leader.
	LeaderElectionEndpoint string `env:"LEADER_ELECTION_ENDPOINT, report"`

	TLS tls.TLS
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		HealthAddr:        "localhost:6064",
		Count:             100,
		ReplicationFactor: 1,
		Interval:          time.Minute,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
