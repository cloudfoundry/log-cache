package main

import (
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a Scheduler.
type Config struct {
	TLS        tls.TLS
	HealthPort int `env:"HEALTH_PORT"`

	Interval          time.Duration `env:"INTERVAL"`
	Count             int           `env:"COUNT"`
	ReplicationFactor int           `env:"REPLICATION_FACTOR"`

	// GroupReaderNodeAddrs are all the GroupReader addresses. They are in
	// order according to their NodeIndex.
	GroupReaderNodeAddrs []string `env:"GROUP_READER_NODE_ADDRS"`

	// NodeAddrs are all the LogCache addresses. They are in order according
	// to their NodeIndex.
	NodeAddrs []string `env:"NODE_ADDRS"`

	// If empty, then the scheduler assumes it is always the leader.
	LeaderElectionEndpoint string `env:"LEADER_ELECTION_ENDPOINT"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		HealthPort:        6064,
		Count:             100,
		ReplicationFactor: 1,
		Interval:          time.Minute,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
