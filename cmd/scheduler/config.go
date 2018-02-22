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

	Interval time.Duration `env:"INTERVAL"`
	Count    int           `env:"COUNT"`

	// NodeAddrs are all the LogCache addresses. They are in order according
	// to their NodeIndex.
	NodeAddrs []string `env:"NODE_ADDRS"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		HealthPort: 6064,
		Count:      100,
		Interval:   time.Minute,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
