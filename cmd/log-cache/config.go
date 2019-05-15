package main

import (
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	Addr       string `env:"ADDR, required, report"`
	HealthPort int    `env:"HEALTH_PORT, report"`

	// QueryTimeout sets the maximum allowed runtime for a single PromQL query.
	// Smaller timeouts are recommended.
	QueryTimeout time.Duration `env:"QUERY_TIMEOUT, report"`

	// MemoryLimit sets the percentage of total system memory to use for the
	// cache. If exceeded, the cache will prune. Default is 50%.
	MemoryLimit uint `env:"MEMORY_LIMIT_PERCENT, report"`

	// MaxPerSource sets the maximum number of items stored per source.
	// Because autoscaler requires a minute of data, apps with more than 1000
	// requests per second will fill up the router logs/metrics in less than a
	// minute. Default is 100000.
	MaxPerSource int `env:"MAX_PER_SOURCE, report"`

	// NodeIndex determines what data the node stores. It splits up the range
	// of 0 - 18446744073709551615 evenly. If data falls out of range of the
	// given node, it will be routed to theh correct one.
	NodeIndex int `env:"NODE_INDEX, report"`

	// NodeAddrs are all the LogCache addresses (including the current
	// address). They are in order according to their NodeIndex.
	//
	// If NodeAddrs is emptpy or size 1, then data is not routed as it is
	// assumed that the current node is the only one.
	NodeAddrs []string `env:"NODE_ADDRS, report"`

	TLS tls.TLS
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Addr:         ":8080",
		HealthPort:   6060,
		QueryTimeout: 10 * time.Second,
		MemoryLimit:  50,
		MaxPerSource: 100000,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
