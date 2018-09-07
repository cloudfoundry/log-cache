package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	Addr        string `env:"ADDR, required, report"`
	HealthAddr  string `env:"HEALTH_ADDR, report"`
	StoragePath string `env:"STORAGE_PATH, report"`

	// MinimumSize sets the lower bound for pruning. It will not prune beyond
	// the set size. Defaults to 500000.
	MinimumSize int `env:"MINIMUM_SIZE, report"`

	// MemoryLimit sets the percentage of total system memory to use for the
	// cache. If exceeded, the cache will prune. Default is 50%.
	MemoryLimit float64 `env:"MEMORY_LIMIT_PERCENT, report"`

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
		Addr:        ":8080",
		HealthAddr:  "localhost:6060",
		StoragePath: "/tmp/log-cache",
		MinimumSize: 500000,
		MemoryLimit: 50,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
