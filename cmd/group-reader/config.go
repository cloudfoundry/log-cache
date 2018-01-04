package main

import envstruct "code.cloudfoundry.org/go-envstruct"

// Config is the configuration for a LogCache GroupReader.
type Config struct {
	Addr         string `env:"ADDR, required"`
	LogCacheAddr string `env:"LOG_CACHE_ADDR, required"`
	HealthPort   int    `env:"HEALTH_PORT"`

	// NodeIndex determines what data the node stores. It splits up the range
	// of 0 - 18446744073709551615 evenly. If a group name falls out of range
	// of the given node, it will be routed to theh correct one.
	NodeIndex int `env:"NODE_INDEX"`

	// NodeAddrs are all the LogCache addresses (including the current
	// address). They are in order according to their NodeIndex.
	//
	// If NodeAddrs is emptpy or size 1, then requests is not routed as it is
	// assumed that the current node is the only one.
	NodeAddrs []string `env:"NODE_ADDRS"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Addr:         ":8082",
		LogCacheAddr: "localhost:8080",
		HealthPort:   6062,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
