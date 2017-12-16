package main

import envstruct "code.cloudfoundry.org/go-envstruct"

// Config is the configuration for a LogCache.
type Config struct {
	Addr       string `env:"ADDR, required"`
	HealthPort int    `env:"HEALTH_PORT"`

	// StoreSize is the number of envelopes to store.
	StoreSize int `env:"STORE_SIZE"`

	// NodeIndex determines what data the node stores. It splits up the
	// range
	// of 0 - 18446744073709551615 evenly. If data falls out of range
	// of the given node, it will be routed to theh correct one.
	NodeIndex int `env:"NODE_INDEX"`

	// NodeAddrs are all the LogCache addresses (including the current
	// address). They are in order according to their NodeIndex.
	//
	// If NodeAddrs is emptpy or size 1, then data is not routed as it is
	// assumed that the current node is the only one.
	NodeAddrs []string `env:"NODE_ADDRS"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Addr:       ":8080",
		StoreSize:  10000,
		HealthPort: 0,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
