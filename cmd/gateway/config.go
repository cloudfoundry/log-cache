package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache Gateway.
type Config struct {
	Addr            string `env:"ADDR, required, report"`
	LogCacheAddr    string `env:"LOG_CACHE_ADDR, required, report"`
	GroupReaderAddr string `env:"GROUP_READER_ADDR, required, report"`
	HealthAddr      string `env:"HEALTH_ADDR, report"`
	TLS             tls.TLS
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Addr:            ":8081",
		HealthAddr:      "localhost:6063",
		LogCacheAddr:    "localhost:8080",
		GroupReaderAddr: "localhost:8082",
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
