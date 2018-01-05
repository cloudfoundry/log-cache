package main

import envstruct "code.cloudfoundry.org/go-envstruct"

// Config is the configuration for a LogCache Gateway.
type Config struct {
	Addr            string `env:"ADDR, required"`
	LogCacheAddr    string `env:"LOG_CACHE_ADDR, required"`
	GroupReaderAddr string `env:"GROUP_READER_ADDR, required"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Addr:            ":8081",
		LogCacheAddr:    "localhost:8080",
		GroupReaderAddr: "localhost:8082",
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
