package main

import envstruct "code.cloudfoundry.org/go-envstruct"

// Config is the configuration for a LogCache Gateway.
type Config struct {
	LogCacheAddr string `env:"LOG_CACHE_ADDR, required"`
	SourceID     string `env:"SOURCE_ID, required"`
	TemplatePath string `env:"TEMPLATE_PATH, required"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
