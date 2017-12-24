package main

import envstruct "code.cloudfoundry.org/go-envstruct"

// Config is the configuration for a LogCache.
type Config struct {
	LogProviderAddr string `env:"LOGS_PROVIDER_ADDR, required"`
	TLS             TLS

	LogCacheAddr string `env:"LOG_CACHE_ADDR, required"`
	HealthPort   int    `env:"HEALTH_PORT"`
}

// TLS is the TLS configuration for a LogCache.
type TLS struct {
	LogProviderCA   string `env:"LOGS_PROVIDER_CA_FILE_PATH, required"`
	LogProviderCert string `env:"LOGS_PROVIDER_CERT_FILE_PATH, required"`
	LogProviderKey  string `env:"LOGS_PROVIDER_KEY_FILE_PATH, required"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		LogCacheAddr: ":8080",
		HealthPort:   6061,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
