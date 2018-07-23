package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	LogProviderAddr string `env:"LOGS_PROVIDER_ADDR, required, report"`
	LogsProviderTLS LogsProviderTLS

	LogCacheAddr string `env:"LOG_CACHE_ADDR, required, report"`
	HealthAddr   string `env:"HEALTH_ADDR, report"`
	ShardId      string `env:"SHARD_ID, required, report"`

	LogCacheTLS tls.TLS
}

// LogsProviderTLS is the LogsProviderTLS configuration for a LogCache.
type LogsProviderTLS struct {
	LogProviderCA   string `env:"LOGS_PROVIDER_CA_FILE_PATH, required, report"`
	LogProviderCert string `env:"LOGS_PROVIDER_CERT_FILE_PATH, required, report"`
	LogProviderKey  string `env:"LOGS_PROVIDER_KEY_FILE_PATH, required, report"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		LogCacheAddr: ":8080",
		HealthAddr:   "localhost:6061",
		ShardId:      "log-cache",
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
