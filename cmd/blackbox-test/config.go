package main

import (
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

type Config struct {
	Interval time.Duration `env:"RUN_INTERVAL"`

	LogCacheAddr string `env:"LOG_CACHE_ADDR, required"`
	TLS          tls.TLS

	DatadogAPIKey     string   `env:"DATADOG_API_KEY"`
	DatadogTags       []string `env:"DATADOG_TAGS"`
	DatadogOriginHost string   `env:"DATADOG_ORIGIN_HOST"`
}

func LoadConfig() (*Config, error) {
	c := Config{
		LogCacheAddr: "localhost:8080",
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
