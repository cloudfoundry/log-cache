package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
	"time"
)

// Config is the configuration for a Syslog Server
type Config struct {
	LogCacheAddr string `env:"LOG_CACHE_ADDR, required, report"`
	SyslogPort   int    `env:"SYSLOG_PORT, required, report"`
	HealthPort   int    `env:"HEALTH_PORT, report"`

	LogCacheTLS       tls.TLS
	SyslogTLSCertPath string `env:"SYSLOG_TLS_CERT_PATH, required, report"`
	SyslogTLSKeyPath  string `env:"SYSLOG_TLS_KEY_PATH, required, report"`

	SyslogIdleTimeout time.Duration `env:"SYSLOG_IDLE_TIMEOUT, report"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		LogCacheAddr:      ":8080",
		SyslogPort:        8888,
		HealthPort:        6061,
		SyslogIdleTimeout: 2 * time.Minute,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}
