package main

import (
	"encoding/json"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	LogCacheAddr      string              `env:"LOG_CACHE_ADDR, required"`
	Interval          time.Duration       `env:"INTERVAL"`
	Counters          CounterDescriptions `env:"COUNTERS_JSON"`
	Gauges            GaugeDescriptions   `env:"GAUGES_JSON"`
	StructuredLogging bool                `env:"STRUCTURED_LOGGING"`

	LogCacheTLS tls.TLS
}

type CounterDescription struct {
	Addr     string            `json:"addr"`
	Name     string            `json:"name"`
	SourceID string            `json:"source_id"`
	Template string            `json:"template"`
	Tags     map[string]string `json:"tags"`
}

type GaugeDescription struct {
	Addr     string            `json:"addr"`
	Name     string            `json:"name"`
	Unit     string            `json:"unit"`
	SourceID string            `json:"source_id"`
	Template string            `json:"template"`
	Tags     map[string]string `json:"tags"`
}

// LoadConfig creates Config object from environment variables
func LoadConfig() (*Config, error) {
	c := Config{
		Interval: time.Minute,
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

type CounterDescriptions struct {
	Descriptions []CounterDescription
}

func (d *CounterDescriptions) UnmarshalEnv(v string) error {
	return json.Unmarshal([]byte(v), &d.Descriptions)
}

type GaugeDescriptions struct {
	Descriptions []GaugeDescription
}

func (d *GaugeDescriptions) UnmarshalEnv(v string) error {
	return json.Unmarshal([]byte(v), &d.Descriptions)
}
