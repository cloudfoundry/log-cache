package main

import (
	"encoding/json"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

// Config is the configuration for a LogCache.
type Config struct {
	LogCacheAddr      string              `env:"LOG_CACHE_ADDR, required, report"`
	InstanceAddr      string              `env:"INSTANCE_ADDR, required, report"`
	Interval          time.Duration       `env:"INTERVAL, report"`
	Counters          CounterDescriptions `env:"COUNTERS_JSON, report"`
	Gauges            GaugeDescriptions   `env:"GAUGES_JSON, report"`
	Maps              MapDescriptions     `env:"MAPS_JSON, report"`
	StructuredLogging bool                `env:"STRUCTURED_LOGGING, report"`

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

type MapDescription struct {
	Addr     string            `json:"addr"`
	Name     string            `json:"name"`
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

type MapDescriptions struct {
	Descriptions []MapDescription
}

func (d *MapDescriptions) UnmarshalEnv(v string) error {
	return json.Unmarshal([]byte(v), &d.Descriptions)
}
