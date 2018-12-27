package main

import (
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

type Config struct {
	EmissionInterval time.Duration `env:"EMISSION_INTERVAL, required, report"`
	SampleInterval   time.Duration `env:"SAMPLE_INTERVAL, required, report"`
	WindowInterval   time.Duration `env:"WINDOW_INTERVAL, required, report"`
	WindowLag        time.Duration `env:"WINDOW_LAG, required, report"`
	SourceId         string        `env:"SOURCE_ID, required, report"`

	CfBlackboxEnabled  bool   `env:"CF_BLACKBOX_ENABLED, report"`
	DataSourceHTTPAddr string `env:"DATA_SOURCE_HTTP_ADDR, report"`
	UaaAddr            string `env:"UAA_ADDR, report"`
	ClientID           string `env:"CLIENT_ID, report"`
	ClientSecret       string `env:"CLIENT_SECRET"`
	SkipTLSVerify      bool   `env:"SKIP_TLS_VERIFY, report"`

	DataSourceGrpcAddr string `env:"DATA_SOURCE_GRPC_ADDR, report"`
	TLS                tls.TLS
}

func LoadConfig() (*Config, error) {
	c := Config{
		DataSourceGrpcAddr: "localhost:8080",
	}

	if err := envstruct.Load(&c); err != nil {
		return nil, err
	}

	envstruct.WriteReport(&c)

	return &c, nil
}
