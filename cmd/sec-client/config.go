package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/tls"
)

type Config struct {
	DataSourceGrpcAddr string `env:"DATA_SOURCE_GRPC_ADDR, report"`
	SkipTLSVerify      bool   `env:"SKIP_TLS_VERIFY, report"`

	TLS tls.TLS
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
