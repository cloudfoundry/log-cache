package main

import (
	envstruct "code.cloudfoundry.org/go-envstruct"
)

type CAPI struct {
	Addr       string `env:"CAPI_ADDR,        required"`
	CertPath   string `env:"CAPI_CERT_PATH,   required"`
	KeyPath    string `env:"CAPI_KEY_PATH,    required"`
	CAPath     string `env:"CAPI_CA_PATH,     required"`
	CommonName string `env:"CAPI_COMMON_NAME, required"`

	ExternalAddr string `env:"CAPI_ADDR_EXTERNAL, required"`
}

type UAA struct {
	ClientID     string `env:"UAA_CLIENT_ID,     required"`
	ClientSecret string `env:"UAA_CLIENT_SECRET, required"`
	Addr         string `env:"UAA_ADDR,          required"`
	CAPath       string `env:"UAA_CA_PATH,       required"`
}

type Config struct {
	LogCacheGatewayAddr string `env:"LOG_CACHE_GATEWAY_ADDR, required"`
	Addr                string `env:"ADDR, required"`
	SkipCertVerify      bool   `env:"SKIP_CERT_VERIFY"`

	CAPI CAPI
	UAA  UAA
}

func LoadConfig() (*Config, error) {
	cfg := Config{
		SkipCertVerify:      false,
		Addr:                ":8083",
		LogCacheGatewayAddr: "localhost:8081",
	}

	err := envstruct.Load(&cfg)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}
