package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"time"

	"crypto/x509"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/log-cache/internal/auth"
	. "code.cloudfoundry.org/log-cache/internal/cfauthproxy"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/log-cache/internal/promql"
	sharedtls "code.cloudfoundry.org/log-cache/internal/tls"
	"code.cloudfoundry.org/log-cache/pkg/client"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log := log.New(os.Stderr, "", log.LstdFlags)
	log.Print("Starting Log Cache CF Auth Reverse Proxy...")
	defer log.Print("Closing Log Cache CF Auth Reverse Proxy.")

	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("failed to load config: %s", err)
	}
	envstruct.WriteReport(cfg)

	metrics := metrics.New()

	uaaClient := auth.NewUAAClient(
		cfg.UAA.Addr,
		buildUAAClient(cfg),
		metrics,
		log,
	)

	// try to get our first token key, but bail out if we can't talk to UAA
	err = uaaClient.RefreshTokenKeys()
	if err != nil {
		log.Fatalf("failed to fetch token from UAA: %s", err)
	}

	gatewayURL, err := url.Parse(cfg.LogCacheGatewayAddr)
	if err != nil {
		log.Fatalf("failed to parse gateway address: %s", err)
	}

	// Force communication with the gateway to happen via HTTPS, regardless of
	// the scheme provided in the config
	gatewayURL.Scheme = "https"

	capiClient := auth.NewCAPIClient(
		cfg.CAPI.Addr,
		buildCAPIClient(cfg),
		metrics,
		log,
		auth.WithTokenPruningInterval(cfg.TokenPruningInterval),
		auth.WithCacheExpirationInterval(cfg.CacheExpirationInterval),
	)

	proxyCACertPool := loadCA(cfg.ProxyCAPath)

	// Calls to /api/v1/meta get sent to the gateway, but not through the
	// reverse proxy like everything else. As a result, we also need to set
	// the Transport here to ensure the correct root CA is available.
	metaHTTPClient := &http.Client{
		Timeout:   5 * time.Second,
		Transport: NewTransportWithRootCA(proxyCACertPool),
	}

	metaFetcher := client.NewClient(
		gatewayURL.String(),
		client.WithHTTPClient(metaHTTPClient),
	)

	middlewareProvider := auth.NewCFAuthMiddlewareProvider(
		uaaClient,
		capiClient,
		metaFetcher,
		promql.ExtractSourceIds,
		capiClient,
	)

	proxy := NewCFAuthProxy(
		gatewayURL.String(),
		cfg.Addr,
		cfg.CertPath,
		cfg.KeyPath,
		proxyCACertPool,
		WithAuthMiddleware(middlewareProvider.Middleware),
	)

	if cfg.SecurityEventLog != "" {
		accessLog, err := os.OpenFile(cfg.SecurityEventLog, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			log.Panicf("Unable to open access log: %s", err)
		}
		defer func() {
			accessLog.Sync()
			accessLog.Close()
		}()

		_, localPort, err := net.SplitHostPort(cfg.Addr)
		if err != nil {
			log.Panicf("Unable to determine local port: %s", err)
		}

		accessLogger := auth.NewAccessLogger(accessLog)
		accessMiddleware := auth.NewAccessMiddleware(accessLogger, cfg.InternalIP, localPort)
		WithAccessMiddleware(accessMiddleware)(proxy)
	}

	proxy.Start()

	// Register prometheus-compatible metric endpoint
	http.Handle("/metrics", metrics)

	// health endpoints (pprof and prometheus)
	log.Printf("Health: %s", http.ListenAndServe(fmt.Sprintf("localhost:%d", cfg.HealthPort), nil))
}

func buildUAAClient(cfg *Config) *http.Client {
	tlsConfig := sharedtls.NewBaseTLSConfig()
	tlsConfig.InsecureSkipVerify = cfg.SkipCertVerify

	tlsConfig.RootCAs = loadCA(cfg.UAA.CAPath)

	transport := &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
		DisableKeepAlives:   true,
	}

	return &http.Client{
		Timeout:   20 * time.Second,
		Transport: transport,
	}
}

func buildCAPIClient(cfg *Config) *http.Client {
	tlsConfig := sharedtls.NewBaseTLSConfig()
	tlsConfig.ServerName = cfg.CAPI.CommonName

	tlsConfig.RootCAs = loadCA(cfg.CAPI.CAPath)

	tlsConfig.InsecureSkipVerify = cfg.SkipCertVerify
	transport := &http.Transport{
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
		DisableKeepAlives:   true,
	}

	return &http.Client{
		Timeout:   20 * time.Second,
		Transport: transport,
	}
}

func loadCA(caCertPath string) *x509.CertPool {
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		log.Fatalf("failed to read CA certificate: %s", err)
	}

	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(caCert)
	if !ok {
		log.Fatal("failed to parse CA certificate.")
	}

	return certPool
}
