package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	envstruct "code.cloudfoundry.org/go-envstruct"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	logcache "code.cloudfoundry.org/log-cache/pkg/client"
)

func main() {
	log := log.New(os.Stderr, "", 0)
	cfg := loadConfig()

	httpClient := newHTTPClient(cfg)

	client := logcache.NewClient(cfg.Addr, logcache.WithHTTPClient(httpClient))

	visitor := func(es []*loggregator_v2.Envelope) bool {
		for _, e := range es {
			fmt.Printf("%+v\n", e)
		}
		return true
	}

	now := time.Now()

	opts := []logcache.WalkOption{
		logcache.WithWalkBackoff(logcache.NewAlwaysRetryBackoff(time.Second)),
		logcache.WithWalkLogger(log),
	}

	if cfg.Duration != 0 {
		log.Printf("Have duration (%v). Walking finite window...", cfg.Duration)
		opts = append(opts,
			logcache.WithWalkStartTime(now.Add(-cfg.Duration)),
			logcache.WithWalkEndTime(now),
		)
	}

	logcache.Walk(
		context.Background(),
		cfg.SourceID,
		visitor,
		client.Read,
		opts...,
	)
}

type config struct {
	Addr      string        `env:"ADDR, required"`
	AuthToken string        `env:"AUTH_TOKEN, required"`
	SourceID  string        `env:"SOURCE_ID, required"`
	Duration  time.Duration `env:"DURATION"`
}

func loadConfig() config {
	c := config{}

	if err := envstruct.Load(&c); err != nil {
		log.Fatal(err)
	}

	return c
}

type HTTPClient struct {
	cfg    config
	client *http.Client
}

func newHTTPClient(c config) *HTTPClient {
	return &HTTPClient{cfg: c, client: http.DefaultClient}
}

func (h *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", h.cfg.AuthToken)
	return h.client.Do(req)
}
