package app

import (
	"context"
	"expvar"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/metrics"
	"code.cloudfoundry.org/log-cache/internal/store"
	"code.cloudfoundry.org/log-cache/internal/web"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	connector  StreamConnector
	egressAddr string
	log        *log.Logger
	lis        net.Listener
	metricMap  MetricMap

	storeSize    int
	maxPerSource int
}

// StreamConnector reads envelopes from the the logs provider.
type StreamConnector interface {
	// Stream creates a EnvelopeStream for the given request.
	Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream
}

// NewLogCache creates a new LogCache.
func NewLogCache(c StreamConnector, opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		connector:    c,
		egressAddr:   ":8080",
		log:          log.New(ioutil.Discard, "", 0),
		storeSize:    10000000,
		maxPerSource: 100000,
	}

	for _, o := range opts {
		o(cache)
	}

	return cache
}

// LogCacheOption configures a LogCache.
type LogCacheOption func(*LogCache)

// WithLogger returns a LogCacheOption that configures the logger used for
// the LogCache. Defaults to silent logger.
func WithLogger(l *log.Logger) LogCacheOption {
	return func(c *LogCache) {
		c.log = l
	}
}

// WithEgressAddr returns a LogCacheOption that configures the LogCache's
// egress address. It defaults to ":8080".
func WithEgressAddr(addr string) LogCacheOption {
	return func(c *LogCache) {
		c.egressAddr = addr
	}
}

// WithStoreSize returns a LogCacheOption that configures the store's
// memory size as number of envelopes. Defaults to 1000000 envelopes.
func WithStoreSize(size int) LogCacheOption {
	return func(c *LogCache) {
		c.storeSize = size
	}
}

// WithMaxPerSource returns a LogCacheOption that configures the store's
// memory size as number of envelopes for a specific sourceID. Defaults to
// 100000 envelopes.
func WithMaxPerSource(size int) LogCacheOption {
	return func(c *LogCache) {
		c.maxPerSource = size
	}
}

// MetricMap mirrors expvar.Map.
type MetricMap interface {
	// Set is implemented by Map.Set()
	Set(key string, av expvar.Var)
}

// WithMetrics returns a LogCacheOption that configures the metrics for the
// LogCache. It will add metrics to the given map.
func WithMetrics(m MetricMap) LogCacheOption {
	return func(c *LogCache) {
		c.metricMap = m
	}
}

// Start starts the LogCache. It has an internal go-routine that it creates
// and therefore does not block.
func (c *LogCache) Start() {
	lis, err := net.Listen("tcp", c.egressAddr)
	if err != nil {
		c.log.Fatalf("failed to listen on addr %s: %s", c.egressAddr, err)
	}
	c.lis = lis
	c.log.Printf("listening on %s...", lis.Addr().String())

	metrics := metrics.New(c.metricMap)
	store := store.NewStore(c.storeSize, c.maxPerSource, metrics)

	go func() {
		router := web.NewRouter(store.Get, metrics)
		server := &http.Server{Handler: router}
		server.Serve(lis)
	}()

	go func() {
		rx := c.connector.Stream(context.Background(), &loggregator_v2.EgressBatchRequest{})
		ingressMetric := metrics.NewCounter("ingress")
		for {
			envelopes := rx()
			store.Put(envelopes)
			ingressMetric(uint64(len(envelopes)))
		}
	}()
}

// EgressAddr returns the address that the LogCache is listening on. This is
// only valid after Start has been invoked.
func (c *LogCache) EgressAddr() string {
	return c.lis.Addr().String()
}
