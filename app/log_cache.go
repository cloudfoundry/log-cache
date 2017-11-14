package app

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"net/http"

	loggregator "code.cloudfoundry.org/go-loggregator"
	"code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2"
	"code.cloudfoundry.org/log-cache/internal/store"
	"code.cloudfoundry.org/log-cache/internal/web"
)

// LogCache is a in memory cache for Loggregator envelopes.
type LogCache struct {
	connector  StreamConnector
	egressAddr string
	log        *log.Logger
	lis        net.Listener

	storeSize int
}

// StreamConnector reads envelopes from the the logs provider.
type StreamConnector interface {
	// Stream creates a EnvelopeStream for the given request.
	Stream(ctx context.Context, req *loggregator_v2.EgressBatchRequest) loggregator.EnvelopeStream
}

// NewLogCache creates a new LogCache.
func NewLogCache(c StreamConnector, opts ...LogCacheOption) *LogCache {
	cache := &LogCache{
		connector:  c,
		egressAddr: ":8080",
		log:        log.New(ioutil.Discard, "", 0),
		storeSize:  10000,
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
// memory size as number of envelopes. Defaults to 10000 envelopes.
func WithStoreSize(size int) LogCacheOption {
	return func(c *LogCache) {
		c.storeSize = size
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

	store := store.NewStore(c.storeSize)

	go func() {
		router := web.NewRouter(store.Get)
		server := &http.Server{Handler: router}
		server.Serve(lis)
	}()

	go func() {
		rx := c.connector.Stream(context.Background(), &loggregator_v2.EgressBatchRequest{})
		for {
			store.Put(rx())
		}
	}()
}

// EgressAddr returns the address that the LogCache is listening on. This is
// only valid after Start has been invoked.
func (c *LogCache) EgressAddr() string {
	return c.lis.Addr().String()
}
